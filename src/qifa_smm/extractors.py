from __future__ import annotations
import io
import asyncio
import ipaddress
import socket
import re
from urllib.parse import urljoin
import logging
import zipfile
from pathlib import PurePosixPath
import httpx
from bs4 import BeautifulSoup
from docx import Document
from pypdf import PdfReader
from .utils import extract_urls
from .limits import FileLimitError
TEXT_MIME_TYPES = {
    "text/plain",
    "text/markdown",
    "text/csv",
    "application/json",
}
MEDIA_PREFIXES = ("image/", "video/", "audio/")


class ContentExtractor:
    def __init__(self, max_download_bytes: int = 20_000_000, max_archive_bytes: int = 50_000_000,
                 max_archive_files: int = 2000, max_pdf_pages: int = 300) -> None:
        self.max_download_bytes = max_download_bytes
        self.max_archive_bytes = max_archive_bytes
        self.max_archive_files = max_archive_files
        self.max_pdf_pages = max_pdf_pages
        self.log = logging.getLogger(__name__)

    def from_bytes(self, name: str, mime_type: str, data: bytes) -> tuple[str, bool]:
        if len(data) > self.max_download_bytes:
            raise FileLimitError("Исходный файл превышает допустимый размер")
        suffix = PurePosixPath(name).suffix.lower()
        if suffix in {".docx", ".xlsx", ".pptx"} or "officedocument" in mime_type:
            self._check_archive(data)
        if mime_type.startswith(MEDIA_PREFIXES):
            return "", False
        if mime_type in TEXT_MIME_TYPES or suffix in {".txt", ".md", ".csv", ".json"}:
            return data.decode("utf-8", errors="replace")[:200_000], True
        if mime_type == "application/pdf" or suffix == ".pdf":
            reader = PdfReader(io.BytesIO(data))
            if len(reader.pages) > self.max_pdf_pages:
                raise FileLimitError("PDF превышает допустимое число страниц")
            text = self._limited_text((page.extract_text() or "" for page in reader.pages), "\n\n")
            return text[:200_000], bool(text.strip())
        if (
            mime_type
            == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            or suffix == ".docx"
        ):
            document = Document(io.BytesIO(data))
            def blocks():
                for block in document.iter_inner_content():
                    if hasattr(block, "rows"):
                        for row in block.rows:
                            yield "\t".join(cell.text for cell in row.cells)
                    else:
                        yield block.text
            text = self._limited_text(blocks(), "\n")
            return text[:200_000], bool(text.strip())
        if suffix in {".pptx", ".xlsx"} or mime_type in {
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        }:
            text = self._extract_open_xml(data)
            return text, bool(text.strip())
        return "", False

    @staticmethod
    def _limited_text(parts, separator: str) -> str:
        result = []
        remaining = 200_000
        for part in parts:
            value = part[:remaining]
            result.append(value)
            remaining -= len(value) + len(separator)
            if remaining <= 0:
                break
        return separator.join(result)[:200_000]

    def _check_archive(self, data: bytes) -> None:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = archive.infolist()
            if len(entries) > self.max_archive_files:
                raise FileLimitError("В архиве слишком много файлов")
            if sum(item.file_size for item in entries) > self.max_archive_bytes:
                raise FileLimitError("Распакованный документ превышает допустимый размер")

    def _extract_open_xml(self, data: bytes) -> str:
        self._check_archive(data)
        def parts():
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                shared = []
                if "xl/sharedStrings.xml" in archive.namelist():
                    soup = BeautifulSoup(archive.read("xl/sharedStrings.xml"), "xml")
                    shared = ["".join(node.get_text() for node in item.find_all("t"))
                              for item in soup.find_all("si")]
                names = sorted(archive.namelist(), key=lambda name: re.sub(
                    r"\d+", lambda match: match.group().zfill(12), name))
                for name in names:
                    if re.fullmatch(r"xl/worksheets/sheet\d+\.xml", name):
                        soup = BeautifulSoup(archive.read(name), "xml")
                        for row in soup.find_all("row"):
                            cells = []
                            for cell in row.find_all("c", recursive=False):
                                value = cell.find("v")
                                text = value.get_text() if value else ""
                                if cell.get("t") == "s" and text:
                                    text = shared[int(text)]
                                elif cell.get("t") == "inlineStr":
                                    text = "".join(node.get_text() for node in cell.find_all("t"))
                                if text:
                                    cells.append(f"{cell.get('r', '')}: {text}")
                            if cells:
                                yield f"{name}: " + "\t".join(cells)
                    elif re.fullmatch(r"ppt/slides/slide\d+\.xml", name):
                        soup = BeautifulSoup(archive.read(name), "xml")
                        for node in soup.find_all("t"):
                            yield node.get_text(" ", strip=True)
        return self._limited_text(parts(), "\n")

    @staticmethod
    async def _public_url(value: str) -> tuple[httpx.URL, str]:
        url = httpx.URL(value)
        host = url.host.rstrip(".").lower()
        if url.scheme not in {"http", "https"} or not host or url.userinfo:
            raise ValueError("Недопустимый URL")
        if url.port not in {None, 80, 443} or host == "localhost" or host.endswith((".localhost", ".local", ".internal")):
            raise ValueError("Внутренние адреса запрещены")
        addresses = await asyncio.get_running_loop().getaddrinfo(host, url.port or (443 if url.scheme == "https" else 80), type=socket.SOCK_STREAM)
        ips = [ipaddress.ip_address(item[4][0]) for item in addresses]
        if not ips or any(not ip.is_global or ip.is_multicast or ip.is_reserved or getattr(ip, "ipv4_mapped", None) for ip in ips):
            raise ValueError("Внутренние адреса запрещены")
        # Connect to the validated IP to avoid a second DNS resolution (rebinding).
        return url.copy_with(host=str(ips[0])), url.host

    async def fetch_url(self, url: str) -> tuple[str, bool]:
        try:
            async with asyncio.timeout(30):
                async with httpx.AsyncClient(follow_redirects=False, timeout=20, trust_env=False) as client:
                    for redirect in range(6):
                        target, hostname = await self._public_url(url)
                        async with client.stream("GET", target,
                                                 headers={"User-Agent": "QIFA-SMM/0.1", "Host": httpx.URL(url).netloc.decode()},
                                                 extensions={"sni_hostname": hostname}) as response:
                            if response.is_redirect:
                                location = response.headers.get("location")
                                if not location or redirect == 5:
                                    raise ValueError("Слишком много перенаправлений")
                                url = urljoin(url, location)
                                continue
                            response.raise_for_status()
                            data = bytearray()
                            async for chunk in response.aiter_bytes(chunk_size=64_000):
                                if len(data) + len(chunk) > self.max_download_bytes:
                                    raise FileLimitError("Страница превышает ограничение загрузки")
                                data.extend(chunk)
                            content_type = response.headers.get("content-type", "").split(";")[0]
                            encoding = response.encoding or "utf-8"
                            break
            if content_type in TEXT_MIME_TYPES or "html" in content_type:
                decoded = bytes(data).decode(encoding, errors="replace")
                if "html" in content_type:
                    soup = BeautifulSoup(decoded, "html.parser")
                    for tag in soup(["script", "style", "nav", "footer"]):
                        tag.decompose()
                    decoded = soup.get_text("\n", strip=True)
                return decoded[:150_000], True
            return "", False
        except Exception as exc:
            self.log.info("URL was not readable: %s", type(exc).__name__)
            return "", False

    async def expand_links(self, text: str) -> list[dict[str, str | bool]]:
        result: list[dict[str, str | bool]] = []
        for url in extract_urls(text):
            page_text, readable = await self.fetch_url(url)
            result.append({"url": url, "text": page_text, "readable": readable})
        return result
