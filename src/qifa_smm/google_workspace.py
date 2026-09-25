from __future__ import annotations
import asyncio
import io
import logging
from dataclasses import dataclass
from typing import Any
from .google_auth import load_credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from .utils import parse_drive_id
from .limits import FileLimitError, LimitedBuffer



@dataclass(slots=True)
class DriveEntry:
    id: str
    name: str
    mime_type: str
    modified_time: str
    size: str
    web_view_link: str
    relative_path: str
    parent_id: str

    @property
    def is_folder(self) -> bool:
        return self.mime_type == "application/vnd.google-apps.folder"


class GoogleWorkspace:
    def __init__(self, token_file: str, spreadsheet_id: str) -> None:
        credentials = load_credentials(token_file)
        self.drive = build("drive", "v3", credentials=credentials, cache_discovery=False)
        self.sheets = build("sheets", "v4", credentials=credentials, cache_discovery=False)
        self.spreadsheet_id = spreadsheet_id
        self.log = logging.getLogger(__name__)

    async def spreadsheet_metadata(self) -> dict[str, Any]:
        return await asyncio.to_thread(
            lambda: self.sheets.spreadsheets()
            .get(spreadsheetId=self.spreadsheet_id, fields="sheets.properties")
            .execute()
        )

    async def read_values(self, a1_range: str) -> list[list[Any]]:
        result = await asyncio.to_thread(
            lambda: self.sheets.spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id, range=a1_range)
            .execute()
        )
        return list(result.get("values", []))

    async def append_values(self, a1_range: str, rows: list[list[Any]]) -> None:
        if not rows:
            return
        await asyncio.to_thread(
            lambda: self.sheets.spreadsheets()
            .values()
            .append(
                spreadsheetId=self.spreadsheet_id,
                range=a1_range,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": rows},
            )
            .execute()
        )

    async def update_values(self, a1_range: str, rows: list[list[Any]]) -> None:
        await asyncio.to_thread(
            lambda: self.sheets.spreadsheets()
            .values()
            .update(
                spreadsheetId=self.spreadsheet_id,
                range=a1_range,
                valueInputOption="RAW",
                body={"values": rows},
            )
            .execute()
        )

    async def list_folder(self, folder_id_or_url: str) -> list[DriveEntry]:
        folder_id = parse_drive_id(folder_id_or_url)
        result: list[DriveEntry] = []
        await self._walk_folder(folder_id, "", result)
        return result

    async def list_direct_files(self, folder_id_or_url: str) -> list[DriveEntry]:
        folder_id = parse_drive_id(folder_id_or_url)
        items = []
        page_token = None
        while True:
            response = await asyncio.to_thread(
                lambda: self.drive.files()
                .list(
                    q=f"'{folder_id}' in parents and trashed = false",
                    spaces="drive",
                    fields=(
                        "nextPageToken,files(id,name,mimeType,modifiedTime,size,webViewLink,parents)"
                    ),
                    pageSize=1000,
                    pageToken=page_token,
                    orderBy="name",
                )
                .execute()
            )
            items.extend(response.get("files", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        result: list[DriveEntry] = []
        for item in items:
            entry = DriveEntry(
                id=item["id"],
                name=item["name"],
                mime_type=item["mimeType"],
                modified_time=item.get("modifiedTime", ""),
                size=item.get("size", ""),
                web_view_link=item.get("webViewLink", ""),
                relative_path=item["name"],
                parent_id=folder_id,
            )
            if not entry.is_folder:
                result.append(entry)
        return result

    async def _walk_folder(
        self, folder_id: str, relative_path: str, result: list[DriveEntry]
    ) -> None:
        page_token: str | None = None
        while True:
            response = await asyncio.to_thread(
                lambda: self.drive.files()
                .list(
                    q=f"'{folder_id}' in parents and trashed = false",
                    spaces="drive",
                    fields=(
                        "nextPageToken, files(id,name,mimeType,modifiedTime,size,webViewLink,parents)"
                    ),
                    pageSize=1000,
                    pageToken=page_token,
                    orderBy="folder,name",
                )
                .execute()
            )
            for item in response.get("files", []):
                path = f"{relative_path}/{item['name']}".lstrip("/")
                entry = DriveEntry(
                    id=item["id"],
                    name=item["name"],
                    mime_type=item["mimeType"],
                    modified_time=item.get("modifiedTime", ""),
                    size=item.get("size", ""),
                    web_view_link=item.get("webViewLink", ""),
                    relative_path=path,
                    parent_id=folder_id,
                )
                result.append(entry)
                if entry.is_folder:
                    await self._walk_folder(entry.id, path, result)
            page_token = response.get("nextPageToken")
            if not page_token:
                return

    async def download_file(self, entry: DriveEntry, max_bytes: int = 20_000_000) -> tuple[bytes, str]:
        if int(entry.size or 0) > max_bytes:
            raise FileLimitError("Файл превышает допустимый размер")
        export_types = {
            "application/vnd.google-apps.document": "text/plain",
            "application/vnd.google-apps.spreadsheet": (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
            "application/vnd.google-apps.presentation": "text/plain",
        }
        if entry.mime_type in export_types:
            request = self.drive.files().export_media(
                fileId=entry.id, mimeType=export_types[entry.mime_type]
            )
            output_mime = export_types[entry.mime_type]
        else:
            request = self.drive.files().get_media(fileId=entry.id)
            output_mime = entry.mime_type

        def _download() -> bytes:
            buffer = LimitedBuffer(max_bytes)
            downloader = MediaIoBaseDownload(buffer, request, chunksize=min(256_000, max_bytes + 1))
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return buffer.getvalue()

        return await asyncio.to_thread(_download), output_mime

    async def create_folder(self, name: str, parent_id: str) -> dict[str, Any]:
        body = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parse_drive_id(parent_id)],
        }
        return await asyncio.to_thread(
            lambda: self.drive.files()
            .create(body=body, fields="id,name,webViewLink")
            .execute()
        )

    async def ensure_folder(self, name: str, parent_id: str) -> dict[str, Any]:
        parent_id = parse_drive_id(parent_id)
        escaped = name.replace("'", "\\'")
        response = await asyncio.to_thread(
            lambda: self.drive.files()
            .list(
                q=(
                    f"'{parent_id}' in parents and trashed = false and "
                    f"mimeType = 'application/vnd.google-apps.folder' and name = '{escaped}'"
                ),
                fields="files(id,name,webViewLink)",
                pageSize=10,
            )
            .execute()
        )
        files = response.get("files", [])
        return files[0] if files else await self.create_folder(name, parent_id)

    async def upload_bytes(
        self, name: str, mime_type: str, data: bytes, parent_id: str
    ) -> dict[str, Any]:
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type, resumable=False)
        return await asyncio.to_thread(
            lambda: self.drive.files()
            .create(
                body={"name": name, "parents": [parse_drive_id(parent_id)]},
                media_body=media,
                fields="id,name,mimeType,webViewLink,modifiedTime,size",
            )
            .execute()
        )

    async def trash_folder_files(self, folder_id_or_url: str) -> int:
        files = await self.list_direct_files(folder_id_or_url)
        for entry in files:
            await asyncio.to_thread(
                lambda file_id=entry.id: self.drive.files()
                .update(fileId=file_id, body={"trashed": True})
                .execute()
            )
        return len(files)


def column_name(number: int) -> str:
    result = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result


class SheetRepository:
    def __init__(self, workspace: GoogleWorkspace) -> None:
        self.workspace = workspace

    async def rows(self, sheet: str) -> list[dict[str, Any]]:
        values = await self.workspace.read_values(f"'{sheet}'!A:ZZ")
        if not values:
            return []
        headers = [str(value).strip() for value in values[0]]
        output: list[dict[str, Any]] = []
        for index, row in enumerate(values[1:], start=2):
            padded = list(row) + [""] * (len(headers) - len(row))
            item = {headers[i]: padded[i] for i in range(len(headers)) if headers[i]}
            item["_row_number"] = index
            if any(str(value).strip() for key, value in item.items() if key != "_row_number"):
                output.append(item)
        return output

    async def headers(self, sheet: str) -> list[str]:
        values = await self.workspace.read_values(f"'{sheet}'!1:1")
        return [str(value).strip() for value in values[0]] if values else []

    async def append_dicts(self, sheet: str, items: list[dict[str, Any]]) -> None:
        if not items:
            return
        headers = await self.headers(sheet)
        if not headers:
            raise ValueError(f"Лист {sheet!r} не содержит строки заголовков")
        rows = [[item.get(header, "") for header in headers] for item in items]
        await self.workspace.append_values(f"'{sheet}'!A:{column_name(len(headers))}", rows)

    async def update_dict(self, sheet: str, row_number: int, values: dict[str, Any]) -> None:
        headers = await self.headers(sheet)
        current_rows = await self.workspace.read_values(
            f"'{sheet}'!A{row_number}:{column_name(len(headers))}{row_number}"
        )
        current = (current_rows[0] if current_rows else []) + [""] * len(headers)
        merged = [values.get(header, current[index]) for index, header in enumerate(headers)]
        await self.workspace.update_values(
            f"'{sheet}'!A{row_number}:{column_name(len(headers))}{row_number}", [merged]
        )

    async def upsert_dicts(
        self, sheet: str, key_column: str, items: list[dict[str, Any]]
    ) -> None:
        existing = await self.rows(sheet)
        by_key = {str(row.get(key_column, "")): row for row in existing if row.get(key_column)}
        new_items: list[dict[str, Any]] = []
        for item in items:
            key = str(item.get(key_column, ""))
            if not key:
                raise ValueError(f"Пустой ключ {key_column!r} для листа {sheet!r}")
            if key in by_key:
                await self.update_dict(sheet, int(by_key[key]["_row_number"]), item)
            else:
                new_items.append(item)
        await self.append_dicts(sheet, new_items)

    async def find_one(self, sheet: str, column: str, value: str) -> dict[str, Any] | None:
        for row in await self.rows(sheet):
            if str(row.get(column, "")).strip() == str(value).strip():
                return row
        return None
