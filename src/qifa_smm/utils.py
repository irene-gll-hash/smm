from __future__ import annotations
import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any
URL_RE = re.compile(r"https?://[^\s<>\]\[\)\(\"']+")


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def stable_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def extract_urls(text: str) -> list[str]:
    return list(dict.fromkeys(URL_RE.findall(text or "")))


def short_id(prefix: str, value: str) -> str:
    digest = hashlib.blake2s(value.encode("utf-8"), digest_size=5).hexdigest().upper()
    return f"{prefix}-{digest}"


def parse_drive_id(value: str) -> str:
    patterns = [
        r"/folders/([A-Za-z0-9_-]+)",
        r"/d/([A-Za-z0-9_-]+)",
        r"[?&]id=([A-Za-z0-9_-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, value or "")
        if match:
            return match.group(1)
    return value.strip()
