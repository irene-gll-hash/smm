from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Any
import httpx
from .security import telegram_response


@dataclass(slots=True)
class MediaAsset:
    name: str
    mime_type: str
    data: bytes


class TelegramPublisher:
    """Transport used only for the dedicated test publishing bot."""

    def __init__(self, token: str, test_channel_id: str) -> None:
        self.token = token
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.test_channel_id = test_channel_id
        self.client = httpx.AsyncClient(timeout=180)

    async def close(self) -> None:
        await self.client.aclose()

    async def _json(self, method: str, payload: dict[str, Any]) -> Any:
        response = await self.client.post(f"{self.base_url}/{method}", json=payload)
        return telegram_response(response, method, self.token)

    async def _multipart(
        self,
        method: str,
        data: dict[str, str],
        files: dict[str, tuple[str, bytes, str]],
    ) -> Any:
        response = await self.client.post(
            f"{self.base_url}/{method}", data=data, files=files
        )
        return telegram_response(response, method, self.token)

    async def publish_test(
        self, text: str, media: list[MediaAsset], *, long_text_mode: str = "caption"
    ) -> list[int]:
        if not media:
            message = await self._json(
                "sendMessage",
                {
                    "chat_id": self.test_channel_id,
                    "text": text[:4096],
                    "disable_web_page_preview": True,
                },
            )
            ids = [int(message["message_id"])]
            remainder = text[4096:]
            while remainder:
                message = await self._json(
                    "sendMessage",
                    {"chat_id": self.test_channel_id, "text": remainder[:4096]},
                )
                ids.append(int(message["message_id"]))
                remainder = remainder[4096:]
            return ids
        if len(media) > 10:
            raise ValueError("Telegram позволяет отправить не более 10 медиа в одном альбоме")
        caption = text if len(text) <= 1024 and long_text_mode == "caption" else ""
        ids: list[int] = []
        if len(media) == 1:
            asset = media[0]
            if asset.mime_type.startswith("image/"):
                method, field = "sendPhoto", "photo"
            elif asset.mime_type.startswith("video/"):
                method, field = "sendVideo", "video"
            else:
                method, field = "sendDocument", "document"
            message = await self._multipart(
                method,
                {"chat_id": self.test_channel_id, "caption": caption},
                {field: (asset.name, asset.data, asset.mime_type)},
            )
            ids.append(int(message["message_id"]))
        elif all(asset.mime_type.startswith(("image/", "video/")) for asset in media):
            media_json: list[dict[str, str]] = []
            files: dict[str, tuple[str, bytes, str]] = {}
            for index, asset in enumerate(media):
                key = f"media{index}"
                media_type = "video" if asset.mime_type.startswith("video/") else "photo"
                item = {"type": media_type, "media": f"attach://{key}"}
                if index == 0 and caption:
                    item["caption"] = caption
                media_json.append(item)
                files[key] = (asset.name, asset.data, asset.mime_type)
            messages = await self._multipart(
                "sendMediaGroup",
                {"chat_id": self.test_channel_id, "media": json.dumps(media_json)},
                files,
            )
            ids.extend(int(item["message_id"]) for item in messages)
        else:
            # Albums accept only compatible media types. Supporting source files
            # here is useful for tests, but verified images still go as photos.
            for index, asset in enumerate(media):
                if asset.mime_type.startswith("image/"):
                    method, field = "sendPhoto", "photo"
                elif asset.mime_type.startswith("video/"):
                    method, field = "sendVideo", "video"
                else:
                    method, field = "sendDocument", "document"
                message = await self._multipart(
                    method,
                    {"chat_id": self.test_channel_id,
                     "caption": caption if index == 0 else ""},
                    {field: (asset.name, asset.data, asset.mime_type)},
                )
                ids.append(int(message["message_id"]))
        if not caption:
            remainder = text
            while remainder:
                message = await self._json(
                    "sendMessage",
                    {"chat_id": self.test_channel_id, "text": remainder[:4096]},
                )
                ids.append(int(message["message_id"]))
                remainder = remainder[4096:]
        return ids
