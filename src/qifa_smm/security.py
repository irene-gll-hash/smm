from __future__ import annotations
import logging
import re
import httpx


class UserFacingError(ValueError):
    """An explicit, secret-free message safe to display to the user."""


def redact(text: str, secrets: tuple[str, ...] = ()) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    return re.sub(r"(https?://api\.telegram\.org/(?:file/)?bot)[^/\s]+", r"\1[REDACTED]", text)


class SecretFilter(logging.Filter):
    def __init__(self, secrets: tuple[str, ...]) -> None:
        super().__init__()
        self.secrets = secrets

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage(), self.secrets)
        record.args = ()
        if record.exc_info:
            record.exc_text = redact(logging.Formatter().formatException(record.exc_info), self.secrets)
            record.exc_info = None
        elif record.exc_text:
            record.exc_text = redact(record.exc_text, self.secrets)
        if record.stack_info:
            record.stack_info = redact(record.stack_info, self.secrets)
        return True


def configure_secret_logging(*secrets: str) -> None:
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)
    for handler in logging.getLogger().handlers:
        handler.addFilter(SecretFilter(secrets))


def telegram_response(response: httpx.Response, method: str, token: str, *, file: bool = False):
    try:
        body = response.json() if not file else {}
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    if not response.is_success or (not file and not body.get("ok")):
        description = redact(str(body.get("description", "Ошибка Telegram API")), (token,))[:300]
        raise RuntimeError(f"Telegram {method}: HTTP {response.status_code}: {description}")
    return body.get("result")
