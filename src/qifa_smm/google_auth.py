"""Desktop OAuth with durable credentials and no interactive service startup."""
from __future__ import annotations
import json
import logging
import os
from pathlib import Path
import tempfile
import threading
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from pydantic_settings import BaseSettings, SettingsConfigDict

SCOPES = (
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
)
AUTH_ERROR = "Ошибка Google OAuth. Проверьте настройки и выполните qifa-google-auth."


class OAuthSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    google_oauth_client_file: Path = Path("secrets/google-oauth-client.json")
    google_oauth_token_file: Path = Path("secrets/google-oauth-token.json")


def protect_oauth_logging() -> None:
    # These libraries may log HTTP bodies, callback URLs and token responses at DEBUG.
    prefixes = ("google.auth", "google.oauth2", "google_auth_oauthlib", "oauthlib",
                "requests_oauthlib", "requests", "urllib3", "googleapiclient",
                "google_auth_httplib2")
    for name in (*prefixes, *tuple(logging.Logger.manager.loggerDict)):
        if any(name == prefix or name.startswith(prefix + ".") for prefix in prefixes):
            logging.getLogger(name).setLevel(logging.CRITICAL + 1)
    import httplib2
    httplib2.debuglevel = 0


def save_token(credentials: Credentials, path: Path) -> None:
    """Replace atomically; mkstemp creates the token with owner-only permissions."""
    temporary = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".oauth-", dir=path.parent)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(credentials.to_json())
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except Exception:
        raise RuntimeError("Не удалось сохранить Google OAuth token. Проверьте права на secrets/.") from None
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


class StoredCredentials(Credentials):
    """Persist refreshes initiated both at startup and by Google's HTTP transport."""
    def bind(self, path: Path) -> None:
        self._token_path = path
        self._refresh_lock = threading.Lock()

    def refresh(self, request) -> None:
        with self._refresh_lock:
            try:
                super().refresh(request)
            except Exception:
                raise RuntimeError(AUTH_ERROR) from None
            save_token(self, self._token_path)


def load_credentials(token_file: str | Path) -> StoredCredentials:
    protect_oauth_logging()
    try:
        path = Path(token_file)
        credentials = StoredCredentials.from_authorized_user_file(str(path))
        if not credentials.refresh_token or not credentials.has_scopes(SCOPES):
            raise ValueError("Missing refresh token or scopes")
        credentials.bind(path)
        if not credentials.valid:
            credentials.refresh(Request())
        return credentials
    except Exception:
        raise RuntimeError(AUTH_ERROR) from None


def authorize(client_file: Path, token_file: Path) -> None:
    protect_oauth_logging()
    try:
        if client_file.resolve() == token_file.resolve():
            raise ValueError("Client and token paths must differ")
        with client_file.open(encoding="utf-8") as stream:
            config = json.load(stream)
        if "installed" not in config or "web" in config:
            raise ValueError("Desktop client required")
        flow = InstalledAppFlow.from_client_config(config, scopes=SCOPES)
        credentials = flow.run_local_server(
            host="localhost", port=0, open_browser=True,
            authorization_prompt_message=None,
            success_message="Авторизация завершена. Можно закрыть окно.",
            timeout_seconds=300, access_type="offline", prompt="consent",
        )
        if not credentials.refresh_token or not credentials.has_scopes(SCOPES):
            raise ValueError("Missing refresh token or scopes")
        if credentials.granted_scopes is not None and not set(SCOPES).issubset(credentials.granted_scopes):
            raise ValueError("Required scopes were not granted")
        save_token(credentials, token_file)
    except Exception:
        raise RuntimeError(AUTH_ERROR) from None


def run() -> None:
    try:
        settings = OAuthSettings()
        authorize(settings.google_oauth_client_file, settings.google_oauth_token_file)
    except Exception:
        raise SystemExit(AUTH_ERROR) from None
    print("Google OAuth: токен сохранён. Можно запускать сервис.")


if __name__ == "__main__":
    run()
