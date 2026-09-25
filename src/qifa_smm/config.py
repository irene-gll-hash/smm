from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from typing import Annotated
from pydantic import Field, field_validator, model_validator
from pydantic_settings import NoDecode, SettingsConfigDict
from .google_auth import OAuthSettings


class Settings(OAuthSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    telegram_control_bot_token: str
    telegram_publish_bot_token: str
    telegram_test_channel_id: str
    telegram_allowed_user_ids: Annotated[frozenset[int], NoDecode] = Field(default_factory=frozenset)
    telegram_approver_user_ids: Annotated[frozenset[int], NoDecode] = Field(default_factory=frozenset)
    telegram_long_poll_seconds: int = 45
    telegram_max_upload_bytes: int = 20_000_000
    max_source_bytes: int = Field(default=20_000_000, gt=0)
    max_media_bytes: int = Field(default=20_000_000, gt=0)
    max_post_media_bytes: int = Field(default=80_000_000, gt=0)
    max_archive_bytes: int = Field(default=50_000_000, gt=0)
    max_archive_files: int = Field(default=2000, gt=0)
    max_pdf_pages: int = Field(default=300, gt=0)
    google_spreadsheet_id: str
    google_drive_root_folder_id: str
    llm_api_key: str = ""
    llm_base_url: str = "https://api.openai.com/v1"
    llm_model: str = ""
    llm_mode: str = "api"
    database_path: Path = Path("/data/qifa_smm.sqlite3")
    timezone: str = "Europe/Moscow"
    job_poll_seconds: float = 2.0
    job_workers: int = Field(default=1, ge=1)
    log_level: str = "INFO"
    sheet_periods: str = "Периоды"
    sheet_materials: str = "Разбор материалов"
    sheet_plan: str = "План публикаций"
    sheet_texts: str = "Тексты"
    sheet_channels: str = "Каналы"
    sheet_rules: str = "Правила"
    sheet_bans: str = "Запреты"
    sheet_references: str = "Эталоны"
    sheet_log: str = "Лог"

    @field_validator("telegram_allowed_user_ids", "telegram_approver_user_ids", mode="before")
    @classmethod
    def parse_user_ids(cls, value: object) -> frozenset[int]:
        if isinstance(value, str):
            value = [item.strip() for item in value.split(",") if item.strip()]
        items = list(value or [])
        if any(isinstance(item, bool) or not str(item).isdigit() or int(item) <= 0 for item in items):
            raise ValueError("ID пользователей должны быть положительными целыми числами")
        return frozenset(int(item) for item in items)

    @field_validator("telegram_long_poll_seconds")
    @classmethod
    def validate_long_poll(cls, value: int) -> int:
        if not 10 <= value <= 50:
            raise ValueError("TELEGRAM_LONG_POLL_SECONDS must be between 10 and 50")
        return value

    @field_validator("llm_mode")
    @classmethod
    def validate_llm_mode(cls, value: str) -> str:
        value = value.strip().lower()
        if value not in {"api", "stub"}:
            raise ValueError("LLM_MODE must be api or stub")
        return value

    def is_approver(self, user_id: int) -> bool:
        allowed = self.telegram_approver_user_ids or self.telegram_allowed_user_ids
        return user_id in allowed

    @model_validator(mode="after")
    def validate_bot_separation(self) -> "Settings":
        if self.telegram_control_bot_token == self.telegram_publish_bot_token:
            raise ValueError("Управляющий бот и бот-публикатор должны быть разными")
        if self.telegram_approver_user_ids and not self.telegram_approver_user_ids.issubset(
            self.telegram_allowed_user_ids
        ):
            raise ValueError("Каждый согласующий должен входить в TELEGRAM_ALLOWED_USER_IDS")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
