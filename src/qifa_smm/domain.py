from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class JobType(StrEnum):
    PROCESS_MATERIALS = "process_materials"
    BUILD_PLAN = "build_plan"
    BUILD_DRAFTS = "build_drafts"
    REWRITE_DRAFT = "rewrite_draft"
    CREATE_EXTRA_POST = "create_extra_post"
    SYNC_MEDIA = "sync_media"
    PUBLISH_TEST = "publish_test"


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    ERROR = "error"


class PlanDecision(StrEnum):
    SUPERSEDED = "superseded"
    REVIEW = "На согласовании"
    APPROVED = "Одобрено"
    CHANGES = "Нужны изменения"
    REJECTED = "Отклонено"
    MOVED = "Перенесено"


class DraftDecision(StrEnum):
    PENDING_REVIEW = "pending_review"
    NEEDS_PREPARATION = "needs_preparation"
    REVIEW = "На согласовании"
    APPROVED = "Одобрено"
    REJECTED = "Отклонено"
    NEEDS_FACT = "Нужен факт"
    SUPERSEDED = "Заменено новой версией"
    TEST_SCHEDULED = "Запланировано в тестовый канал"
    TEST_PUBLISHED = "Отправлено в тестовый канал"


class PreparationMode(StrEnum):
    FROM_MATERIALS = "Написать по материалам"
    FROM_BRIEF = "Написать по тезисам"
    ADAPT_TEXT = "Адаптировать готовый текст"
    CHECK_ONLY = "Проверить готовый текст"


class MediaChangeMode(StrEnum):
    ADD = "add"
    REPLACE = "replace"


HARD_FACT_RUBRICS = {
    "Компания и PR",
    "Новость компании",
    "PR",
    "Закон",
    "Регулирование",
    "Цифры",
    "Кейс",
}


@dataclass(slots=True)
class SourceItem:
    source_id: str
    name: str
    mime_type: str
    url: str
    path: str
    fingerprint: str
    text: str = ""
    urls: list[str] = field(default_factory=list)
    readable: bool = True


@dataclass(slots=True)
class TelegramEvent:
    update_id: int
    user_id: int
    chat_id: int
    kind: str
    text: str = ""
    callback_id: str = ""
    callback_data: str = ""
    message_id: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)
