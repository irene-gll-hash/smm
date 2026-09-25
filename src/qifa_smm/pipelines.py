from __future__ import annotations
import json
import asyncio
import hashlib
import logging
import re
from collections import defaultdict
from datetime import datetime
from functools import wraps
from typing import Any
from .config import Settings
from .db import Database, Job
from .domain import DraftDecision, HARD_FACT_RUBRICS, JobType, PlanDecision, PreparationMode, SourceItem
from .extractors import ContentExtractor
from .limits import FileLimitError
from .security import UserFacingError
from .google_workspace import DriveEntry, GoogleWorkspace, SheetRepository
from .llm import LLMClient
from .prompts import MATERIAL_ANALYZER, PLANNER, WRITER
from .publisher import MediaAsset, TelegramPublisher
from .utils import extract_urls, short_id, stable_hash, utc_now


def first(row: dict[str, Any], *names: str, default: Any = "") -> Any:
    for name in names:
        if name in row and str(row[name]).strip():
            return row[name]
    return default


def integer(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def serialized_versions(method):
    @wraps(method)
    async def wrapped(self, *args, **kwargs):
        async with self._version_lock:
            return await method(self, *args, **kwargs)
    return wrapped


class PipelineService:
    def __init__(self, settings: Settings, database: Database, workspace: GoogleWorkspace,
                 sheets: SheetRepository, llm: LLMClient, publisher: TelegramPublisher) -> None:
        self.settings = settings
        self.database = database
        self.workspace = workspace
        self.sheets = sheets
        self.llm = llm
        self.publisher = publisher
        self.extractor = ContentExtractor(settings.max_source_bytes, settings.max_archive_bytes, settings.max_archive_files, settings.max_pdf_pages)
        self.log = logging.getLogger(__name__)
        self._version_lock = asyncio.Lock()

    def handlers(self):
        return {
            JobType.PROCESS_MATERIALS: self.process_materials,
            JobType.BUILD_PLAN: self.build_plan,
            JobType.BUILD_DRAFTS: self.build_drafts,
            JobType.REWRITE_DRAFT: self.rewrite_draft,
            JobType.CREATE_EXTRA_POST: self.create_extra_post,
            JobType.SYNC_MEDIA: self.sync_media,
            JobType.PUBLISH_TEST: self.publish_test,
        }

    async def list_periods(self) -> list[dict[str, str]]:
        result = []
        for row in await self.sheets.rows(self.settings.sheet_periods):
            period_id = str(first(row, "period_id", "ID периода", "Период"))
            if period_id:
                result.append({"period_id": period_id,
                               "title": str(first(row, "Период", "Название периода", default=period_id)),
                               "folder": str(first(row, "Ссылка на папку", "Папка Drive", "Папка материалов"))})
        return result

    async def list_channels(self) -> list[str]:
        result: list[str] = []
        for row in await self.sheets.rows(self.settings.sheet_channels):
            value = str(first(row, "Канал", "Площадка", "Название", "channel"))
            if value and value not in result:
                result.append(value)
        return result

    async def _period(self, period_id: str) -> dict[str, Any]:
        for row in await self.sheets.rows(self.settings.sheet_periods):
            if str(first(row, "period_id", "ID периода", "Период")) == period_id:
                return row
        raise UserFacingError(f"Период {period_id!r} не найден")

    async def process_materials(self, job: Job) -> str:
        period_id = str(job.payload["period_id"])
        period = await self._period(period_id)
        folder = str(first(period, "Ссылка на папку", "Папка Drive", "Папка материалов"))
        if not folder:
            raise UserFacingError("У периода не указана папка материалов")
        entries = await self.workspace.list_folder(folder)
        theme_links = {item.name: item.web_view_link for item in entries if item.is_folder and "/" not in item.relative_path}
        groups: dict[str, list[DriveEntry]] = defaultdict(list)
        for entry in entries:
            if entry.is_folder:
                continue
            theme = entry.relative_path.split("/", 1)[0]
            if theme == "Без даты" or re.fullmatch(r"\d{4}-\d{2}-\d{2}", theme):
                continue
            groups[theme].append(entry)
        updated = unchanged = unreadable = 0
        for theme, files in groups.items():
            sources: list[SourceItem] = []
            changed = False
            for entry in files:
                fingerprint = stable_hash({"id": entry.id, "modified": entry.modified_time, "size": entry.size})
                changed = changed or not await self.database.source_is_current(entry.id, fingerprint)
                try:
                    source = await self._extract_drive_entry(entry, fingerprint)
                except FileLimitError as exc:
                    self.log.warning("Skipped source %s: %s", entry.id, exc)
                    source = SourceItem(entry.id, entry.name, entry.mime_type, entry.web_view_link,
                                        entry.relative_path, fingerprint, text=f"Не прочитан: {exc}", readable=False)
                unreadable += int(not source.readable)
                sources.append(source)
            if not changed:
                unchanged += 1
                continue
            material_id = short_id("MAT", f"{period_id}:{theme}")
            response = await self.llm.json_response(
                MATERIAL_ANALYZER,
                json.dumps({"period_id": period_id, "material_id": material_id, "theme_folder": theme,
                            "sources": [{"source_id": item.source_id, "name": item.name, "path": item.path,
                                         "url": item.url, "mime_type": item.mime_type, "readable": item.readable,
                                         "text": item.text[:40_000], "links": item.urls} for item in sources]},
                           ensure_ascii=False), max_tokens=4000)
            readable_ids = {item.source_id for item in sources if item.readable}
            facts = [f"{item.get('fact', '')} [источники: {', '.join(item.get('source_ids', []))}]"
                     for item in response.get("confirmed_facts", [])
                     if item.get("fact") and item.get("source_ids") and set(item["source_ids"]).issubset(readable_ids)]
            topics = [f"{item.get('topic', '')}: {item.get('angle', '')}" for item in response.get("topic_ideas", [])]
            headers = await self.sheets.headers(self.settings.sheet_materials)
            row = self._map_headers(headers, {
                ("material_id", "ID материала", "Материал ID"): material_id,
                ("period_id", "ID периода", "Период"): period_id,
                ("Название материала", "Название"): response.get("title", theme),
                ("Категория",): response.get("category", ""),
                ("Ссылка на папку", "Ссылка на папку / файл", "Источник"): theme_links.get(theme, folder),
                ("Что произошло",): response.get("what_happened", ""),
                ("Подтверждённые факты", "Факты"): "\n".join(facts),
                ("Важная дата или эмбарго", "Эмбарго"): response.get("important_date_or_embargo", ""),
                ("Чего не хватает", "Недостающие факты"): "\n".join(response.get("missing_information", [])),
                ("Возможные темы", "Идеи и темы"): "\n".join(topics),
                ("Статус использования", "Статус"): "Не использован", ("Обновлено",): utc_now()})
            key = self._required_header(headers, "material_id", "ID материала", "Материал ID")
            await self.sheets.upsert_dicts(self.settings.sheet_materials, key, [row])
            for source in sources:
                await self.database.save_source_file(source.source_id, source.fingerprint, period_id, source.path, source.readable)
            updated += 1
        return f"Материалы разобраны: обновлено {updated}, без изменений {unchanged}, нечитаемых файлов {unreadable}."

    async def _extract_drive_entry(self, entry: DriveEntry, fingerprint: str) -> SourceItem:
        if entry.mime_type.startswith(("image/", "video/", "audio/")):
            return SourceItem(entry.id, entry.name, entry.mime_type, entry.web_view_link,
                              entry.relative_path, fingerprint, readable=False)
        supported = {"application/pdf", "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/plain", "text/markdown",
                     "text/csv", "application/vnd.google-apps.document", "application/vnd.google-apps.spreadsheet",
                     "application/vnd.google-apps.presentation"}
        if entry.mime_type not in supported and integer(entry.size) > 5_000_000:
            return SourceItem(entry.id, entry.name, entry.mime_type, entry.web_view_link,
                              entry.relative_path, fingerprint, readable=False)
        data, output_mime = await self.workspace.download_file(entry, self.settings.max_source_bytes)
        text, readable = self.extractor.from_bytes(entry.name, output_mime, data)
        links = await self.extractor.expand_links(text)
        linked = "".join(f"\n\nИсточник URL: {item['url']}\n{item['text']}" for item in links if item["readable"])
        return SourceItem(entry.id, entry.name, entry.mime_type, entry.web_view_link, entry.relative_path,
                          fingerprint, (text + linked)[:120_000], [str(item["url"]) for item in links],
                          readable or any(bool(item["readable"]) for item in links))

    async def _load_operation(self, key: str) -> dict[str, Any]:
        value = await self.database.get_setting(f"operation:{key}")
        return json.loads(value) if value else {}

    async def _store_operation(self, key: str, value: dict[str, Any]) -> None:
        await self.database.set_setting(f"operation:{key}", json.dumps(value, ensure_ascii=False))

    async def _resume_plan_changes(self, period_id: str) -> None:
        # A failed worker may leave a reserved revision that is not visible in Sheets yet.
        # Finish it before another action reads the current period and chooses max + 1.
        for key in await self.database.pending_plan_operations(period_id):
            await self._commit_plan(period_id, [], key)

    @staticmethod
    def _current_plans(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        latest: dict[str, dict[str, Any]] = {}
        for row in rows:
            post_id = str(first(row, "post_id", "ID поста", "plan_id"))
            if post_id and (post_id not in latest or
                           integer(first(row, "Версия плана", "plan_version"), 1) >
                           integer(first(latest[post_id], "Версия плана", "plan_version"), 1)):
                latest[post_id] = row
        return [row for row in latest.values()
                if first(row, "Решение редактора", "Статус") != PlanDecision.SUPERSEDED.value]

    @staticmethod
    def _plan_content(row: dict[str, Any]) -> dict[str, Any]:
        ignored = {"_row_number", "Версия плана", "plan_version", "Решение редактора", "Статус",
                   "Зафиксировано человеком", "Закреплено", "Автор плана", "Автор"}
        return {key: str(value).strip() for key, value in row.items()
                if key not in ignored and str(value).strip()}

    async def _published_plans(self, drafts: list[dict[str, Any]]) -> set[tuple[str, int]]:
        published = await self.database.published_versions()
        return {(str(first(row, "post_id", "ID поста", "plan_id")),
                 integer(first(row, "plan_version", "Версия плана"), 1))
                for row in drafts
                if first(row, "Статус", "Статус согласования") == DraftDecision.TEST_PUBLISHED.value
                or (str(first(row, "post_id", "ID поста", "plan_id")),
                    integer(first(row, "draft_version", "Версия"))) in published}

    async def _commit_plan(self, period_id: str, candidates: list[dict[str, Any]],
                           operation_key: str) -> str:
        """Append a period revision; checkpoint before writes and reconcile uncertain appends."""
        operation = await self._load_operation(operation_key)
        if operation.get("done"):
            return operation["message"]
        headers = await self.sheets.headers(self.settings.sheet_plan)
        post_key = self._required_header(headers, "post_id", "ID поста", "plan_id")
        version_key = self._required_header(headers, "Версия плана", "plan_version")
        status_key = self._required_header(headers, "Решение редактора", "Статус")
        if not operation:
            history = [row for row in await self.sheets.rows(self.settings.sheet_plan)
                       if str(first(row, "period_id", "ID периода", "Период")) == period_id]
            current = self._current_plans(history)
            published = await self._published_plans(await self.sheets.rows(self.settings.sheet_texts))
            frozen = {str(row[post_key]) for row in current
                      if (str(row[post_key]), integer(row.get(version_key), 1)) in published}
            by_id = {str(row[post_key]): row for row in current}
            candidates = [by_id[str(row[post_key])] if str(row[post_key]) in frozen else row
                          for row in candidates]
            before = {str(row[post_key]): self._plan_content(row) for row in current}
            after = {str(row[post_key]): self._plan_content(row) for row in candidates}
            changed = before != after
            version = max((integer(row.get(version_key), 1) for row in history), default=0) + 1
            new_rows = []
            if changed:
                for row in candidates:
                    if str(row[post_key]) in frozen:
                        continue
                    new_rows.append({**{k: v for k, v in row.items() if k != "_row_number"},
                                     version_key: version,
                                     status_key: (row[status_key] if row.get(status_key) == "Заблокирован — нужен исходник"
                                                  else PlanDecision.REVIEW.value)})
            operation = {"period_id": period_id, "rows": new_rows,
                         "previous": [[str(row[post_key]), integer(row.get(version_key), 1)]
                                      for row in current if changed and str(row[post_key]) not in frozen],
                         "message": f"План сохранён: версия {version}." if changed else "Содержание плана не изменилось.",
                         "changed": changed, "done": False}
            await self._store_operation(operation_key, operation)
        # Do not overwrite a saved version, including one published after an earlier attempt.
        history = await self.sheets.rows(self.settings.sheet_plan)
        existing = {(str(row.get(post_key)), integer(row.get(version_key), 1)) for row in history}
        missing = [row for row in operation["rows"]
                   if (str(row[post_key]), integer(row[version_key])) not in existing]
        await self.sheets.append_dicts(self.settings.sheet_plan, missing)
        drafts = await self.sheets.rows(self.settings.sheet_texts)
        published = await self._published_plans(drafts)
        previous = {tuple(pair) for pair in operation["previous"]} - published
        for row in history:
            if (str(row.get(post_key)), integer(row.get(version_key), 1)) in previous:
                if row.get(status_key) != PlanDecision.SUPERSEDED.value:
                    await self.sheets.update_dict(self.settings.sheet_plan, int(row["_row_number"]),
                                                  {status_key: PlanDecision.SUPERSEDED.value})
        for draft in drafts:
            pair = (str(first(draft, "post_id", "ID поста", "plan_id")),
                    integer(first(draft, "plan_version", "Версия плана"), 1))
            if pair in previous and first(draft, "Статус", "Статус согласования") != DraftDecision.NEEDS_PREPARATION.value:
                await self._set_draft_status(draft, DraftDecision.NEEDS_PREPARATION.value)
        operation["done"] = True
        await self._store_operation(operation_key, operation)
        return operation["message"]

    async def apply_plan_edit(self, post_id: str, field: str, value: str, operation_key: str) -> str:
        async with self._version_lock:
            result = await self._load_operation(operation_key)
            if result.get("done"):
                return result["message"]
            message = await self.update_plan_field(post_id, field, value, operation_key=operation_key + ":plan")
            revision = await self._load_operation(operation_key + ":plan")
            if revision["changed"] and await self.latest_draft(post_id):
                if field == "datetime":
                    await self._revision_draft(post_id, "Дата изменена — требуется согласование",
                                               operation_key + ":draft", regenerate=False)
                else:
                    await self._revision_draft(post_id, f"План изменён: поле {field}. Перепиши по актуальному плану.",
                                               operation_key + ":draft", regenerate=True)
                message += " Новая версия текста ожидает согласования."
            await self._store_operation(operation_key, {"done": True, "message": message})
            return message

    async def _revision_draft(self, post_id: str, comment: str, operation_key: str,
                              *, regenerate: bool, expected_version: int | None = None) -> dict[str, Any]:
        saved = await self._load_operation(operation_key)
        if not saved:
            current = await self.latest_draft(post_id)
            if not current:
                raise UserFacingError("Черновик не найден")
            current_version = integer(first(current, "draft_version", "Версия"))
            if expected_version is not None and current_version != expected_version:
                raise UserFacingError("Версия для переписывания устарела. Откройте текущий черновик.")
            plan = await self.get_plan(post_id)
            saved = {"plan": plan, "version": await self.database.reserve_draft_version(post_id, current_version + 1),
                     "current_text": str(first(current, "Текст")), "comment": comment}
            await self._store_operation(operation_key, saved)
        if "text" not in saved:
            plan = saved["plan"]
            text = saved["current_text"]
            comment = saved["comment"]
            if regenerate:
                material_ids = set(re.split(r"[,\s]+", str(first(plan, "Материалы", "Ссылки на материалы", "Папка материала"))))
                materials = [row for row in await self.sheets.rows(self.settings.sheet_materials)
                             if str(first(row, "material_id", "ID материала", "Материал ID")) in material_ids]
                facts = "\n".join(str(first(row, "Подтверждённые факты", "Факты")) for row in [*materials, plan])
                channel = str(first(plan, "Канал", "Площадка"))
                channel_rules = [row for row in await self.sheets.rows(self.settings.sheet_channels)
                                 if str(first(row, "Канал", "Площадка", "Название", "channel")) == channel]
                response = await self.llm.json_response(
                    WRITER + "\nПерепиши current_text по editor_comment. Верни новую редакцию, не критику.",
                    json.dumps({"plan": plan, "current_text": text, "confirmed_facts": facts,
                                "materials": materials, "channel_rules": channel_rules,
                                "rules": await self.sheets.rows(self.settings.sheet_rules),
                                "bans": await self.sheets.rows(self.settings.sheet_bans),
                                "tone_references": await self.sheets.rows(self.settings.sheet_references),
                                "editor_comment": comment}, ensure_ascii=False, default=str), max_tokens=3500)
                text = response.get("text")
                if not isinstance(text, str) or not text.strip():
                    raise RuntimeError("Модель не вернула текст новой версии")
            saved["text"] = text
            await self._store_operation(operation_key, saved)
        existing = [row for row in await self.sheets.rows(self.settings.sheet_texts)
                    if str(first(row, "post_id", "ID поста", "plan_id")) == post_id
                    and integer(first(row, "draft_version", "Версия")) == saved["version"]]
        if not existing:
            await self._save_draft(saved["plan"], saved["version"], saved["text"],
                                   DraftDecision.PENDING_REVIEW.value, saved["comment"], "ИИ" if regenerate else "Человек")
        return {"post_id": post_id, "draft_version": saved["version"]}

    async def build_plan(self, job: Job) -> str:
        async with self._version_lock:
            return await self._build_plan(job)

    async def _build_plan(self, job: Job) -> str:
        operation_key = f"plan-job:{job.id}"
        saved = await self._load_operation(operation_key)
        if saved:
            return await self._commit_plan(saved["period_id"], [], operation_key)
        period_id = str(job.payload["period_id"])
        await self._resume_plan_changes(period_id)
        period = await self._period(period_id)
        materials = [row for row in await self.sheets.rows(self.settings.sheet_materials)
                     if str(first(row, "period_id", "ID периода", "Период")) == period_id]
        existing = await self.plan_rows(period_id)
        response = await self.llm.json_response(
            PLANNER,
            json.dumps({"period": period, "materials": materials, "existing_plan": existing,
                        "channels": await self.sheets.rows(self.settings.sheet_channels),
                        "rules": await self.sheets.rows(self.settings.sheet_rules),
                        "bans": await self.sheets.rows(self.settings.sheet_bans),
                        "human_comment": job.payload.get("comment", "")}, ensure_ascii=False, default=str),
            max_tokens=6000)
        headers = await self.sheets.headers(self.settings.sheet_plan)
        post_header = self._required_header(headers, "post_id", "ID поста", "plan_id")
        existing_by_id = {str(first(row, "post_id", "ID поста", "plan_id")): row for row in existing}
        rows = []
        for item in response.get("items", []):
            signature = f"{period_id}:{item.get('topic', '')}:{item.get('channel', '')}:{item.get('date', '')}"
            content_id = item.get("content_id") or short_id("CNT", signature)
            post_id = short_id("POST", f"{content_id}:{item.get('channel', '')}:{item.get('date', '')}")
            current = existing_by_id.get(post_id)
            if current and (str(first(current, "Зафиксировано человеком", "Закреплено")).lower() in {"да", "yes", "true", "1"}
                            or str(first(current, "Автор плана", "Автор")).upper() in {"SMM", "ЧЕЛОВЕК"}):
                continue
            blocked = str(item.get("blocked_reason", ""))
            rows.append(self._map_headers(headers, {
                ("content_id", "ID темы"): content_id, ("post_id", "ID поста", "plan_id"): post_id,
                ("period_id", "ID периода", "Период"): period_id,
                ("Дата", "Дата публикации"): item.get("date", ""), ("Время", "Время публикации"): item.get("time", ""),
                ("Канал", "Площадка"): item.get("channel", ""), ("Рубрика",): item.get("rubric", ""),
                ("Тема",): item.get("topic", ""),
                ("Краткое описание", "Описание", "Краткое описание будущего поста"): item.get("brief", ""),
                ("Зачем это аудитории", "Основание / польза"): item.get("business_value", ""),
                ("Материалы", "Ссылки на материалы", "Папка материала"): ", ".join(item.get("material_ids", [])),
                ("Нужен визуал",): "да" if item.get("visual_needed") else "нет", ("Автор плана", "Автор"): "ИИ",
                ("Режим подготовки",): item.get("preparation_mode", PreparationMode.FROM_MATERIALS.value),
                ("Зафиксировано человеком", "Закреплено"): "нет", ("Версия плана", "plan_version"): 1,
                ("Решение редактора", "Статус"): "Заблокирован — нужен исходник" if blocked else PlanDecision.REVIEW.value,
                ("Комментарий редактора", "Комментарий"): blocked}))
        candidates = {str(first(row, "post_id", "ID поста", "plan_id")): dict(row) for row in existing}
        for row in rows:
            post_id = str(row[post_header])
            candidates[post_id] = {**candidates.get(post_id, {}), **row}
        return await self._commit_plan(period_id, list(candidates.values()), operation_key)

    async def plan_rows(self, period_id: str, review_only: bool = False) -> list[dict[str, Any]]:
        rows = [row for row in await self.sheets.rows(self.settings.sheet_plan)
                if str(first(row, "period_id", "ID периода", "Период")) == period_id]
        rows = self._current_plans(rows)
        if review_only:
            rows = [row for row in rows if str(first(row, "Решение редактора", "Статус")) == PlanDecision.REVIEW.value]
        return sorted(rows, key=lambda row: (str(first(row, "Дата", "Дата публикации")), str(first(row, "Время", "Время публикации"))))

    @serialized_versions
    async def set_plan_decision(self, post_id: str, decision: str, user_id: int, comment: str = "") -> None:
        self.require_approver(user_id)
        headers = await self.sheets.headers(self.settings.sheet_plan)
        row = await self.get_plan(post_id)
        values = {self._required_header(headers, "Решение редактора", "Статус"): decision}
        if comment:
            values[self._required_header(headers, "Комментарий редактора", "Комментарий")] = comment
        await self.sheets.update_dict(self.settings.sheet_plan, int(row["_row_number"]), values)

    async def update_plan_field(self, post_id: str, field: str, value: str, *, operation_key: str) -> str:
        aliases = {"topic": ("Тема",), "brief": ("Краткое описание", "Описание", "Краткое описание будущего поста"),
                   "channel": ("Канал", "Площадка"), "rubric": ("Рубрика",),
                   "materials": ("Материалы", "Ссылки на материалы", "Папка материала"),
                   "facts": ("Подтверждённые факты", "Факты")}
        headers = await self.sheets.headers(self.settings.sheet_plan)
        row = await self.get_plan(post_id)
        updates: dict[str, Any] = {}
        if field == "datetime":
            parts = value.strip().split(maxsplit=1)
            try:
                datetime.strptime(parts[0], "%Y-%m-%d")
                if len(parts) > 1:
                    datetime.strptime(parts[1], "%H:%M")
            except (ValueError, IndexError):
                raise UserFacingError("Неверная дата. Используйте ГГГГ-ММ-ДД ЧЧ:ММ.") from None
            updates[self._required_header(headers, "Дата", "Дата публикации")] = parts[0]
            updates[self._required_header(headers, "Время", "Время публикации")] = parts[1] if len(parts) > 1 else ""
        elif field in aliases:
            updates[self._required_header(headers, *aliases[field])] = value
        else:
            raise UserFacingError("Это поле нельзя изменить через бота")
        updates[self._required_header(headers, "Зафиксировано человеком", "Закреплено")] = "да"
        period_id = str(first(row, "period_id", "ID периода", "Период"))
        await self._resume_plan_changes(period_id)
        candidates = await self.plan_rows(period_id)
        candidates = [{**item, **updates} if str(first(item, "post_id", "ID поста", "plan_id")) == post_id
                      else item for item in candidates]
        return await self._commit_plan(period_id, candidates, operation_key)

    async def approve_period_plan(self, period_id: str, user_id: int) -> int:
        self.require_approver(user_id)
        rows = await self.plan_rows(period_id, review_only=True)
        for row in rows:
            await self.set_plan_decision(str(first(row, "post_id", "ID поста", "plan_id")), PlanDecision.APPROVED.value, user_id)
        return len(rows)

    @serialized_versions
    async def build_drafts(self, job: Job) -> str:
        period_id = str(job.payload["period_id"])
        plans = [row for row in await self.plan_rows(period_id)
                 if str(first(row, "Решение редактора", "Статус")) in {PlanDecision.APPROVED.value, "approved"}]
        by_post = {str(first(row, "post_id", "ID поста", "plan_id")): row for row in await self.latest_drafts()}
        created = blocked = skipped = 0
        for plan in plans:
            post_id = str(first(plan, "post_id", "ID поста", "plan_id"))
            plan_version = integer(first(plan, "Версия плана", "plan_version"), 1)
            if post_id in by_post and integer(first(by_post[post_id], "plan_version", "Версия плана"), 1) == plan_version:
                skipped += 1
                continue
            result = await self._draft_one(plan, "")
            blocked += int(result["status"] == DraftDecision.NEEDS_FACT.value)
            created += int(result["status"] != DraftDecision.NEEDS_FACT.value)
        return f"Черновики: создано {created}, нужен исходник {blocked}, уже актуальны {skipped}."

    async def rewrite_draft(self, job: Job) -> str:
        async with self._version_lock:
            result = await self._revision_draft(
                str(job.payload["post_id"]), str(job.payload.get("comment", "")),
                f"rewrite-job:{job.id}", regenerate=True,
                expected_version=integer(job.payload["draft_version"]) if "draft_version" in job.payload else None)
            return f"Создана версия {result['draft_version']} поста {result['post_id']}."

    async def _draft_one(self, plan: dict[str, Any], comment: str) -> dict[str, Any]:
        post_id = str(first(plan, "post_id", "ID поста", "plan_id"))
        rubric = str(first(plan, "Рубрика"))
        material_ids = str(first(plan, "Материалы", "Ссылки на материалы", "Папка материала"))
        materials = []
        for row in await self.sheets.rows(self.settings.sheet_materials):
            material_id = str(first(row, "material_id", "ID материала", "Материал ID"))
            if material_id and material_id in material_ids:
                materials.append(row)
        facts = "\n".join(value for value in [*(str(first(row, "Подтверждённые факты", "Факты")) for row in materials),
                                                  str(first(plan, "Подтверждённые факты", "Факты"))] if value.strip())
        hard = rubric in HARD_FACT_RUBRICS or any(token in rubric.lower() for token in ("pr", "закон", "цифр", "кейс", "компан"))
        mode = str(first(plan, "Режим подготовки", default=PreparationMode.FROM_MATERIALS.value))
        supplied = str(first(plan, "Готовый текст", "Исходный текст"))
        versions = [integer(first(row, "draft_version", "Версия")) for row in await self.sheets.rows(self.settings.sheet_texts)
                    if str(first(row, "post_id", "ID поста", "plan_id")) == post_id]
        version = await self.database.reserve_draft_version(post_id, max(versions, default=0) + 1)
        if mode in {PreparationMode.ADAPT_TEXT.value, PreparationMode.CHECK_ONLY.value} and not supplied.strip():
            text, status, note = "", DraftDecision.NEEDS_FACT.value, "Не указан готовый текст"
        elif hard and not facts.strip():
            text, status, note = "", DraftDecision.NEEDS_FACT.value, "Для этой рубрики нужен подтверждённый исходник"
        elif mode == PreparationMode.CHECK_ONLY.value:
            text, status, note = supplied, DraftDecision.REVIEW.value, ""
        else:
            response = await self.llm.json_response(
                WRITER,
                json.dumps({"plan": plan, "materials": materials, "confirmed_facts": facts,
                            "supplied_text": supplied, "preparation_mode": mode,
                            "rules": await self.sheets.rows(self.settings.sheet_rules),
                            "bans": await self.sheets.rows(self.settings.sheet_bans),
                            "tone_references": await self.sheets.rows(self.settings.sheet_references),
                            "editor_comment": comment}, ensure_ascii=False, default=str), max_tokens=3500)
            text = str(response.get("text", ""))
            note = "\n".join(str(item) for item in response.get("missing_facts", []))
            status = DraftDecision.REVIEW.value
        return await self._save_draft(plan, version, text, status, note, "ИИ")

    @serialized_versions
    async def create_manual_version(self, post_id: str, text: str, user_id: int) -> dict[str, Any]:
        current = await self.latest_draft(post_id)
        version = await self.database.reserve_draft_version(post_id, integer(first(current or {}, "draft_version", "Версия"), 0) + 1)
        return await self._save_draft(await self.get_plan(post_id), version, text, DraftDecision.REVIEW.value,
                                      f"Ручная версия от {user_id}", "Человек")

    async def _save_draft(self, plan: dict[str, Any], version: int, text: str, status: str,
                          note: str, author: str) -> dict[str, Any]:
        post_id = str(first(plan, "post_id", "ID поста", "plan_id"))
        plan_version = integer(first(plan, "Версия плана", "plan_version"), 1)
        fingerprint, _ = await self._content_fingerprint(plan, version, text)
        headers = await self.sheets.headers(self.settings.sheet_texts)
        row = self._map_headers(headers, {
            ("version_id", "ID версии"): f"{post_id}-V{version}", ("post_id", "ID поста", "plan_id"): post_id,
            ("plan_version", "Версия плана"): plan_version, ("draft_version", "Версия"): version,
            ("Канал", "Площадка"): first(plan, "Канал", "Площадка"), ("Текст",): text,
            ("Источники",): first(plan, "Материалы", "Ссылки на материалы", "Папка материала"),
            ("Медиа", "Активные медиа"): first(plan, "Папка медиа", "Медиа"),
            ("Статус", "Статус согласования"): status,
            ("Комментарий согласующего", "Комментарий человека"): note, ("Автор версии",): author,
            ("Создано",): utc_now(), ("approval_fingerprint", "Хеш одобренной версии", "content_hash"): fingerprint})
        await self.sheets.upsert_dicts(self.settings.sheet_texts, self._required_header(headers, "version_id", "ID версии"), [row])
        return {"post_id": post_id, "draft_version": version, "plan_version": plan_version,
                "fingerprint": fingerprint, "status": status, "text": text}

    @serialized_versions
    async def create_extra_post(self, job: Job) -> str:
        payload = job.payload
        payload.setdefault("sources", "\n".join(extract_urls(f"{payload.get('brief', '')}\n{payload.get('facts', '')}")))
        content_id = short_id("CNT", json.dumps(payload, ensure_ascii=False, sort_keys=True))
        post_id = short_id("POST", f"{content_id}:{payload.get('channel', '')}:{utc_now()}")
        headers = await self.sheets.headers(self.settings.sheet_plan)
        row = self._map_headers(headers, {
            ("content_id", "ID темы"): content_id, ("post_id", "ID поста", "plan_id"): post_id,
            ("period_id", "ID периода", "Период"): payload.get("period_id", "Внеплановые"),
            ("Дата", "Дата публикации"): payload.get("date", ""), ("Время", "Время публикации"): payload.get("time", ""),
            ("Канал", "Площадка"): payload.get("channel", "Telegram"), ("Рубрика",): payload.get("rubric", ""),
            ("Тема",): payload.get("topic", ""),
            ("Краткое описание", "Описание", "Краткое описание будущего поста"): payload.get("brief", ""),
            ("Материалы", "Ссылки на материалы", "Папка материала"): payload.get("sources", ""),
            ("Подтверждённые факты", "Факты"): payload.get("facts", ""),
            ("Готовый текст", "Исходный текст"): payload.get("ready_text", ""),
            ("Автор плана", "Автор"): "SMM", ("Тип",): payload.get("post_type", "Внеплановый"),
            ("Режим подготовки",): payload.get("mode", PreparationMode.FROM_BRIEF.value),
            ("Зафиксировано человеком", "Закреплено"): "да", ("Версия плана", "plan_version"): 1,
            ("Решение редактора", "Статус"): PlanDecision.APPROVED.value})
        key = self._required_header(headers, "post_id", "ID поста", "plan_id")
        await self.sheets.upsert_dicts(self.settings.sheet_plan, key, [row])
        result = await self._draft_one(await self.get_plan(post_id), str(payload.get("comment", "")))
        return f"Создан {post_id}, версия {result['draft_version']}. Откройте «Черновики» для согласования и добавления медиа."

    async def get_plan(self, post_id: str) -> dict[str, Any]:
        rows = self._current_plans(await self.sheets.rows(self.settings.sheet_plan))
        for row in rows:
            if str(first(row, "post_id", "ID поста", "plan_id")) == post_id:
                return row
        raise UserFacingError(f"Пост {post_id} не найден")

    async def latest_draft(self, post_id: str) -> dict[str, Any] | None:
        rows = [row for row in await self.sheets.rows(self.settings.sheet_texts)
                if str(first(row, "post_id", "ID поста", "plan_id")) == post_id]
        return max(rows, key=lambda row: integer(first(row, "draft_version", "Версия"))) if rows else None

    async def latest_drafts(self, status: str | None = None) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for row in await self.sheets.rows(self.settings.sheet_texts):
            post_id = str(first(row, "post_id", "ID поста", "plan_id"))
            if post_id and (post_id not in grouped or integer(first(row, "draft_version", "Версия")) > integer(first(grouped[post_id], "draft_version", "Версия"))):
                grouped[post_id] = row
        rows = list(grouped.values())
        return [row for row in rows if str(first(row, "Статус", "Статус согласования")) == status] if status else rows

    @serialized_versions
    async def approve_draft(self, post_id: str, draft_version: int, decision: str,
                            user_id: int, comment: str = "") -> dict[str, Any]:
        self.require_approver(user_id)
        if decision not in {DraftDecision.APPROVED.value, DraftDecision.REJECTED.value}:
            raise UserFacingError("Технический статус не является решением согласующего")
        latest = await self.latest_draft(post_id)
        if not latest:
            raise UserFacingError("Черновик не найден")
        actual = integer(first(latest, "draft_version", "Версия"))
        if actual != draft_version:
            raise UserFacingError(f"Версия {draft_version} устарела; текущая {actual}")
        fingerprint = str(first(latest, "approval_fingerprint", "Хеш одобренной версии", "content_hash"))
        if decision == DraftDecision.APPROVED.value:
            if first(latest, "Статус", "Статус согласования") == DraftDecision.NEEDS_PREPARATION.value:
                raise UserFacingError("План изменён. Сначала подготовьте новую версию текста.")
            await self._verify_content(post_id, latest, require_approval=False)
        await self.database.record_approval(post_id, integer(first(latest, "plan_version", "Версия плана"), 1),
                                            actual, fingerprint, decision, user_id)
        headers = await self.sheets.headers(self.settings.sheet_texts)
        updates = {self._required_header(headers, "Статус", "Статус согласования"): decision}
        if comment:
            updates[self._required_header(headers, "Комментарий согласующего", "Комментарий человека")] = comment
        await self.sheets.update_dict(self.settings.sheet_texts, int(latest["_row_number"]), updates)
        latest.update(updates)
        return latest

    async def add_reference(self, post_id: str, what_to_copy: str, comment: str, user_id: int) -> None:
        draft, plan = await self.latest_draft(post_id), await self.get_plan(post_id)
        if not draft:
            raise UserFacingError("Черновик не найден")
        headers = await self.sheets.headers(self.settings.sheet_references)
        row = self._map_headers(headers, {
            ("Название / тема", "Название", "Тема"): first(plan, "Тема"),
            ("Текст поста", "Текст"): first(draft, "Текст"), ("Канал", "Площадка"): first(plan, "Канал", "Площадка"),
            ("Рубрика",): first(plan, "Рубрика"), ("Что нравится / перенять", "Что перенять"): what_to_copy,
            ("Комментарий",): comment, ("Добавлено", "Дата добавления"): utc_now(), ("Добавил", "Автор"): str(user_id)})
        await self.sheets.append_dicts(self.settings.sheet_references, [row])

    async def ensure_media_folder(self, post_id: str) -> str:
        plan = await self.get_plan(post_id)
        existing = str(first(plan, "Папка медиа", "Медиа"))
        if existing:
            return existing
        period_id = str(first(plan, "period_id", "ID периода", "Период"))
        if period_id in {"Внеплановые", "Тесты"}:
            root = await self.workspace.ensure_folder(period_id, self.settings.google_drive_root_folder_id)
            period_folder = str(root["id"])
        else:
            period = await self._period(period_id)
            period_folder = str(first(period, "Ссылка на папку", "Папка Drive", "Папка материалов"))
        date_folder = await self.workspace.ensure_folder(str(first(plan, "Дата", "Дата публикации")) or "Без даты", period_folder)
        topic = re.sub(r"[^\w\- А-Яа-яЁё]", "", str(first(plan, "Тема")))[:60].strip()
        post_folder = await self.workspace.ensure_folder(f"{post_id} — {topic}".rstrip(" —"), str(date_folder["id"]))
        link = str(post_folder.get("webViewLink") or post_folder["id"])
        headers = await self.sheets.headers(self.settings.sheet_plan)
        await self.sheets.update_dict(self.settings.sheet_plan, int(plan["_row_number"]),
                                      {self._required_header(headers, "Папка медиа", "Медиа"): link})
        return link

    async def replace_media(self, post_id: str) -> int:
        return await self.workspace.trash_folder_files(await self.ensure_media_folder(post_id))

    async def upload_media(self, post_id: str, name: str, mime_type: str, data: bytes) -> dict[str, Any]:
        return await self.workspace.upload_bytes(name, mime_type, data, await self.ensure_media_folder(post_id))

    async def media_entries(self, post_id: str) -> list[DriveEntry]:
        folder = str(first(await self.get_plan(post_id), "Папка медиа", "Медиа"))
        return await self.workspace.list_direct_files(folder) if folder else []

    @serialized_versions
    async def sync_media(self, job: Job) -> str:
        post_id = str(job.payload["post_id"])
        latest = await self.latest_draft(post_id)
        if not latest:
            raise UserFacingError("У поста ещё нет текста")
        result = await self._save_draft(await self.get_plan(post_id), await self.database.reserve_draft_version(post_id, integer(first(latest, "draft_version", "Версия")) + 1),
                                        str(first(latest, "Текст")), DraftDecision.REVIEW.value,
                                        "Набор медиа изменён — требуется повторное согласование", "Человек")
        return f"Медиа обновлены. Создана версия {result['draft_version']} для повторного согласования."

    @serialized_versions
    async def refresh_draft_version(self, post_id: str, note: str) -> dict[str, Any] | None:
        latest = await self.latest_draft(post_id)
        if not latest:
            return None
        return await self._save_draft(
            await self.get_plan(post_id),
            await self.database.reserve_draft_version(post_id, integer(first(latest, "draft_version", "Версия")) + 1),
            str(first(latest, "Текст")),
            DraftDecision.REVIEW.value,
            note,
            "Человек",
        )

    def require_approver(self, user_id: int) -> None:
        if not self.settings.is_approver(user_id):
            raise UserFacingError("У вас нет права согласования и публикации")

    async def _set_draft_status(self, draft: dict[str, Any], status: str, **extra: Any) -> None:
        headers = await self.sheets.headers(self.settings.sheet_texts)
        updates = {self._required_header(headers, "Статус", "Статус согласования"): status, **extra}
        await self.sheets.update_dict(self.settings.sheet_texts, int(draft["_row_number"]), updates)
        draft.update(updates)

    async def _content_fingerprint(self, plan: dict[str, Any], version: int, text: str) -> tuple[str, list[MediaAsset]]:
        folder = str(first(plan, "Папка медиа", "Медиа"))
        entries = await self.workspace.list_direct_files(folder) if folder else []
        entries = sorted(entries, key=lambda item: (item.name, item.id))
        if len(entries) > 10:
            raise FileLimitError("У поста не может быть более 10 медиа")
        total = 0
        assets, manifest = [], []
        for index, entry in enumerate(entries):
            remaining = self.settings.max_post_media_bytes - total
            if remaining <= 0:
                raise FileLimitError("Превышен суммарный размер медиа поста")
            data, mime = await self.workspace.download_file(entry, min(self.settings.max_media_bytes, remaining))
            total += len(data)
            assets.append(MediaAsset(entry.name, mime, data))
            manifest.append({"id": entry.id, "name": entry.name, "size": len(data), "mime": mime,
                             "sha256": hashlib.sha256(data).hexdigest(), "order": index})
        return stable_hash({"post_id": first(plan, "post_id", "ID поста", "plan_id"),
                            "plan_version": integer(first(plan, "Версия плана", "plan_version"), 1),
                            "draft_version": version, "text": text, "folder": folder, "media": manifest,
                            "channel": first(plan, "Канал", "Площадка"),
                            "date": first(plan, "Дата", "Дата публикации"),
                            "time": first(plan, "Время", "Время публикации")}), assets

    async def _verify_content(self, post_id: str, draft: dict[str, Any], *, require_approval: bool = True) -> list[MediaAsset]:
        plan = await self.get_plan(post_id)
        version = integer(first(draft, "draft_version", "Версия"))
        plan_version = integer(first(plan, "plan_version", "Версия плана"), 1)
        fingerprint, assets = await self._content_fingerprint(plan, version, str(first(draft, "Текст")))
        saved = str(first(draft, "approval_fingerprint", "Хеш одобренной версии", "content_hash"))
        if fingerprint != saved or integer(first(draft, "plan_version", "Версия плана"), 1) != plan_version:
            # Refresh the review snapshot so that an explicit new approval can succeed.
            headers = await self.sheets.headers(self.settings.sheet_texts)
            await self._set_draft_status(draft, DraftDecision.REVIEW.value, **{
                self._required_header(headers, "approval_fingerprint", "Хеш одобренной версии", "content_hash"): fingerprint,
                self._required_header(headers, "plan_version", "Версия плана"): plan_version,
            })
            await self.database.record_approval(post_id, plan_version, version, fingerprint, DraftDecision.REVIEW.value, 0)
            raise UserFacingError("После согласования изменились текст, параметры или медиа. Требуется повторное согласование")
        if require_approval:
            approval = await self.database.get_approval(post_id, plan_version, version)
            if not approval or approval["decision"] != DraftDecision.APPROVED.value or approval["fingerprint"] != fingerprint or not self.settings.is_approver(approval["decided_by"]):
                await self._set_draft_status(draft, DraftDecision.REVIEW.value)
                raise UserFacingError("Требуется повторное согласование")
        return assets

    @serialized_versions
    async def schedule_draft(self, post_id: str, version: int, user_id: int) -> None:
        self.require_approver(user_id)
        draft = await self.latest_draft(post_id)
        if not draft or integer(first(draft, "draft_version", "Версия")) != version:
            raise UserFacingError("Версия устарела")
        if first(draft, "Статус", "Статус согласования") not in {DraftDecision.APPROVED.value, DraftDecision.TEST_SCHEDULED.value}:
            raise UserFacingError("Планировать можно только одобренную версию")
        await self._verify_content(post_id, draft)
        await self._set_draft_status(draft, DraftDecision.TEST_SCHEDULED.value)

    @serialized_versions
    async def publish_test(self, job: Job) -> str:
        self.require_approver(job.requested_by)
        post_id = str(job.payload["post_id"])
        expected = integer(job.payload.get("draft_version"))
        latest = await self.latest_draft(post_id)
        if not latest:
            raise UserFacingError("Черновик не найден")
        actual = integer(first(latest, "draft_version", "Версия"))
        if actual != expected:
            raise UserFacingError(f"Публикация отменена: версия {expected} устарела, текущая {actual}")
        if str(first(latest, "Статус", "Статус согласования")) not in {DraftDecision.APPROVED.value, DraftDecision.TEST_SCHEDULED.value}:
            raise UserFacingError("Публиковать можно только одобренную версию")
        assets = await self._verify_content(post_id, latest)
        scheduled = str(job.payload.get("scheduled_for") or "") or None
        try:
            message_ids = await self.publisher.publish_test(str(first(latest, "Текст")), assets,
                                                            long_text_mode=str(job.payload.get("long_text_mode", "caption")))
        except Exception as exc:
            await self.database.record_publication(post_id, actual, "telegram_test", "error", job.requested_by,
                                                   scheduled, error=type(exc).__name__)
            raise
        await self.database.record_publication(post_id, actual, "telegram_test", "done", job.requested_by,
                                               scheduled, message_ids=message_ids)
        await self._set_draft_status(latest, DraftDecision.TEST_PUBLISHED.value)
        return f"Пост {post_id} отправлен в тестовый канал. Сообщений: {len(message_ids)}."

    @staticmethod
    def _map_headers(headers: list[str], values: dict[tuple[str, ...], Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for aliases, value in values.items():
            for alias in aliases:
                if alias in headers:
                    result[alias] = value
                    break
        return result

    @staticmethod
    def _required_header(headers: list[str], *aliases: str) -> str:
        for alias in aliases:
            if alias in headers:
                return alias
        raise ValueError(f"Не найден обязательный столбец: {' / '.join(aliases)}")
