from __future__ import annotations
import asyncio
import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
import httpx
from .security import telegram_response, UserFacingError
from .config import Settings
from .db import Database
from .domain import DraftDecision, JobType, PlanDecision, PreparationMode, TelegramEvent
from .pipelines import PipelineService, first, integer
from .utils import stable_hash


class TelegramAPI:
    def __init__(self, token: str, long_poll_seconds: int) -> None:
        self.token = token
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.file_url = f"https://api.telegram.org/file/bot{token}"
        self.long_poll_seconds = long_poll_seconds
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(long_poll_seconds + 30))

    async def close(self) -> None:
        await self.client.aclose()

    async def call(self, method: str, payload: dict[str, Any] | None = None) -> Any:
        response = await self.client.post(f"{self.base_url}/{method}", json=payload or {})
        return telegram_response(response, method, self.token)

    async def delete_webhook(self) -> None:
        await self.call("deleteWebhook", {"drop_pending_updates": False})

    async def updates(self, offset: int) -> list[dict[str, Any]]:
        return await self.call("getUpdates", {"offset": offset, "limit": 100,
                                               "timeout": self.long_poll_seconds,
                                               "allowed_updates": ["message", "callback_query"]})

    async def send_message(self, chat_id: int, text: str,
                           reply_markup: dict[str, Any] | None = None) -> dict[str, Any]:
        chunks = [text[index:index + 4096] for index in range(0, len(text), 4096)] or [""]
        result: dict[str, Any] = {}
        for index, chunk in enumerate(chunks):
            payload: dict[str, Any] = {"chat_id": chat_id, "text": chunk, "disable_web_page_preview": True}
            if reply_markup and index == len(chunks) - 1:
                payload["reply_markup"] = reply_markup
            result = await self.call("sendMessage", payload)
        return result

    async def answer_callback(self, callback_id: str, text: str = "") -> None:
        await self.call("answerCallbackQuery", {"callback_query_id": callback_id, "text": text[:200]})

    async def get_file(self, file_id: str) -> dict[str, Any]:
        return await self.call("getFile", {"file_id": file_id})

    async def download_file(self, file_path: str, max_bytes: int = 20_000_000) -> bytes:
        async with self.client.stream("GET", f"{self.file_url}/{file_path}") as response:
            telegram_response(response, "downloadFile", self.token, file=True)
            data = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=64_000):
                if len(data) + len(chunk) > max_bytes:
                    raise UserFacingError("Файл превышает допустимый размер")
                data.extend(chunk)
            return bytes(data)

    async def send_document(self, chat_id: int, name: str, content: bytes, caption: str = "") -> dict[str, Any]:
        response = await self.client.post(f"{self.base_url}/sendDocument",
                                          data={"chat_id": str(chat_id), "caption": caption[:1024]},
                                          files={"document": (name, content, "text/plain; charset=utf-8")})
        return telegram_response(response, "sendDocument", self.token)

    async def send_media_preview(self, chat_id: int, name: str, mime_type: str,
                                 content: bytes, caption: str = "") -> dict[str, Any]:
        if mime_type.startswith("image/"):
            method, field = "sendPhoto", "photo"
        elif mime_type.startswith("video/"):
            method, field = "sendVideo", "video"
        else:
            method, field = "sendDocument", "document"
        response = await self.client.post(
            f"{self.base_url}/{method}",
            data={"chat_id": str(chat_id), "caption": caption[:1024]},
            files={field: (name, content, mime_type)},
        )
        return telegram_response(response, method, self.token)


def inline_keyboard(rows: list[list[tuple[str, str]]]) -> dict[str, Any]:
    return {"inline_keyboard": [[{"text": text, "callback_data": value} for text, value in row] for row in rows]}


class TelegramController:
    def __init__(self, settings: Settings, api: TelegramAPI,
                 database: Database, pipelines: PipelineService) -> None:
        self.settings = settings
        self.api = api
        self.database = database
        self.pipelines = pipelines
        self.log = logging.getLogger(__name__)

    async def run(self) -> None:
        await self.api.delete_webhook()
        self.log.info("Control bot long polling started")
        while True:
            try:
                pending = await self.database.pending_telegram_updates()
                if not pending:
                    offset = int(await self.database.get_setting("telegram_offset", "0") or "0")
                    await self.database.save_telegram_updates(await self.api.updates(offset))
                    pending = await self.database.pending_telegram_updates()
                for raw in pending:
                    event = self._normalize(raw)
                    if not await self.database.is_update_processed(event.update_id):
                        if event.callback_id:
                            try:
                                await self.api.answer_callback(event.callback_id)
                            except Exception:
                                self.log.warning("Could not acknowledge Telegram callback", exc_info=True)
                        try:
                            await self.handle(event)
                        except UserFacingError as exc:
                            await self.api.send_message(event.chat_id, str(exc))
                    await self.database.mark_update_processed(event.update_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Telegram polling failed")
                await asyncio.sleep(3)

    @staticmethod
    def _normalize(raw: dict[str, Any]) -> TelegramEvent:
        if "callback_query" in raw:
            callback = raw["callback_query"]
            message = callback.get("message", {})
            return TelegramEvent(update_id=int(raw["update_id"]), user_id=int(callback["from"]["id"]),
                                 chat_id=int(message.get("chat", {}).get("id", callback["from"]["id"])),
                                 kind="callback", callback_id=str(callback["id"]),
                                 callback_data=str(callback.get("data", "")), message_id=message.get("message_id"), raw=raw)
        message = raw.get("message", {})
        return TelegramEvent(update_id=int(raw["update_id"]), user_id=int(message.get("from", {}).get("id", 0)),
                             chat_id=int(message.get("chat", {}).get("id", 0)), kind="message",
                             text=str(message.get("text") or message.get("caption") or ""),
                             message_id=message.get("message_id"), raw=raw)

    async def handle(self, event: TelegramEvent) -> None:
        if event.user_id not in self.settings.telegram_allowed_user_ids:
            self.log.warning("Rejected Telegram user %s", event.user_id)
            return
        if event.kind == "callback":
            await self._callback(event)
            return
        message = event.raw.get("message", {})
        if (message.get("photo") or message.get("video")) and not event.text.strip():
            await self.api.send_message(event.chat_id, "Файл не принят. Добавьте короткое описание в подпись или отправьте медиа как документ с понятным названием")
            return
        command = (event.text.strip().split(maxsplit=1) or [""])[0].lower()
        commands = {"/start": self.show_menu, "/menu": self.show_menu, "/status": self.show_status,
                    "/drafts": self.show_drafts, "/export": self.export_drafts}
        if command in commands:
            await commands[command](event.chat_id)
            return
        if command == "/cancel":
            await self.database.clear_session(event.user_id)
            await self.api.send_message(event.chat_id, "Действие отменено.")
            return
        if command in {"/materials", "/plan"}:
            await self._period_menu(event, "materials" if command == "/materials" else "plan")
            return
        if command in {"/extra", "/test"}:
            await self._begin_extra(event, test=command == "/test")
            return
        session = await self.database.get_session(event.user_id)
        if session:
            await self._session_message(event, session)
        else:
            await self.show_menu(event.chat_id, "Выберите действие.")

    async def show_menu(self, chat_id: int, prefix: str = "") -> None:
        await self.api.send_message(chat_id, (prefix + "\n\n" if prefix else "") + "Управление SMM КИФА",
                                    inline_keyboard([
                                        [("Разобрать материалы", "m:materials"), ("Сформировать план", "m:plan")],
                                        [("Согласовать план", "m:plan_review"), ("Подготовить тексты", "m:build_drafts")],
                                        [("Черновики", "m:drafts"), ("Внеплановый пост", "m:extra")],
                                        [("Тестовый пост", "m:test"), ("Экспорт", "m:export")],
                                        [("Статус", "m:status")],
                                    ]))

    async def _callback(self, event: TelegramEvent) -> None:
        data = event.callback_data
        if data.startswith("m:"):
            action = data[2:]
            if action in {"materials", "plan", "plan_review", "build_drafts"}:
                await self._period_menu(event, action)
            elif action == "drafts": await self.show_drafts(event.chat_id)
            elif action == "extra": await self._begin_extra(event)
            elif action == "test": await self._begin_extra(event, True)
            elif action == "export": await self.export_drafts(event.chat_id)
            elif action == "status": await self.show_status(event.chat_id)
            return
        if data.startswith("per:"):
            index = int(data[4:])
            session = await self.database.get_session(event.user_id)
            if not session or session["mode"] != "period_select":
                raise UserFacingError("Список периодов устарел")
            choice = session["payload"]["choices"][index]
            action, period_id = session["step"], choice["period_id"]
            if action == "plan_review":
                await self.show_plan(event.chat_id, period_id)
            else:
                mapping = {"materials": JobType.PROCESS_MATERIALS, "plan": JobType.BUILD_PLAN,
                           "build_drafts": JobType.BUILD_DRAFTS}
                await self._enqueue(event, mapping[action], {"period_id": period_id})
            await self.database.clear_session(event.user_id)
            return
        if data.startswith("pa:"):
            _, action, post_id = data.split(":", 2)
            if not self.settings.is_approver(event.user_id):
                raise UserFacingError("У вас нет права согласования")
            if action == "ok":
                await self.pipelines.set_plan_decision(post_id, PlanDecision.APPROVED.value, event.user_id)
                await self.api.send_message(event.chat_id, "Пункт плана одобрен.")
            elif action == "no":
                await self.pipelines.set_plan_decision(post_id, PlanDecision.REJECTED.value, event.user_id)
                await self.api.send_message(event.chat_id, "Пункт плана отклонён.")
            elif action == "edit":
                await self.database.set_session(event.user_id, event.chat_id, "plan_edit", "field", {"post_id": post_id})
                await self.api.send_message(event.chat_id, "Что изменить?",
                                            inline_keyboard([[("Тема", "pf:topic"), ("Описание", "pf:brief")],
                                                             [("Канал", "pf:channel"), ("Дата и время", "pf:datetime")],
                                                             [("Рубрика", "pf:rubric"), ("Материалы", "pf:materials")],
                                                             [("Факты", "pf:facts")]]))
            return
        if data.startswith("pall:"):
            if not self.settings.is_approver(event.user_id): raise UserFacingError("У вас нет права согласования")
            count = await self.pipelines.approve_period_plan(data.split(":", 1)[1], event.user_id)
            await self.api.send_message(event.chat_id, f"Одобрено пунктов: {count}.")
            return
        if data.startswith("pf:"):
            session = await self.database.get_session(event.user_id)
            if not session or session["mode"] != "plan_edit": raise UserFacingError("Диалог устарел")
            payload = session["payload"]
            payload["field"] = data[3:]
            await self.database.set_session(event.user_id, event.chat_id, "plan_edit", "value", payload)
            await self.api.send_message(event.chat_id, "Пришлите новое значение целиком. Для даты: ГГГГ-ММ-ДД ЧЧ:ММ.")
            return
        if data.startswith("xm:"):
            await self._callback_extra_mode(event, data == "xm:ready")
            return
        if data.startswith(("da:", "dr:", "dm:", "dai:", "dmed:", "dd:", "dp:", "dt:")):
            await self._draft_callback(event, data)
            return
        if data.startswith("med:"):
            _, action, post_id = data.split(":", 2)
            if action == "cancel":
                await self.database.clear_session(event.user_id)
                await self.api.send_message(event.chat_id, "Изменение медиа отменено.")
                return
            if action == "done":
                await self._enqueue(event, JobType.SYNC_MEDIA, {"post_id": post_id})
                await self.database.clear_session(event.user_id)
                return
            if action == "replace":
                await self.api.send_message(
                    event.chat_id,
                    "Точно заменить весь текущий набор? Старые файлы будут перемещены в корзину Google Drive.",
                    inline_keyboard([[("Да, заменить", f"med:replaceok:{post_id}"),
                                      ("Отмена", f"med:cancel:{post_id}")]]),
                )
                return
            if action == "replaceok":
                removed = await self.pipelines.replace_media(post_id)
            else:
                removed = 0
            await self.database.set_session(event.user_id, event.chat_id, "media", "files",
                                            {"post_id": post_id, "files": [], "action": action})
            await self.api.send_message(event.chat_id,
                                        f"Старых файлов удалено: {removed}. Отправьте фото, видео или документ. "
                                        "Лучше как документ — так Telegram не сожмёт оригинал. Когда закончите, нажмите «Готово».",
                                        inline_keyboard([[("Готово", f"med:done:{post_id}"), ("Отмена", f"med:cancel:{post_id}")]]))
            return
        if data.startswith("et:"):
            _, answer, post_id = data.split(":", 2)
            if answer == "no":
                await self._publication_options(event.chat_id, post_id)
            else:
                await self.api.send_message(event.chat_id, "Что стоит перенять из этого поста?",
                                            inline_keyboard([[("Тон", f"ec:tone:{post_id}"), ("Структуру", f"ec:structure:{post_id}")],
                                                             [("Подачу фактов", f"ec:facts:{post_id}"), ("Начало", f"ec:start:{post_id}")],
                                                             [("Формат", f"ec:format:{post_id}"), ("Весь пост", f"ec:whole:{post_id}")]]))
            return
        if data.startswith("ec:"):
            _, kind, post_id = data.split(":", 2)
            names = {"tone": "тон", "structure": "структуру", "facts": "подачу фактов",
                     "start": "начало", "format": "формат", "whole": "весь пост"}
            await self.database.set_session(event.user_id, event.chat_id, "etalon", "comment",
                                            {"post_id": post_id, "what": names[kind]})
            await self.api.send_message(event.chat_id, "Добавьте короткий комментарий или отправьте «без комментария».")
            return
        if data.startswith("pub:"):
            self.pipelines.require_approver(event.user_id)
            _, action, post_id, version = data.split(":", 3)
            if action == "now":
                await self._enqueue(event, JobType.PUBLISH_TEST, {"post_id": post_id, "draft_version": int(version)})
            elif action == "schedule":
                await self.database.set_session(event.user_id, event.chat_id, "publish", "datetime",
                                                {"post_id": post_id, "draft_version": int(version)})
                await self.api.send_message(event.chat_id, "Когда отправить в тестовый канал? ГГГГ-ММ-ДД ЧЧ:ММ")
            return

    async def _draft_callback(self, event: TelegramEvent, data: str) -> None:
        prefix, post_id, version_raw = data.split(":", 2)
        version = int(version_raw)
        if prefix == "da":
            if not self.settings.is_approver(event.user_id): raise UserFacingError("У вас нет права согласования")
            await self.pipelines.approve_draft(post_id, version, DraftDecision.APPROVED.value, event.user_id)
            await self.api.send_message(event.chat_id, "Текст одобрен. Добавить его в «Эталоны»?",
                                        inline_keyboard([[("Да", f"et:yes:{post_id}"), ("Нет", f"et:no:{post_id}")]]))
        elif prefix == "dr":
            await self.database.set_session(event.user_id, event.chat_id, "rewrite", "comment",
                                            {"post_id": post_id, "draft_version": version})
            await self.api.send_message(event.chat_id, "Напишите инструкцию для новой ИИ-версии.")
        elif prefix == "dm":
            await self.database.set_session(event.user_id, event.chat_id, "manual", "text", {"post_id": post_id})
            await self.api.send_message(event.chat_id, "Пришлите исправленный текст целиком одним сообщением.")
        elif prefix == "dai":
            await self.database.set_session(event.user_id, event.chat_id, "rewrite", "comment",
                                            {"post_id": post_id, "draft_version": version})
            await self.api.send_message(event.chat_id, "Что изменить? Например: «сократить до 900 знаков».")
        elif prefix == "dmed":
            await self.api.send_message(event.chat_id, "Как изменить медиа?",
                                        inline_keyboard([[("Добавить", f"med:add:{post_id}"),
                                                          ("Заменить всё", f"med:replace:{post_id}")],
                                                         [("Я изменил папку Drive", f"med:done:{post_id}")]]))
        elif prefix == "dd":
            await self.database.set_session(event.user_id, event.chat_id, "plan_edit", "value",
                                            {"post_id": post_id, "field": "datetime"})
            await self.api.send_message(event.chat_id, "Пришлите новую дату и время: ГГГГ-ММ-ДД ЧЧ:ММ.")
        elif prefix == "dp":
            latest = await self.pipelines.latest_draft(post_id)
            if not latest or integer(first(latest, "draft_version", "Версия")) != version:
                raise UserFacingError("Эта версия уже устарела")
            entries = await self.pipelines.media_entries(post_id)
            if not entries:
                await self.api.send_message(event.chat_id, "У поста нет медиа.\n\n" + str(first(latest, "Текст")))
            else:
                for index, entry in enumerate(entries):
                    content, mime = await self.pipelines.workspace.download_file(entry)
                    await self.api.send_media_preview(
                        event.chat_id, entry.name, mime, content,
                        str(first(latest, "Текст")) if index == 0 and len(str(first(latest, "Текст"))) <= 1024 else "",
                    )
                if len(str(first(latest, "Текст"))) > 1024:
                    await self.api.send_message(event.chat_id, str(first(latest, "Текст")))
        elif prefix == "dt":
            await self.pipelines.approve_draft(post_id, version, DraftDecision.REJECTED.value, event.user_id)
            await self.api.send_message(event.chat_id, "Черновик отклонён.")

    async def _period_menu(self, event: TelegramEvent, action: str) -> None:
        periods = await self.pipelines.list_periods()
        if not periods:
            await self.api.send_message(event.chat_id, "В таблице нет периодов.")
            return
        await self.database.set_session(event.user_id, event.chat_id, "period_select", action, {"choices": periods})
        await self.api.send_message(event.chat_id, "Выберите период:",
                                    inline_keyboard([[(item["title"], f"per:{index}")] for index, item in enumerate(periods[:30])]))

    async def show_plan(self, chat_id: int, period_id: str) -> None:
        rows = await self.pipelines.plan_rows(period_id)
        if not rows:
            await self.api.send_message(chat_id, "Для периода ещё нет плана.")
            return
        for row in rows:
            post_id = str(first(row, "post_id", "ID поста", "plan_id"))
            text = (f"{first(row, 'Дата', 'Дата публикации')} {first(row, 'Время', 'Время публикации')} · "
                    f"{first(row, 'Канал', 'Площадка')}\n{first(row, 'Тема')}\n\n"
                    f"{first(row, 'Краткое описание', 'Описание')}\n\nСтатус: {first(row, 'Решение редактора', 'Статус')}")
            await self.api.send_message(chat_id, text,
                                        inline_keyboard([[("Одобрить", f"pa:ok:{post_id}"), ("Изменить", f"pa:edit:{post_id}")],
                                                         [("Отклонить", f"pa:no:{post_id}")]]))
        await self.api.send_message(chat_id, "Одобрить все оставшиеся пункты?",
                                    inline_keyboard([[("Одобрить всё", f"pall:{period_id}")]]))

    async def show_drafts(self, chat_id: int) -> None:
        drafts = await self.pipelines.latest_drafts()
        active = [row for row in drafts if str(first(row, "Статус", "Статус согласования")) in
                  {DraftDecision.REVIEW.value, DraftDecision.PENDING_REVIEW.value, DraftDecision.NEEDS_PREPARATION.value,
                   DraftDecision.NEEDS_FACT.value, DraftDecision.APPROVED.value,
                   DraftDecision.TEST_SCHEDULED.value}]
        if not active:
            await self.api.send_message(chat_id, "Активных черновиков нет.")
            return
        for row in active[:30]:
            post_id, version = str(first(row, "post_id", "ID поста", "plan_id")), integer(first(row, "draft_version", "Версия"))
            plan = await self.pipelines.get_plan(post_id)
            media = await self.pipelines.media_entries(post_id)
            text = (f"{first(plan, 'Тема')}\nКанал: {first(plan, 'Канал', 'Площадка')}\n"
                    f"Дата: {first(plan, 'Дата', 'Дата публикации')} {first(plan, 'Время', 'Время публикации')}\n"
                    f"Версия: {version} · Статус: {first(row, 'Статус', 'Статус согласования')} · Медиа: {len(media)}\n\n"
                    f"{first(row, 'Текст')}\n\n{first(row, 'Комментарий согласующего', 'Комментарий человека')}")
            await self.api.send_message(chat_id, text, inline_keyboard([
                [("Одобрить", f"da:{post_id}:{version}"), ("Исправить вручную", f"dm:{post_id}:{version}")],
                [("Переписать ИИ", f"dr:{post_id}:{version}"), ("Изменить медиа", f"dmed:{post_id}:{version}")],
                [("Изменить дату", f"dd:{post_id}:{version}"), ("Предпросмотр", f"dp:{post_id}:{version}")],
                [("Отклонить", f"dt:{post_id}:{version}")],
            ]))

    async def _publication_options(self, chat_id: int, post_id: str) -> None:
        draft = await self.pipelines.latest_draft(post_id)
        if not draft: return
        version = integer(first(draft, "draft_version", "Версия"))
        await self.api.send_message(chat_id, "Что сделать с одобренным постом?",
                                    inline_keyboard([[("Отправить в тестовый сейчас", f"pub:now:{post_id}:{version}")],
                                                     [("Запланировать в тестовый", f"pub:schedule:{post_id}:{version}")]]))

    async def _begin_extra(self, event: TelegramEvent, test: bool = False) -> None:
        payload = {"period_id": "Тесты" if test else "Внеплановые",
                   "post_type": "Тестовый" if test else "Внеплановый", "channel": "Telegram"}
        await self.database.set_session(event.user_id, event.chat_id, "extra", "mode", payload)
        await self.api.send_message(event.chat_id, "Что у вас есть?",
                                    inline_keyboard([[("Тема и материалы", "xm:brief"), ("Готовый текст", "xm:ready")]]))

    async def _session_message(self, event: TelegramEvent, session: dict[str, Any]) -> None:
        mode, payload = session["mode"], session["payload"]
        if mode == "rewrite":
            payload["comment"] = event.text
            await self._enqueue(event, JobType.REWRITE_DRAFT, payload)
            await self.database.clear_session(event.user_id)
        elif mode == "manual":
            result = await self.pipelines.create_manual_version(str(payload["post_id"]), event.text, event.user_id)
            await self.database.clear_session(event.user_id)
            await self.api.send_message(event.chat_id, f"Сохранена ручная версия {result['draft_version']}. Она снова ждёт согласования.")
        elif mode == "plan_edit":
            if "field" not in payload:
                raise UserFacingError("Сначала выберите поле кнопкой в сообщении бота.")
            result = await self.pipelines.apply_plan_edit(
                str(payload["post_id"]), str(payload["field"]), event.text, f"plan-edit:{event.update_id}")
            await self.api.send_message(event.chat_id, result)
            await self.database.complete_session_update(event.user_id, event.update_id)
        elif mode == "etalon":
            comment = "" if event.text.lower() == "без комментария" else event.text
            await self.pipelines.add_reference(str(payload["post_id"]), str(payload["what"]), comment, event.user_id)
            await self.database.clear_session(event.user_id)
            await self.api.send_message(event.chat_id, "Пост добавлен в «Эталоны».")
            await self._publication_options(event.chat_id, str(payload["post_id"]))
        elif mode == "publish":
            self.pipelines.require_approver(event.user_id)
            try:
                local = datetime.strptime(event.text.strip(), "%Y-%m-%d %H:%M")
            except ValueError:
                raise UserFacingError("Неверная дата. Используйте ГГГГ-ММ-ДД ЧЧ:ММ.") from None
            local = local.replace(tzinfo=ZoneInfo(self.settings.timezone))
            run_after = local.astimezone(UTC).isoformat()
            data = {**payload, "scheduled_for": local.isoformat()}
            await self.pipelines.schedule_draft(str(payload["post_id"]), int(payload["draft_version"]), event.user_id)
            await self._enqueue(event, JobType.PUBLISH_TEST, data, run_after=run_after)
            await self.database.clear_session(event.user_id)
        elif mode == "extra":
            await self._extra_message(event, session)
        elif mode == "media":
            await self._media_message(event, session)
        else:
            await self.database.clear_session(event.user_id)
            await self.api.send_message(event.chat_id, "Диалог устарел. Откройте меню заново.")

    async def _callback_extra_mode(self, event: TelegramEvent, ready: bool) -> None:
        session = await self.database.get_session(event.user_id)
        if not session or session["mode"] != "extra": raise UserFacingError("Диалог устарел")
        payload = session["payload"]
        payload["mode"] = PreparationMode.CHECK_ONLY.value if ready else PreparationMode.FROM_BRIEF.value
        await self.database.set_session(event.user_id, event.chat_id, "extra", "topic", payload)
        await self.api.send_message(event.chat_id, "Укажите тему поста.")

    async def _extra_message(self, event: TelegramEvent, session: dict[str, Any]) -> None:
        payload, step = session["payload"], session["step"]
        if step == "topic":
            payload["topic"] = event.text
            next_step, prompt = ("ready_text", "Пришлите готовый текст целиком.") if payload["mode"] == PreparationMode.CHECK_ONLY.value else ("brief", "Пришлите тезисы, факты, ссылки и названия файлов одним сообщением.")
        elif step == "brief":
            payload["brief"] = payload["facts"] = event.text
            next_step, prompt = "datetime", "Дата и время: ГГГГ-ММ-ДД ЧЧ:ММ, либо «без даты»."
        elif step == "ready_text":
            payload["ready_text"] = payload["brief"] = event.text
            next_step, prompt = "facts", "Пришлите подтверждённые факты и ссылки либо «нет»."
        elif step == "facts":
            payload["facts"] = "" if event.text.lower() in {"нет", "-"} else event.text
            next_step, prompt = "datetime", "Дата и время: ГГГГ-ММ-ДД ЧЧ:ММ, либо «без даты»."
        elif step == "datetime":
            if event.text.strip().lower() != "без даты":
                parts = event.text.split(maxsplit=1)
                try:
                    datetime.strptime(parts[0], "%Y-%m-%d")
                    if len(parts) > 1:
                        datetime.strptime(parts[1], "%H:%M")
                except (ValueError, IndexError):
                    raise UserFacingError("Неверная дата. Используйте ГГГГ-ММ-ДД ЧЧ:ММ.") from None
                payload["date"], payload["time"] = parts[0], parts[1] if len(parts) > 1 else ""
            await self._enqueue(event, JobType.CREATE_EXTRA_POST, payload)
            await self.database.clear_session(event.user_id)
            return
        else:
            raise UserFacingError("Неизвестный шаг")
        await self.database.set_session(event.user_id, event.chat_id, "extra", next_step, payload)
        await self.api.send_message(event.chat_id, prompt)

    async def _media_message(self, event: TelegramEvent, session: dict[str, Any]) -> None:
        payload = session["payload"]
        message = event.raw.get("message", {})
        document = message.get("document")
        photo = (message.get("photo") or [None])[-1]
        video = message.get("video")
        item = document or video or photo
        if not item:
            await self.api.send_message(event.chat_id, "Отправьте фото, видео или документ. Завершение — кнопкой «Готово».")
            return
        size = int(item.get("file_size", 0) or 0)
        if size > self.settings.telegram_max_upload_bytes:
            await self.api.send_message(event.chat_id, "Файл слишком большой для загрузки через бота. Положите его в папку поста на Drive и нажмите «Я изменил папку Drive».")
            return
        info = await self.api.get_file(item["file_id"])
        content = await self.api.download_file(info["file_path"], min(self.settings.telegram_max_upload_bytes, self.settings.max_media_bytes))
        number = len(payload.get("files", [])) + 1
        if document:
            name = str(document.get("file_name") or Path(info["file_path"]).name)
            mime = str(document.get("mime_type") or "application/octet-stream")
        elif video:
            name, mime = f"{number:02d}_video.mp4", str(video.get("mime_type") or "video/mp4")
        else:
            name, mime = f"{number:02d}_photo.jpg", "image/jpeg"
        suffix = Path(name).suffix
        description = event.text.strip() or (Path(name).stem if document else "")
        safe_name = re.sub(r"[^\w А-Яа-яЁё.-]", "", description).strip(" .")[:100]
        if not safe_name:
            await self.api.send_message(event.chat_id, "Файл не принят. Укажите понятное описание или имя документа.")
            return
        name = safe_name + (suffix if not safe_name.lower().endswith(suffix.lower()) else "")
        uploaded = await self.pipelines.upload_media(str(payload["post_id"]), name, mime, content)
        payload.setdefault("files", []).append({"id": uploaded.get("id"), "name": name})
        await self.database.set_session(event.user_id, event.chat_id, "media", "files", payload)
        await self.api.send_message(event.chat_id, f"Принято: {name}. Можно отправить ещё или нажать «Готово».",
                                    inline_keyboard([[("Готово", f"med:done:{payload['post_id']}")]]))

    async def _enqueue(self, event: TelegramEvent, job_type: JobType, payload: dict[str, Any],
                       run_after: str | None = None) -> None:
        key = f"tg:{event.update_id}:{job_type.value}:{stable_hash(payload)[:12]}"
        job_id, created = await self.database.enqueue_job(job_type, payload, event.user_id, event.chat_id, key, run_after)
        when = f" на {payload.get('scheduled_for')}" if run_after else ""
        await self.api.send_message(event.chat_id, f"Задание №{job_id} {'принято' if created else 'уже было принято'}{when}.")

    async def show_status(self, chat_id: int) -> None:
        summary = await self.database.queue_summary()
        await self.api.send_message(chat_id, "Очередь пуста." if not summary else "Задания: " + ", ".join(f"{key} — {value}" for key, value in sorted(summary.items())))

    async def export_drafts(self, chat_id: int) -> None:
        drafts = await self.pipelines.latest_drafts()
        if not drafts:
            await self.api.send_message(chat_id, "Черновиков нет.")
            return
        blocks = [f"POST: {first(row, 'post_id', 'ID поста', 'plan_id')}\nКанал: {first(row, 'Канал', 'Площадка')}\nВерсия: {first(row, 'draft_version', 'Версия')}\nСтатус: {first(row, 'Статус', 'Статус согласования')}\n\n{first(row, 'Текст')}" for row in drafts]
        await self.api.send_document(chat_id, "qifa-drafts.txt", ("\n\n" + "=" * 60 + "\n\n").join(blocks).encode(), "Последние версии")
