from __future__ import annotations
import asyncio
import logging
from collections.abc import Awaitable, Callable
from .db import Database, Job
from .domain import JobType
from .security import UserFacingError
JobHandler = Callable[[Job], Awaitable[str]]


class JobRunner:
    def __init__(
        self,
        database: Database,
        handlers: dict[JobType, JobHandler],
        notify: Callable[[int, str], Awaitable[None]],
        poll_seconds: float,
    ) -> None:
        self.database = database
        self.handlers = handlers
        self.notify = notify
        self.poll_seconds = poll_seconds
        self.log = logging.getLogger(__name__)

    async def _notify(self, chat_id: int, text: str) -> None:
        try:
            await self.notify(chat_id, text)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.log.exception("Failed to send job notification")

    async def run(self, worker_name: str) -> None:
        self.log.info("Job worker %s started", worker_name)
        while True:
            try:
                job = await self.database.claim_next_job()
                if job is None:
                    await asyncio.sleep(self.poll_seconds)
                    continue
                try:
                    result = await self.handlers[job.type](job)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.log.exception("Job %s failed", job.id)
                    await self.database.fail_job(job.id, type(exc).__name__)
                    await self._notify(job.reply_chat_id, str(exc) if isinstance(exc, UserFacingError) else f"Ошибка задания №{job.id}. Подробности записаны в журнал сервиса.")
                else:
                    await self.database.finish_job(job.id)
                    await self._notify(job.reply_chat_id, result)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Job worker iteration failed")
                await asyncio.sleep(self.poll_seconds)
