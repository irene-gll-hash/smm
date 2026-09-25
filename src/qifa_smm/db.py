from __future__ import annotations
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import aiosqlite
from .domain import JobStatus, JobType
from .utils import utc_now


@dataclass(slots=True)
class Job:
    id: int
    type: JobType
    payload: dict[str, Any]
    status: JobStatus
    requested_by: int
    reply_chat_id: int
    attempts: int
    idempotency_key: str


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path

    @asynccontextmanager
    async def connection(self):
        db = await aiosqlite.connect(self.path)
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute("PRAGMA busy_timeout=5000")
        try:
            yield db
        finally:
            await db.close()

    async def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        async with self.connection() as db:
            await db.executescript(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS processed_updates (
                    update_id INTEGER PRIMARY KEY,
                    processed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS telegram_inbox (
                    update_id INTEGER PRIMARY KEY,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS bot_sessions (
                    user_id INTEGER PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    step TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL,
                    expires_at TEXT
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    requested_by INTEGER NOT NULL,
                    reply_chat_id INTEGER NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    run_after TEXT,
                    started_at TEXT,
                    finished_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_status_id ON jobs(status, id);

                CREATE TABLE IF NOT EXISTS approvals (
                    post_id TEXT NOT NULL,
                    plan_version INTEGER NOT NULL,
                    draft_version INTEGER NOT NULL,
                    fingerprint TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    decided_by INTEGER NOT NULL,
                    decided_at TEXT NOT NULL,
                    PRIMARY KEY(post_id, plan_version, draft_version)
                );

                CREATE TABLE IF NOT EXISTS source_files (
                    file_id TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    period_id TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    readable INTEGER NOT NULL,
                    processed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS publications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id TEXT NOT NULL,
                    draft_version INTEGER NOT NULL,
                    target TEXT NOT NULL,
                    status TEXT NOT NULL,
                    requested_by INTEGER NOT NULL,
                    scheduled_for TEXT,
                    telegram_message_ids_json TEXT NOT NULL DEFAULT '[]',
                    error TEXT,
                    created_at TEXT NOT NULL,
                    finished_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_publications_post
                    ON publications(post_id, draft_version, target);
                """
            )
            columns = {
                str(row["name"])
                for row in await (await db.execute("PRAGMA table_info(jobs)")).fetchall()
            }
            if "run_after" not in columns:
                await db.execute("ALTER TABLE jobs ADD COLUMN run_after TEXT")
            # A process can stop after claiming a job. On the next start the
            # unfinished item returns to the durable queue instead of being lost.
            await db.execute(
                "UPDATE jobs SET status = ?, started_at = NULL WHERE status = ?",
                (JobStatus.QUEUED.value, JobStatus.RUNNING.value),
            )
            await db.commit()

    async def source_is_current(self, file_id: str, fingerprint: str) -> bool:
        async with self.connection() as db:
            row = await (
                await db.execute(
                    "SELECT fingerprint FROM source_files WHERE file_id = ?", (file_id,)
                )
            ).fetchone()
            return bool(row and row["fingerprint"] == fingerprint)

    async def save_source_file(
        self,
        file_id: str,
        fingerprint: str,
        period_id: str,
        relative_path: str,
        readable: bool,
    ) -> None:
        async with self.connection() as db:
            await db.execute(
                """
                INSERT INTO source_files(
                    file_id, fingerprint, period_id, relative_path, readable, processed_at
                ) VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_id) DO UPDATE SET
                    fingerprint=excluded.fingerprint,
                    period_id=excluded.period_id,
                    relative_path=excluded.relative_path,
                    readable=excluded.readable,
                    processed_at=excluded.processed_at
                """,
                (file_id, fingerprint, period_id, relative_path, int(readable), utc_now()),
            )
            await db.commit()

    async def get_setting(self, key: str, default: str = "") -> str:
        async with self.connection() as db:
            row = await (await db.execute("SELECT value FROM settings WHERE key = ?", (key,))).fetchone()
            return str(row["value"]) if row else default

    async def set_setting(self, key: str, value: str) -> None:
        async with self.connection() as db:
            await db.execute(
                """
                INSERT INTO settings(key, value, updated_at) VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, value, utc_now()),
            )
            await db.commit()

    async def is_update_processed(self, update_id: int) -> bool:
        async with self.connection() as db:
            row = await (
                await db.execute("SELECT 1 FROM processed_updates WHERE update_id = ?", (update_id,))
            ).fetchone()
            return row is not None

    async def mark_update_processed(self, update_id: int) -> None:
        async with self.connection() as db:
            await db.execute(
                "INSERT OR IGNORE INTO processed_updates(update_id, processed_at) VALUES(?, ?)",
                (update_id, utc_now()),
            )
            await db.execute("DELETE FROM telegram_inbox WHERE update_id = ?", (update_id,))
            await db.execute(
                "DELETE FROM processed_updates WHERE update_id < ?",
                (max(0, update_id - 10000),),
            )
            await db.commit()

    async def save_telegram_updates(self, updates: list[dict[str, Any]]) -> None:
        """Persist the whole batch before acknowledging it through getUpdates offset."""
        if not updates:
            return
        async with self.connection() as db:
            await db.execute("BEGIN IMMEDIATE")
            for raw in updates:
                await db.execute(
                    "INSERT OR IGNORE INTO telegram_inbox(update_id, payload_json) "
                    "SELECT ?, ? WHERE NOT EXISTS "
                    "(SELECT 1 FROM processed_updates WHERE update_id = ?)",
                    (int(raw["update_id"]), json.dumps(raw, ensure_ascii=False), int(raw["update_id"])),
                )
            await db.execute(
                "INSERT INTO settings(key, value, updated_at) VALUES('telegram_offset', ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (str(max(int(raw["update_id"]) for raw in updates) + 1), utc_now()),
            )
            await db.commit()

    async def pending_telegram_updates(self) -> list[dict[str, Any]]:
        async with self.connection() as db:
            rows = await (await db.execute(
                "SELECT payload_json FROM telegram_inbox ORDER BY update_id LIMIT 100"
            )).fetchall()
            return [json.loads(row["payload_json"]) for row in rows]

    async def set_session(
        self, user_id: int, chat_id: int, mode: str, step: str, payload: dict[str, Any]
    ) -> None:
        async with self.connection() as db:
            await db.execute(
                """
                INSERT INTO bot_sessions(user_id, chat_id, mode, step, payload_json, updated_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    chat_id=excluded.chat_id,
                    mode=excluded.mode,
                    step=excluded.step,
                    payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at
                """,
                (user_id, chat_id, mode, step, json.dumps(payload, ensure_ascii=False), utc_now()),
            )
            await db.commit()

    async def get_session(self, user_id: int) -> dict[str, Any] | None:
        async with self.connection() as db:
            row = await (
                await db.execute("SELECT * FROM bot_sessions WHERE user_id = ?", (user_id,))
            ).fetchone()
            if not row:
                return None
            return {
                "user_id": row["user_id"],
                "chat_id": row["chat_id"],
                "mode": row["mode"],
                "step": row["step"],
                "payload": json.loads(row["payload_json"]),
            }

    async def clear_session(self, user_id: int) -> None:
        async with self.connection() as db:
            await db.execute("DELETE FROM bot_sessions WHERE user_id = ?", (user_id,))
            await db.commit()

    async def complete_session_update(self, user_id: int, update_id: int) -> None:
        """Finish a successful dialog and its inbox event in one local transaction."""
        async with self.connection() as db:
            await db.execute("DELETE FROM bot_sessions WHERE user_id = ?", (user_id,))
            await db.execute(
                "INSERT OR IGNORE INTO processed_updates(update_id, processed_at) VALUES(?, ?)",
                (update_id, utc_now()),
            )
            await db.execute("DELETE FROM telegram_inbox WHERE update_id = ?", (update_id,))
            await db.commit()

    async def published_versions(self) -> set[tuple[str, int]]:
        async with self.connection() as db:
            rows = await (await db.execute(
                "SELECT post_id, draft_version FROM publications WHERE status = 'done'"
            )).fetchall()
            return {(str(row['post_id']), int(row['draft_version'])) for row in rows}

    async def reserve_draft_version(self, post_id: str, minimum: int) -> int:
        """Allocate across jobs, including versions reserved by an interrupted action."""
        key = f"draft-version:{post_id}"
        async with self.connection() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (await db.execute("SELECT value FROM settings WHERE key = ?", (key,))).fetchone()
            version = max(minimum, int(row['value']) + 1 if row else 1)
            await db.execute(
                "INSERT INTO settings(key, value, updated_at) VALUES(?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (key, str(version), utc_now()),
            )
            await db.commit()
            return version

    async def pending_plan_operations(self, period_id: str) -> list[str]:
        async with self.connection() as db:
            rows = await (await db.execute(
                "SELECT key, value FROM settings WHERE key LIKE 'operation:%' ORDER BY rowid"
            )).fetchall()
            result = []
            for row in rows:
                operation = json.loads(row['value'])
                if operation.get('period_id') == period_id and 'rows' in operation and not operation.get('done'):
                    result.append(str(row['key']).removeprefix('operation:'))
            return result

    async def enqueue_job(
        self,
        job_type: JobType,
        payload: dict[str, Any],
        requested_by: int,
        reply_chat_id: int,
        idempotency_key: str,
        run_after: str | None = None,
    ) -> tuple[int, bool]:
        async with self.connection() as db:
            cursor = await db.execute(
                """
                INSERT OR IGNORE INTO jobs(
                    type, payload_json, status, requested_by, reply_chat_id,
                    idempotency_key, created_at, run_after
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_type.value,
                    json.dumps(payload, ensure_ascii=False),
                    JobStatus.QUEUED.value,
                    requested_by,
                    reply_chat_id,
                    idempotency_key,
                    utc_now(),
                    run_after,
                ),
            )
            created = cursor.rowcount == 1
            if created:
                job_id = int(cursor.lastrowid)
            else:
                row = await (
                    await db.execute(
                        "SELECT id FROM jobs WHERE idempotency_key = ?", (idempotency_key,)
                    )
                ).fetchone()
                job_id = int(row["id"])
            await db.commit()
            return job_id, created

    async def claim_next_job(self) -> Job | None:
        async with self.connection() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (
                await db.execute(
                    """
                    SELECT * FROM jobs
                    WHERE status = ? AND (run_after IS NULL OR run_after <= ?)
                    ORDER BY COALESCE(run_after, created_at), id
                    LIMIT 1
                    """,
                    (JobStatus.QUEUED.value, utc_now()),
                )
            ).fetchone()
            if not row:
                await db.rollback()
                return None
            await db.execute(
                """
                UPDATE jobs SET status = ?, attempts = attempts + 1, started_at = ? WHERE id = ?
                """,
                (JobStatus.RUNNING.value, utc_now(), row["id"]),
            )
            await db.commit()
            return Job(
                id=int(row["id"]),
                type=JobType(row["type"]),
                payload=json.loads(row["payload_json"]),
                status=JobStatus.RUNNING,
                requested_by=int(row["requested_by"]),
                reply_chat_id=int(row["reply_chat_id"]),
                attempts=int(row["attempts"]) + 1,
                idempotency_key=str(row["idempotency_key"]),
            )

    async def finish_job(self, job_id: int) -> None:
        async with self.connection() as db:
            await db.execute(
                "UPDATE jobs SET status = ?, finished_at = ?, error = NULL WHERE id = ?",
                (JobStatus.DONE.value, utc_now(), job_id),
            )
            await db.commit()

    async def fail_job(self, job_id: int, error: str) -> None:
        async with self.connection() as db:
            await db.execute(
                "UPDATE jobs SET status = ?, finished_at = ?, error = ? WHERE id = ?",
                (JobStatus.ERROR.value, utc_now(), error[:2000], job_id),
            )
            await db.commit()

    async def queue_summary(self) -> dict[str, int]:
        async with self.connection() as db:
            rows = await (
                await db.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status")
            ).fetchall()
            return {str(row["status"]): int(row["count"]) for row in rows}

    async def record_approval(
        self,
        post_id: str,
        plan_version: int,
        draft_version: int,
        fingerprint: str,
        decision: str,
        decided_by: int,
    ) -> None:
        async with self.connection() as db:
            await db.execute(
                """
                INSERT INTO approvals(
                    post_id, plan_version, draft_version, fingerprint,
                    decision, decided_by, decided_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(post_id, plan_version, draft_version) DO UPDATE SET
                    fingerprint=excluded.fingerprint,
                    decision=excluded.decision,
                    decided_by=excluded.decided_by,
                    decided_at=excluded.decided_at
                """,
                (
                    post_id,
                    plan_version,
                    draft_version,
                    fingerprint,
                    decision,
                    decided_by,
                    utc_now(),
                ),
            )
            await db.commit()

    async def get_approval(self, post_id: str, plan_version: int, draft_version: int) -> dict[str, Any] | None:
        async with self.connection() as db:
            row = await (await db.execute(
                "SELECT * FROM approvals WHERE post_id = ? AND plan_version = ? AND draft_version = ?",
                (post_id, plan_version, draft_version),
            )).fetchone()
            return dict(row) if row else None

    async def record_publication(
        self,
        post_id: str,
        draft_version: int,
        target: str,
        status: str,
        requested_by: int,
        scheduled_for: str | None,
        message_ids: list[int] | None = None,
        error: str = "",
    ) -> None:
        async with self.connection() as db:
            await db.execute(
                """
                INSERT INTO publications(
                    post_id, draft_version, target, status, requested_by,
                    scheduled_for, telegram_message_ids_json, error,
                    created_at, finished_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    post_id,
                    draft_version,
                    target,
                    status,
                    requested_by,
                    scheduled_for,
                    json.dumps(message_ids or []),
                    error[:2000],
                    utc_now(),
                    utc_now() if status in {"done", "error"} else None,
                ),
            )
            await db.commit()
