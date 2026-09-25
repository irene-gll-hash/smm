from __future__ import annotations
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from qifa_smm.config import Settings
from qifa_smm.db import Database
from qifa_smm.domain import JobType


class SettingsTests(unittest.TestCase):
    def make_settings(self, **overrides):
        values = {
            "telegram_control_bot_token": "control",
            "telegram_publish_bot_token": "publisher",
            "telegram_test_channel_id": "-1001",
            "telegram_allowed_user_ids": "1,2",
            "telegram_approver_user_ids": "2",
            "google_oauth_token_file": Path("/tmp/key.json"),
            "google_spreadsheet_id": "sheet",
            "google_drive_root_folder_id": "root",
            "llm_mode": "stub",
        }
        values.update(overrides)
        return Settings(**values)

    def test_roles(self):
        settings = self.make_settings()
        self.assertTrue(settings.is_approver(2))
        self.assertFalse(settings.is_approver(1))

    def test_bots_must_be_separate(self):
        with self.assertRaises(ValueError):
            self.make_settings(telegram_publish_bot_token="control")


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp.name) / "state.sqlite3")
        await self.db.initialize()

    async def asyncTearDown(self):
        self.temp.cleanup()

    async def test_scheduled_job_waits_and_immediate_job_runs(self):
        future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
        await self.db.enqueue_job(
            JobType.PUBLISH_TEST, {"post_id": "P", "draft_version": 1},
            2, 2, "future", future,
        )
        self.assertIsNone(await self.db.claim_next_job())
        immediate_id, _ = await self.db.enqueue_job(
            JobType.BUILD_PLAN, {"period_id": "X"}, 1, 1, "now"
        )
        job = await self.db.claim_next_job()
        self.assertIsNotNone(job)
        self.assertEqual(job.id, immediate_id)
        self.assertEqual(job.type, JobType.BUILD_PLAN)

    async def test_records_approval_and_publication(self):
        await self.db.record_approval("P", 1, 1, "hash", "Одобрено", 2)
        await self.db.record_publication(
            "P", 1, "telegram_test", "done", 2, None, [100, 101]
        )
