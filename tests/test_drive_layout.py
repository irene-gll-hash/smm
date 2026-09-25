from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from qifa_smm.db import Database
from qifa_smm.google_workspace import DriveEntry
from qifa_smm.pipelines import PipelineService
from test_security import settings


FOLDER = "application/vnd.google-apps.folder"


def entry(file_id: str, path: str, mime: str = "text/plain", *, link: str = "") -> DriveEntry:
    return DriveEntry(file_id, Path(path).name, mime, "2026-09-25T00:00:00Z", "10",
                      link, path, "parent")


class DriveLayoutTests(unittest.IsolatedAsyncioTestCase):
    def test_date_then_theme_layout_and_legacy_layout_are_grouped(self):
        entries = [
            entry("d1", "2026-10-01", FOLDER, link="date-link"),
            entry("t1", "2026-10-01/Новости", FOLDER, link="theme-link"),
            entry("f1", "2026-10-01/Новости/brief.txt"),
            entry("f2", "2026-10-01/Новости/вложенная/notes.txt"),
            entry("f3", "01.10.2026/Без темы.txt"),
            entry("old", "Старая тема/source.txt"),
            entry("p1", "2026-10-01/POST-0123456789 — Пост", FOLDER),
            entry("p2", "2026-10-01/POST-0123456789 — Пост/01_photo.jpg", "image/jpeg"),
            entry("root", "loose.txt"),
        ]
        groups, links, ignored = PipelineService._material_groups(entries)
        self.assertEqual(set(groups), {
            ("2026-10-01", "Новости"), ("01.10.2026", "Без темы"), ("", "Старая тема")
        })
        self.assertEqual([item.id for item in groups[("2026-10-01", "Новости")]], ["f1", "f2"])
        self.assertEqual(links[("2026-10-01", "Новости")], "theme-link")
        self.assertEqual(ignored, 2)

    async def test_processing_uses_date_and_theme_and_detects_moved_files(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Database(Path(tmp.name) / "state.sqlite")
        await database.initialize()
        cfg = settings()
        workspace = AsyncMock()
        sheets = AsyncMock()
        llm = AsyncMock()
        publisher = AsyncMock()
        service = PipelineService(cfg, database, workspace, sheets, llm, publisher)
        service._period = AsyncMock(return_value={"Ссылка на папку": "root"})
        workspace.list_folder.return_value = [
            entry("theme", "2026-10-01/Новости", FOLDER, link="theme-link"),
            entry("file", "2026-10-01/Новости/source.txt"),
        ]
        workspace.download_file.return_value = (b"source", "text/plain")
        llm.json_response.return_value = {
            "title": "Новость", "category": "PR", "confirmed_facts": [], "topic_ideas": []
        }
        headers = ["material_id", "period_id", "Дата материалов", "Название материала",
                   "Категория", "Ссылка на папку", "Что произошло", "Подтверждённые факты",
                   "Важная дата или эмбарго", "Чего не хватает", "Возможные темы",
                   "Статус использования", "Обновлено"]
        sheets.headers.return_value = headers
        job = SimpleNamespace(payload={"period_id": "P"})

        result = await service.process_materials(job)
        self.assertIn("обновлено 1", result)
        prompt = json.loads(llm.json_response.call_args.args[1])
        self.assertEqual(prompt["date_folder"], "2026-10-01")
        self.assertEqual(prompt["theme_folder"], "Новости")
        row = sheets.upsert_dicts.call_args.args[2][0]
        self.assertEqual(row["Дата материалов"], "2026-10-01")
        self.assertEqual(row["Название материала"], "2026-10-01 — Новость")
        self.assertEqual(row["Ссылка на папку"], "theme-link")

        llm.reset_mock()
        result = await service.process_materials(job)
        self.assertIn("без изменений 1", result)
        llm.json_response.assert_not_awaited()

        workspace.list_folder.return_value[1].relative_path = "2026-10-02/Новости/source.txt"
        result = await service.process_materials(job)
        self.assertIn("обновлено 1", result)
        self.assertEqual(json.loads(llm.json_response.call_args.args[1])["date_folder"], "2026-10-02")

    def test_supported_date_folder_names(self):
        for value in ("2026-10-01", "01.10.2026", "01-10-2026", "Без даты"):
            with self.subTest(value=value):
                self.assertTrue(PipelineService._is_date_folder(value))
        for value in ("Октябрь", "2026_10_01", "Тема"):
            with self.subTest(value=value):
                self.assertFalse(PipelineService._is_date_folder(value))
