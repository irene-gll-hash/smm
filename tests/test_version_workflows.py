import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from qifa_smm.db import Database
from qifa_smm.domain import DraftDecision, PlanDecision, PreparationMode, TelegramEvent, JobType
from qifa_smm.pipelines import PipelineService
from qifa_smm.telegram import TelegramController
from qifa_smm.utils import short_id
from test_security import settings


PLAN_HEADERS = ['content_id', 'post_id', 'period_id', 'Дата', 'Время', 'Канал', 'Рубрика',
                'Тема', 'Краткое описание', 'Зачем это аудитории', 'Материалы',
                'Подтверждённые факты', 'Готовый текст', 'Нужен визуал', 'Папка медиа',
                'Автор плана', 'Тип', 'Режим подготовки', 'Зафиксировано человеком',
                'Версия плана', 'Решение редактора', 'Комментарий редактора']
TEXT_HEADERS = ['version_id', 'post_id', 'plan_version', 'draft_version', 'Канал', 'Текст',
                'Источники', 'Медиа', 'Статус', 'Комментарий согласующего', 'Автор версии',
                'Создано', 'approval_fingerprint']


class MemorySheets:
    """A sheet-shaped store, including uncertain successful writes followed by errors."""
    def __init__(self, cfg):
        self.data = {}
        self.schema = {cfg.sheet_plan: PLAN_HEADERS, cfg.sheet_texts: TEXT_HEADERS}
        self.fail_after_append = None
        self.fail_update = None

    async def headers(self, sheet):
        return self.schema.get(sheet, [])[:]

    async def rows(self, sheet):
        return [{**copy.deepcopy(row), '_row_number': i} for i, row in enumerate(self.data.get(sheet, []), 2)]

    async def append_dicts(self, sheet, rows):
        self.data.setdefault(sheet, []).extend(copy.deepcopy(rows))
        if rows and self.fail_after_append == sheet:
            self.fail_after_append = None
            raise RuntimeError('connection dropped after committed append')

    async def update_dict(self, sheet, row_number, values):
        if self.fail_update == sheet:
            self.fail_update = None
            raise RuntimeError('temporary update failure')
        self.data[sheet][row_number - 2].update(copy.deepcopy(values))

    async def upsert_dicts(self, sheet, key, items):
        for item in items:
            found = next((row for row in await self.rows(sheet) if row.get(key) == item[key]), None)
            if found:
                await self.update_dict(sheet, found['_row_number'], item)
            else:
                await self.append_dicts(sheet, [item])


class VersionWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / 'state.sqlite')
        await self.db.initialize()
        self.cfg = settings()
        self.sheets = MemorySheets(self.cfg)
        self.llm = AsyncMock()
        self.llm.json_response.return_value = {'text': 'Rewritten text'}
        self.api = AsyncMock()
        self.new_service()
        self.post = short_id('POST', 'C:Telegram:2026-10-01')
        self.plan = {key: '' for key in PLAN_HEADERS}
        self.plan.update({'content_id': 'C', 'post_id': self.post, 'period_id': 'P',
                          'Дата': '2026-10-01', 'Канал': 'Telegram', 'Тема': 'Topic',
                          'Краткое описание': 'Old brief', 'Версия плана': 1,
                          'Решение редактора': PlanDecision.APPROVED.value, 'Автор плана': 'ИИ',
                          'Режим подготовки': PreparationMode.CHECK_ONLY.value,
                          'Готовый текст': 'Original supplied text', 'Подтверждённые факты': 'Confirmed fact',
                          'Материалы': 'M1', 'Зафиксировано человеком': 'нет', 'Нужен визуал': 'нет'})
        self.sheets.data[self.cfg.sheet_plan] = [copy.deepcopy(self.plan)]
        self.sheets.data[self.cfg.sheet_texts] = [
            {'version_id': self.post + '-V1', 'post_id': self.post, 'plan_version': 1,
             'draft_version': 1, 'Текст': 'Current edited text', 'Статус': DraftDecision.REVIEW.value}]
        self.sheets.data[self.cfg.sheet_periods] = [{'period_id': 'P'}]
        self.sheets.data[self.cfg.sheet_materials] = [{'material_id': 'M1', 'Подтверждённые факты': 'Source fact'}]
        self.sheets.data[self.cfg.sheet_channels] = [{'Канал': 'Telegram', 'Лимит': '900'}, {'Канал': 'VK'}]
        self.sheets.data[self.cfg.sheet_rules] = [{'Правило': 'Use short sentences'}]

    def new_service(self):
        self.service = PipelineService(self.cfg, self.db, AsyncMock(), self.sheets, self.llm, AsyncMock())
        self.controller = TelegramController(self.cfg, self.api, self.db, self.service)

    async def asyncTearDown(self):
        self.tmp.cleanup()

    def job(self, job_id, **payload):
        return SimpleNamespace(id=job_id, payload=payload, requested_by=2)

    async def edit_event(self, field, text, update_id=50):
        await self.db.set_session(2, 2, 'plan_edit', 'value', {'post_id': self.post, 'field': field})
        return TelegramEvent(update_id=update_id, user_id=2, chat_id=2, kind='message', text=text)

    def proposal(self, brief='New brief'):
        return {'items': [{'content_id': 'C', 'channel': 'Telegram', 'date': '2026-10-01',
                           'topic': 'Topic', 'brief': brief, 'material_ids': ['M1'],
                           'preparation_mode': PreparationMode.CHECK_ONLY.value}]}

    async def test_invalid_manual_date_preserves_session_and_writes_nothing(self):
        event = await self.edit_event('datetime', 'invalid')
        before = copy.deepcopy(self.sheets.data)
        with self.assertRaisesRegex(ValueError, 'Неверная дата'):
            await self.controller.handle(event)
        self.assertIsNotNone(await self.db.get_session(2))
        self.assertEqual(self.sheets.data, before)
        self.llm.json_response.assert_not_awaited()

    async def test_manual_edit_retry_after_draft_append_survives_restart(self):
        event = await self.edit_event('brief', 'New manual brief')
        self.sheets.fail_after_append = self.cfg.sheet_texts
        with self.assertRaises(RuntimeError):
            await self.controller.handle(event)
        self.assertIsNotNone(await self.db.get_session(2))
        self.assertFalse(await self.db.is_update_processed(50))
        self.new_service()
        await self.controller.handle(event)
        self.assertIsNone(await self.db.get_session(2))
        self.assertTrue(await self.db.is_update_processed(50))
        self.assertEqual(len(self.sheets.data[self.cfg.sheet_plan]), 2)
        texts = self.sheets.data[self.cfg.sheet_texts]
        self.assertEqual(len(texts), 2)
        self.assertEqual(texts[1]['Статус'], 'pending_review')
        self.assertEqual(texts[0]['Текст'], 'Current edited text')
        self.llm.json_response.assert_awaited_once()
        await self.service.apply_plan_edit(self.post, 'brief', event.text, 'plan-edit:50')
        self.assertEqual(len(self.sheets.data[self.cfg.sheet_texts]), 2)

    async def test_manual_date_creates_copy_before_clearing_dialog(self):
        event = await self.edit_event('datetime', '2026-10-02 13:00')
        self.api.send_message.side_effect = RuntimeError('notification failed')
        with self.assertRaises(RuntimeError):
            await self.controller.handle(event)
        self.assertIsNotNone(await self.db.get_session(2))
        self.api.send_message.side_effect = None
        await self.controller.handle(event)
        self.assertIsNone(await self.db.get_session(2))
        texts = self.sheets.data[self.cfg.sheet_texts]
        self.assertEqual(len(texts), 2)
        self.assertEqual(texts[1]['Текст'], texts[0]['Текст'])
        self.assertEqual(texts[1]['plan_version'], 2)
        self.llm.json_response.assert_not_awaited()

    async def test_same_manual_content_does_not_create_versions(self):
        await self.service.apply_plan_edit(self.post, 'brief', 'Old brief', 'same')
        self.assertEqual(len(self.sheets.data[self.cfg.sheet_plan]), 1)
        self.assertEqual(len(self.sheets.data[self.cfg.sheet_texts]), 1)

    async def test_rewrite_uses_current_text_facts_channel_rules_and_comment(self):
        original = copy.deepcopy(self.sheets.data[self.cfg.sheet_texts][0])
        job = self.job(1, post_id=self.post, draft_version=1, comment='Shorten to 900')
        await self.service.rewrite_draft(job)
        prompt = json.loads(self.llm.json_response.call_args.args[1])
        self.assertEqual(prompt['current_text'], 'Current edited text')
        self.assertIn('Source fact', prompt['confirmed_facts'])
        self.assertIn('Confirmed fact', prompt['confirmed_facts'])
        self.assertEqual(prompt['channel_rules'], [{'Канал': 'Telegram', 'Лимит': '900', '_row_number': 2}])
        self.assertEqual(prompt['editor_comment'], 'Shorten to 900')
        self.assertTrue(prompt['rules'])
        texts = self.sheets.data[self.cfg.sheet_texts]
        self.assertEqual(texts[0], original)
        self.assertEqual(texts[1]['draft_version'], 2)
        self.assertEqual(texts[1]['Статус'], 'pending_review')
        self.new_service()
        await self.service.rewrite_draft(job)
        self.assertEqual(len(texts), 2)
        self.llm.json_response.assert_awaited_once()
        await self.controller.show_drafts(2)
        self.assertTrue(self.api.send_message.await_count)

    async def test_stale_rewrite_does_not_call_model(self):
        with self.assertRaisesRegex(ValueError, 'устарела'):
            await self.service.rewrite_draft(self.job(2, post_id=self.post, draft_version=0))
        self.llm.json_response.assert_not_awaited()

    async def test_rewrite_button_enqueues_real_job_once(self):
        event = TelegramEvent(update_id=30, user_id=2, chat_id=2, kind='callback', callback_data=f'dr:{self.post}:1')
        await self.controller.handle(event)
        session = await self.db.get_session(2)
        message = TelegramEvent(update_id=31, user_id=2, chat_id=2, kind='message', text='Make shorter')
        await self.controller._session_message(message, session)
        await self.controller._session_message(message, session)
        job = await self.db.claim_next_job()
        self.assertEqual(job.type, JobType.REWRITE_DRAFT)
        self.assertEqual(job.payload['comment'], 'Make shorter')
        await self.service.handlers()[job.type](job)
        self.assertIsNone(await self.db.claim_next_job())
        self.assertEqual(self.sheets.data[self.cfg.sheet_texts][-1]['Статус'], 'pending_review')

    async def test_period_versions_start_at_one_and_identical_content_is_not_saved(self):
        self.sheets.data[self.cfg.sheet_plan] = []
        self.sheets.data[self.cfg.sheet_texts] = []
        self.llm.json_response.return_value = self.proposal()
        await self.service.build_plan(self.job(10, period_id='P'))
        self.assertEqual(self.sheets.data[self.cfg.sheet_plan][0]['Версия плана'], 1)
        await self.service.build_plan(self.job(11, period_id='P'))
        self.assertEqual(len(self.sheets.data[self.cfg.sheet_plan]), 1)
        self.llm.json_response.return_value = self.proposal('Different')
        await self.service.build_plan(self.job(12, period_id='P'))
        history = self.sheets.data[self.cfg.sheet_plan]
        self.assertEqual([r['Версия плана'] for r in history], [1, 2])
        self.assertEqual(history[0]['Решение редактора'], 'superseded')
        self.assertEqual((await self.service.get_plan(self.post))['Версия плана'], 2)
        self.assertEqual(len(await self.service.plan_rows('P')), 1)

    async def test_changed_plan_invalidates_unpublished_drafts(self):
        self.sheets.data[self.cfg.sheet_texts][0]['Статус'] = DraftDecision.TEST_SCHEDULED.value
        self.llm.json_response.return_value = self.proposal()
        await self.service.build_plan(self.job(20, period_id='P'))
        self.assertEqual(self.sheets.data[self.cfg.sheet_texts][0]['Статус'], 'needs_preparation')
        await self.service.set_plan_decision(self.post, PlanDecision.APPROVED.value, 2)
        self.assertEqual(self.sheets.data[self.cfg.sheet_plan][0]['Решение редактора'], 'superseded')
        await self.service.build_drafts(self.job(21, period_id='P'))
        self.assertEqual(self.sheets.data[self.cfg.sheet_texts][-1]['plan_version'], 2)

    async def test_plan_append_failure_reconciles_without_duplicate_revision(self):
        self.llm.json_response.return_value = self.proposal()
        self.sheets.fail_after_append = self.cfg.sheet_plan
        with self.assertRaises(RuntimeError):
            await self.service.build_plan(self.job(22, period_id='P'))
        self.new_service()
        await self.service.build_plan(self.job(22, period_id='P'))
        self.assertEqual(len(self.sheets.data[self.cfg.sheet_plan]), 2)
        self.assertEqual(self.sheets.data[self.cfg.sheet_plan][0]['Решение редактора'], 'superseded')
        self.assertEqual(self.sheets.data[self.cfg.sheet_texts][0]['Статус'], 'needs_preparation')
        self.llm.json_response.assert_awaited_once()

    async def test_published_records_are_untouched_even_if_sheet_status_is_stale(self):
        await self.db.record_publication(self.post, 1, 'telegram_test', 'done', 2, None, [123])
        published_plan = copy.deepcopy(self.plan)
        published_draft = copy.deepcopy(self.sheets.data[self.cfg.sheet_texts][0])
        self.llm.json_response.return_value = self.proposal()
        await self.service.build_plan(self.job(23, period_id='P'))
        self.assertEqual(self.sheets.data[self.cfg.sheet_plan], [published_plan])
        self.assertEqual(self.sheets.data[self.cfg.sheet_texts], [published_draft])

    async def test_version_uses_period_max_and_retains_published_rows(self):
        other = {**self.plan, 'post_id': 'published', 'Версия плана': 7}
        self.sheets.data[self.cfg.sheet_plan].append(other)
        published = {'post_id': 'published', 'plan_version': 7, 'draft_version': 1,
                     'Статус': DraftDecision.TEST_PUBLISHED.value, 'Текст': 'Published'}
        self.sheets.data[self.cfg.sheet_texts].append(published)
        before = copy.deepcopy(other)
        self.llm.json_response.return_value = self.proposal()
        await self.service.build_plan(self.job(24, period_id='P'))
        self.assertEqual((await self.service.get_plan(self.post))['Версия плана'], 8)
        self.assertEqual(self.sheets.data[self.cfg.sheet_plan][1], before)
        self.assertEqual(self.sheets.data[self.cfg.sheet_texts][1], published)

    async def test_rewrite_failure_before_model_result_can_resume_without_overwriting_other_version(self):
        job = self.job(40, post_id=self.post, draft_version=1, comment='Rewrite')
        self.llm.json_response.side_effect = RuntimeError('model unavailable')
        with self.assertRaises(RuntimeError):
            await self.service.rewrite_draft(job)
        manual = await self.service.create_manual_version(self.post, 'Separate manual edit', 2)
        self.assertEqual(manual['draft_version'], 3)
        self.llm.json_response.side_effect = None
        self.new_service()
        await self.service.rewrite_draft(job)
        versions = {row['draft_version']: row['Текст'] for row in self.sheets.data[self.cfg.sheet_texts]}
        self.assertEqual(versions, {1: 'Current edited text', 2: 'Rewritten text', 3: 'Separate manual edit'})
        self.assertEqual((await self.service.latest_draft(self.post))['draft_version'], 3)

    async def test_manual_edit_retry_after_invalidation_failure(self):
        event = await self.edit_event('brief', 'Changed')
        self.sheets.fail_update = self.cfg.sheet_texts
        with self.assertRaises(RuntimeError):
            await self.controller.handle(event)
        self.assertIsNotNone(await self.db.get_session(2))
        self.new_service()
        await self.controller.handle(event)
        self.assertEqual(len(self.sheets.data[self.cfg.sheet_plan]), 2)
        self.assertEqual(len(self.sheets.data[self.cfg.sheet_texts]), 2)
        self.assertIsNone(await self.db.get_session(2))

    async def test_plan_change_blocks_approval_of_old_text(self):
        self.llm.json_response.return_value = self.proposal()
        await self.service.build_plan(self.job(41, period_id='P'))
        with self.assertRaisesRegex(ValueError, 'подготовьте'):
            await self.service.approve_draft(self.post, 1, DraftDecision.APPROVED.value, 2)

    async def test_rewrite_pending_review_can_be_approved(self):
        await self.service.rewrite_draft(self.job(42, post_id=self.post, draft_version=1, comment='Rewrite'))
        await self.service.approve_draft(self.post, 2, DraftDecision.APPROVED.value, 2)
        self.assertEqual(self.sheets.data[self.cfg.sheet_texts][-1]['Статус'], DraftDecision.APPROVED.value)

    async def test_protected_manual_content_is_preserved(self):
        self.sheets.data[self.cfg.sheet_plan][0]['Зафиксировано человеком'] = 'да'
        before = copy.deepcopy(self.sheets.data)
        self.llm.json_response.return_value = self.proposal('Overwrite manual')
        await self.service.build_plan(self.job(43, period_id='P'))
        self.assertEqual(self.sheets.data, before)

    async def test_same_content_after_approval_does_not_create_new_revision(self):
        self.llm.json_response.return_value = self.proposal()
        await self.service.build_plan(self.job(44, period_id='P'))
        await self.service.set_plan_decision(self.post, PlanDecision.APPROVED.value, 2)
        before = copy.deepcopy(self.sheets.data)
        await self.service.build_plan(self.job(45, period_id='P'))
        self.assertEqual(self.sheets.data, before)

    async def test_new_plan_job_finishes_interrupted_revision_first(self):
        self.llm.json_response.side_effect = [self.proposal(), self.proposal('Next change')]
        self.sheets.fail_after_append = self.cfg.sheet_plan
        with self.assertRaises(RuntimeError):
            await self.service.build_plan(self.job(46, period_id='P'))
        self.new_service()
        await self.service.build_plan(self.job(47, period_id='P'))
        history = self.sheets.data[self.cfg.sheet_plan]
        self.assertEqual([row['Версия плана'] for row in history], [1, 2, 3])
        self.assertEqual([row['Решение редактора'] for row in history[:2]],
                         [PlanDecision.SUPERSEDED.value, PlanDecision.SUPERSEDED.value])
        self.assertEqual((await self.service.get_plan(self.post))['Краткое описание'], 'Next change')
