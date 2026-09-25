from __future__ import annotations
import asyncio
import io
import logging
import os
import socket
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
import httpx
from qifa_smm.config import Settings
from qifa_smm.db import Database
from qifa_smm.domain import DraftDecision, TelegramEvent
from qifa_smm.extractors import ContentExtractor
from qifa_smm.google_workspace import DriveEntry, GoogleWorkspace
from qifa_smm.jobs import JobRunner
from qifa_smm.limits import FileLimitError, LimitedBuffer
from qifa_smm.pipelines import PipelineService
from qifa_smm.security import SecretFilter, telegram_response
from qifa_smm.telegram import TelegramAPI, TelegramController


def settings(**overrides):
    values = dict(_env_file=None, telegram_control_bot_token='control', telegram_publish_bot_token='publisher',
                  telegram_test_channel_id='-1001', telegram_allowed_user_ids={1, 2}, telegram_approver_user_ids={2},
                  google_oauth_token_file=Path('/tmp/unused'), google_spreadsheet_id='sheet', google_drive_root_folder_id='root')
    values.update(overrides)
    with patch.dict(os.environ, {}, clear=True):
        return Settings(**values)


class ConfigTests(unittest.TestCase):
    def test_ids_from_environment(self):
        base = dict(TELEGRAM_CONTROL_BOT_TOKEN='control', TELEGRAM_PUBLISH_BOT_TOKEN='publisher',
                    TELEGRAM_TEST_CHANNEL_ID='-1', GOOGLE_OAUTH_TOKEN_FILE='/tmp/unused',
                    GOOGLE_SPREADSHEET_ID='s', GOOGLE_DRIVE_ROOT_FOLDER_ID='r')
        for raw, expected in [('1', {1}), ('1,2', {1, 2}), ('1, 2, 3', {1, 2, 3}), ('', set())]:
            with self.subTest(raw=raw), patch.dict(os.environ, {**base, 'TELEGRAM_ALLOWED_USER_IDS': raw}, clear=True):
                self.assertEqual(Settings(_env_file=None).telegram_allowed_user_ids, expected)
        for raw in ('abc', '0', '-1', '1.5'):
            with self.subTest(raw=raw), patch.dict(os.environ, {**base, 'TELEGRAM_ALLOWED_USER_IDS': raw}, clear=True):
                with self.assertRaises(ValueError):
                    Settings(_env_file=None)
        with patch.dict(os.environ, {**base, 'TELEGRAM_ALLOWED_USER_IDS': '1', 'TELEGRAM_APPROVER_USER_IDS': '2'}, clear=True):
            with self.assertRaises(ValueError):
                Settings(_env_file=None)


class LogTests(unittest.TestCase):
    def test_secrets_removed_from_args_and_traceback(self):
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        handler.addFilter(SecretFilter(('control-secret', 'publish-secret')))
        logger = logging.Logger('isolated', logging.INFO)
        logger.addHandler(handler)
        logger.info('URL %s', 'https://api.telegram.org/botcontrol-secret/getUpdates')
        try:
            raise RuntimeError('publish-secret')
        except RuntimeError:
            logger.exception('failed %s', 'control-secret')
        for secret in ('control-secret', 'publish-secret'):
            self.assertNotIn(secret, output.getvalue())
        self.assertIn('[REDACTED]', output.getvalue())

    def test_safe_telegram_error(self):
        response = httpx.Response(403, request=httpx.Request('POST', 'https://api.telegram.org/botsecret/sendMessage'),
                                  json={'ok': False, 'description': 'forbidden secret'})
        with self.assertRaises(RuntimeError) as error:
            telegram_response(response, 'sendMessage', 'secret')
        self.assertNotIn('secret', str(error.exception))
        self.assertNotIn('https://', str(error.exception))
        self.assertIn('403', str(error.exception))


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_notification_failures_do_not_stop_worker(self):
        for fails in (False, True):
            with self.subTest(handler_fails=fails):
                jobs = [SimpleNamespace(id=n, type='work', reply_chat_id=1) for n in (1, 2)]
                db = AsyncMock()
                db.claim_next_job.side_effect = [*jobs, asyncio.CancelledError()]
                handler = AsyncMock(side_effect=ValueError('secret technical data')) if fails else AsyncMock(return_value='done')
                notify = AsyncMock(side_effect=RuntimeError('offline'))
                runner = JobRunner(db, {'work': handler}, notify, 0)
                with self.assertLogs('qifa_smm.jobs', level='ERROR'), self.assertRaises(asyncio.CancelledError):
                    await runner.run('worker')
                self.assertEqual(handler.await_count, 2)
                self.assertEqual(notify.await_count, 2)
                self.assertEqual((db.fail_job if fails else db.finish_job).await_count, 2)
                self.assertNotIn('secret technical data', str(notify.call_args_list))

    async def test_queue_error_retries(self):
        db = AsyncMock()
        db.claim_next_job.side_effect = [RuntimeError('busy'), asyncio.CancelledError()]
        with self.assertLogs('qifa_smm.jobs', level='ERROR'), self.assertRaises(asyncio.CancelledError):
            await JobRunner(db, {}, AsyncMock(), 0).run('worker')
        self.assertEqual(db.claim_next_job.await_count, 2)


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / 'state.sqlite')
        await self.db.initialize()
        self.workspace = AsyncMock()
        self.sheets = AsyncMock()
        self.publisher = AsyncMock()
        self.publisher.publish_test.return_value = [100]
        self.service = PipelineService(settings(), self.db, self.workspace, self.sheets, AsyncMock(), self.publisher)
        self.plan = {'post_id': 'P', 'plan_version': 1, 'Папка медиа': 'folder', 'Канал': 'Telegram', 'Дата': '2026-09-25', 'Время': '12:00'}
        self.entry = DriveEntry('file', '01_photo.jpg', 'image/jpeg', '', '3', '', '', 'folder')
        self.workspace.list_direct_files.return_value = [self.entry]
        self.workspace.download_file.return_value = (b'abc', 'image/jpeg')
        self.service.get_plan = AsyncMock(side_effect=lambda _: self.plan.copy())
        fingerprint, _ = await self.service._content_fingerprint(self.plan, 1, 'approved text')
        self.draft = {'post_id': 'P', 'plan_version': 1, 'draft_version': 1, 'Текст': 'approved text',
                      'Статус': DraftDecision.REVIEW.value, 'approval_fingerprint': fingerprint, '_row_number': 2}
        self.service.latest_draft = AsyncMock(side_effect=lambda _: self.draft)
        self.sheets.headers.return_value = list(self.draft)
        self.job = SimpleNamespace(payload={'post_id': 'P', 'draft_version': 1}, requested_by=2)
        await self.service.approve_draft('P', 1, DraftDecision.APPROVED.value, 2)

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def test_publish_exact_approved_bytes_preserves_approval(self):
        before = await self.db.get_approval('P', 1, 1)
        await self.service.schedule_draft('P', 1, 2)
        self.assertEqual(before, await self.db.get_approval('P', 1, 1))
        await self.service.publish_test(self.job)
        self.assertEqual(before, await self.db.get_approval('P', 1, 1))
        self.assertEqual(self.publisher.publish_test.call_args.args[1][0].data, b'abc')

    async def test_text_change_blocks_and_requires_explicit_reapproval(self):
        self.draft['Текст'] = 'edited directly'
        with self.assertRaisesRegex(ValueError, 'повторное согласование'):
            await self.service.publish_test(self.job)
        self.publisher.publish_test.assert_not_awaited()
        self.assertEqual(self.draft['Статус'], DraftDecision.REVIEW.value)
        await self.service.approve_draft('P', 1, DraftDecision.APPROVED.value, 2)
        await self.service.publish_test(self.job)
        self.assertEqual(self.publisher.publish_test.call_args.args[0], 'edited directly')

    async def test_media_change_blocks(self):
        self.workspace.download_file.return_value = (b'xyz', 'image/jpeg')
        with self.assertRaisesRegex(ValueError, 'повторное согласование'):
            await self.service.publish_test(self.job)
        self.publisher.publish_test.assert_not_awaited()

    async def test_plan_changes_block(self):
        for key, value in [('Канал', 'Changed'), ('Дата', '2030-01-01'), ('Время', '13:00'), ('Папка медиа', 'other')]:
            with self.subTest(key=key):
                old = self.plan[key]
                self.plan[key] = value
                with self.assertRaisesRegex(ValueError, 'повторное согласование'):
                    await self.service._verify_content('P', self.draft)
                self.plan[key] = old
                fingerprint, _ = await self.service._content_fingerprint(self.plan, 1, self.draft['Текст'])
                self.draft['approval_fingerprint'] = fingerprint
        self.publisher.publish_test.assert_not_awaited()

    async def test_missing_db_approval_cannot_be_forged_in_sheet(self):
        async with self.db.connection() as db:
            await db.execute('DELETE FROM approvals')
            await db.commit()
        with self.assertRaisesRegex(ValueError, 'согласование'):
            await self.service.publish_test(self.job)
        self.publisher.publish_test.assert_not_awaited()

    async def test_unprivileged_actions_blocked(self):
        for action in (lambda: self.service.schedule_draft('P', 1, 1),
                       lambda: self.service.approve_draft('P', 1, DraftDecision.APPROVED.value, 1),
                       lambda: self.service.publish_test(SimpleNamespace(payload=self.job.payload, requested_by=1)),
                       lambda: self.service.approve_period_plan('period', 1)):
            with self.assertRaisesRegex(ValueError, 'права'):
                await action()
        self.publisher.publish_test.assert_not_awaited()

    async def test_declined_draft_cannot_be_scheduled(self):
        await self.service.approve_draft('P', 1, DraftDecision.REJECTED.value, 2)
        with self.assertRaises(ValueError):
            await self.service.schedule_draft('P', 1, 2)

    async def test_reference_can_be_added_manually_without_approval(self):
        await self.service.approve_draft('P', 1, DraftDecision.REJECTED.value, 2)
        self.draft['Текст'] = 'manually selected reference'
        self.sheets.headers.return_value = ['Текст поста', 'Добавил']
        await self.service.add_reference('P', 'тон', '', 1)
        self.sheets.append_dicts.assert_awaited_once_with(
            self.service.settings.sheet_references,
            [{'Текст поста': 'manually selected reference', 'Добавил': '1'}],
        )

    async def test_changed_content_cannot_be_approved_on_first_click(self):
        self.draft['Текст'] = 'modified'
        with self.assertRaisesRegex(ValueError, 'повторное согласование'):
            await self.service.approve_draft('P', 1, DraftDecision.APPROVED.value, 2)


class NetworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_internal_addresses_blocked(self):
        for host in ('127.0.0.1', '10.0.0.1', '192.168.1.1', '169.254.169.254', '::1', 'fc00::1', '224.0.0.1'):
            url = f'http://[{host}]/' if ':' in host else f'http://{host}/'
            with self.subTest(host=host), patch.object(asyncio.get_running_loop(), 'getaddrinfo', AsyncMock(return_value=[(0, 0, 0, '', (host, 80))])):
                with self.assertRaises(ValueError):
                    await ContentExtractor._public_url(url)
        for url in ('http://localhost/', 'http://service.local/', 'http://service.internal/', 'http://example.com:8080/', 'http://user:password@example.com/', 'file:///etc/passwd'):
            with self.subTest(url=url), self.assertRaises(ValueError):
                await ContentExtractor._public_url(url)

    async def test_public_redirect_to_private_blocked_and_ip_pinned(self):
        requests = []
        def respond(request):
            requests.append(request)
            return httpx.Response(302, headers={'location': 'http://127.0.0.1/'})
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        async def resolve(host, *args, **kwargs):
            return [(socket.AF_INET, socket.SOCK_STREAM, 0, '', ('93.184.216.34' if host == 'example.com' else '127.0.0.1', 80))]
        with patch('qifa_smm.extractors.httpx.AsyncClient', return_value=client), patch.object(asyncio.get_running_loop(), 'getaddrinfo', side_effect=resolve):
            self.assertEqual(await ContentExtractor().fetch_url('https://example.com/'), ('', False))
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].url.host, '93.184.216.34')
        self.assertEqual(requests[0].headers['host'], 'example.com')
        self.assertEqual(requests[0].extensions['sni_hostname'], 'example.com')

    async def test_response_limit(self):
        client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b'x' * 11)))
        with patch('qifa_smm.extractors.httpx.AsyncClient', return_value=client), patch.object(ContentExtractor, '_public_url', AsyncMock(return_value=(httpx.URL('http://93.184.216.34/'), 'example.com'))):
            self.assertEqual(await ContentExtractor(max_download_bytes=10).fetch_url('http://example.com/'), ('', False))


class LimitTests(unittest.IsolatedAsyncioTestCase):
    def test_archive_and_buffer_limits(self):
        data = io.BytesIO()
        with zipfile.ZipFile(data, 'w', zipfile.ZIP_DEFLATED) as archive:
            archive.writestr('xl/sharedStrings.xml', '<t>' + 'x' * 1000 + '</t>')
        with self.assertRaises(FileLimitError):
            ContentExtractor(max_archive_bytes=100).from_bytes('test.xlsx', 'application/octet-stream', data.getvalue())
        with self.assertRaises(FileLimitError):
            ContentExtractor(max_download_bytes=2).from_bytes('test.txt', 'text/plain', b'abc')
        buffer = LimitedBuffer(3)
        buffer.write(b'abc')
        with self.assertRaises(FileLimitError):
            buffer.write(b'd')
        self.assertEqual(buffer.getvalue(), b'abc')

    async def test_drive_metadata_limit_prevents_download(self):
        workspace = object.__new__(GoogleWorkspace)
        entry = DriveEntry('id', 'big.pdf', 'application/pdf', '', '100', '', '', '')
        with self.assertRaises(FileLimitError):
            await workspace.download_file(entry, max_bytes=10)

    async def test_raw_sheet_writes(self):
        workspace = object.__new__(GoogleWorkspace)
        workspace.spreadsheet_id = 'sheet'
        workspace.sheets = MagicMock()
        await workspace.append_values('A1', [['=IMPORTXML(...)']])
        await workspace.update_values('A1', [['=IMPORTXML(...)']])
        values = workspace.sheets.spreadsheets.return_value.values.return_value
        self.assertEqual(values.append.call_args.kwargs['valueInputOption'], 'RAW')
        self.assertEqual(values.update.call_args.kwargs['valueInputOption'], 'RAW')


class MediaTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_caption_rejected_without_exception(self):
        api = AsyncMock()
        controller = TelegramController(settings(), api, AsyncMock(), AsyncMock())
        for field, value in [('photo', [{'file_id': 'id'}]), ('video', {'file_id': 'id'})]:
            await controller.handle(TelegramEvent(update_id=1, user_id=1, chat_id=1, kind='message', text='', raw={'message': {field: value}}))
        self.assertEqual(api.send_message.await_count, 2)
        api.download_file.assert_not_awaited()

    async def test_document_name_used_without_caption(self):
        api, db, pipelines = AsyncMock(), AsyncMock(), AsyncMock()
        api.get_file.return_value = {'file_path': 'documents/file.pdf'}
        api.download_file.return_value = b'pdf'
        pipelines.upload_media.return_value = {'id': 'uploaded'}
        session = {'mode': 'media', 'payload': {'post_id': 'P', 'files': []}}
        db.get_session.return_value = session
        event = TelegramEvent(update_id=1, user_id=1, chat_id=1, kind='message', text='', raw={'message': {'document': {'file_id': 'id', 'file_name': 'Отчёт.pdf', 'mime_type': 'application/pdf'}}})
        await TelegramController(settings(), api, db, pipelines).handle(event)
        self.assertEqual(pipelines.upload_media.call_args.args[1], 'Отчёт.pdf')

    async def test_telegram_download_limit(self):
        api = TelegramAPI('dummy', 10)
        await api.client.aclose()
        api.client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b'1234')))
        try:
            with self.assertRaises(ValueError):
                await api.download_file('file', 3)
        finally:
            await api.close()


class AdditionalRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_archive_count_and_pdf_pages(self):
        data = io.BytesIO()
        with zipfile.ZipFile(data, 'w') as archive:
            archive.writestr('one.xml', '')
            archive.writestr('two.xml', '')
        with self.assertRaises(FileLimitError):
            ContentExtractor(max_archive_files=1).from_bytes('many.docx', '', data.getvalue())
        with patch('qifa_smm.extractors.PdfReader') as reader:
            reader.return_value.pages = [MagicMock(), MagicMock()]
            with self.assertRaises(FileLimitError):
                ContentExtractor(max_pdf_pages=1).from_bytes('big.pdf', 'application/pdf', b'pdf')
            reader.return_value.pages[0].extract_text.assert_not_called()

    async def test_drive_stream_limit_with_unknown_size(self):
        workspace = object.__new__(GoogleWorkspace)
        workspace.drive = MagicMock()
        entry = DriveEntry('id', 'big.pdf', 'application/pdf', '', '', '', '', '')
        def downloader(buffer, request, **kwargs):
            mock = MagicMock()
            mock.next_chunk.side_effect = lambda: (buffer.write(b'123456'), True)
            return mock
        with patch('qifa_smm.google_workspace.MediaIoBaseDownload', side_effect=downloader):
            with self.assertRaises(FileLimitError):
                await workspace.download_file(entry, 5)

    async def test_drive_direct_listing_fetches_all_pages(self):
        workspace = object.__new__(GoogleWorkspace)
        workspace.drive = MagicMock()
        workspace.drive.files.return_value.list.return_value.execute.side_effect = [
            {'nextPageToken': 'next', 'files': [{'id': '1', 'name': 'a', 'mimeType': 'image/jpeg'}]},
            {'files': [{'id': '2', 'name': 'b', 'mimeType': 'image/jpeg'}]},
        ]
        entries = await workspace.list_direct_files('folder')
        self.assertEqual([entry.id for entry in entries], ['1', '2'])
        self.assertEqual(workspace.drive.files.return_value.list.call_args.kwargs['pageToken'], 'next')

    async def test_unprivileged_callback_and_date_do_not_schedule(self):
        service = PipelineService(settings(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())
        service.schedule_draft = AsyncMock()
        db = AsyncMock()
        controller = TelegramController(settings(), AsyncMock(), db, service)
        event = TelegramEvent(update_id=1, user_id=1, chat_id=1, kind='callback', callback_data='pub:schedule:P:1')
        with self.assertRaisesRegex(ValueError, 'права'):
            await controller.handle(event)
        event.kind = 'message'
        event.text = '2026-10-01 12:00'
        db.get_session.return_value = {'mode': 'publish', 'payload': {'post_id': 'P', 'draft_version': 1}}
        with self.assertRaisesRegex(ValueError, 'права'):
            await controller.handle(event)
        service.schedule_draft.assert_not_awaited()
        db.enqueue_job.assert_not_awaited()

    async def test_telegram_success_and_failure_logs_are_redacted(self):
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        handler.addFilter(SecretFilter(('control-secret', 'publisher-secret')))
        logger = logging.getLogger('httpx')
        old_level = logger.level
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        try:
            for token in ('control-secret', 'publisher-secret'):
                api = TelegramAPI(token, 10)
                await api.client.aclose()
                replies = iter([httpx.Response(200, json={'ok': True, 'result': []}),
                                httpx.Response(401, json={'ok': False, 'description': 'invalid token'})])
                api.client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: next(replies)))
                try:
                    await api.call('getUpdates')
                    with self.assertRaises(RuntimeError) as error:
                        await api.call('getUpdates')
                    self.assertNotIn(token, str(error.exception))
                finally:
                    await api.close()
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)
        self.assertNotIn('control-secret', output.getvalue())
        self.assertNotIn('publisher-secret', output.getvalue())
        self.assertIn('[REDACTED]', output.getvalue())
