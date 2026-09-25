from __future__ import annotations

from contextlib import redirect_stdout, redirect_stderr
from datetime import UTC, datetime, timedelta
import io
import json
import logging
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from google.oauth2.credentials import Credentials
from qifa_smm.google_auth import (
    AUTH_ERROR, OAuthSettings, SCOPES, authorize, load_credentials, save_token,
)
from qifa_smm.google_workspace import GoogleWorkspace, SheetRepository


class OAuthTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'token.json'
        self.credentials = Credentials(
            token='secret-access', refresh_token='secret-refresh',
            token_uri='https://oauth2.googleapis.com/token',
            client_id='test-client', client_secret='secret-client', scopes=SCOPES,
            expiry=datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=1),
        )
        levels = {name: obj.level for name, obj in logging.Logger.manager.loggerDict.items()
                  if isinstance(obj, logging.Logger)}
        self.addCleanup(lambda: [logging.getLogger(name).setLevel(level)
                                for name, level in levels.items()])

    def test_paths_without_service_settings(self):
        with patch.dict(os.environ, {}, clear=True):
            settings = OAuthSettings(_env_file=None)
        self.assertEqual(settings.google_oauth_client_file, Path('secrets/google-oauth-client.json'))
        self.assertEqual(settings.google_oauth_token_file, Path('secrets/google-oauth-token.json'))

    def test_valid_token_and_private_atomic_save(self):
        save_token(self.credentials, self.path)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        with patch.object(Credentials, 'refresh') as refresh:
            loaded = load_credentials(self.path)
        refresh.assert_not_called()
        self.assertEqual(loaded.token, 'secret-access')
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_refresh_on_startup_and_during_transport_request(self):
        self.credentials.expiry = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=1)
        save_token(self.credentials, self.path)
        def refresh(creds, request):
            creds.token = 'new-access'
            creds.expiry = datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=1)
        with patch.object(Credentials, 'refresh', autospec=True, side_effect=refresh) as renew:
            loaded = load_credentials(self.path)
            self.assertEqual(renew.call_count, 1)
            self.assertEqual(json.loads(self.path.read_text())['token'], 'new-access')
            loaded.expiry = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=1)
            loaded.before_request(Mock(), 'GET', 'https://example.test', {})
            self.assertEqual(renew.call_count, 2)
        self.assertEqual(json.loads(self.path.read_text())['refresh_token'], 'secret-refresh')

    def test_missing_corrupt_or_wrong_scope_token(self):
        for content in (None, '{bad', json.dumps({'refresh_token': 'secret-refresh'})):
            if content is not None:
                self.path.write_text(content)
            with self.assertRaisesRegex(RuntimeError, 'qifa-google-auth'):
                load_credentials(self.path)
        self.credentials._scopes = ['https://www.googleapis.com/auth/drive']
        save_token(self.credentials, self.path)
        with self.assertRaisesRegex(RuntimeError, 'qifa-google-auth'):
            load_credentials(self.path)

    def test_refresh_failure_is_safe_and_preserves_file(self):
        self.credentials.expiry = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=1)
        save_token(self.credentials, self.path)
        before = self.path.read_bytes()
        with patch.object(Credentials, 'refresh', side_effect=ValueError('secret-refresh')):
            with self.assertRaises(RuntimeError) as error:
                load_credentials(self.path)
        self.assertEqual(str(error.exception), AUTH_ERROR)
        self.assertTrue(error.exception.__suppress_context__)
        self.assertEqual(self.path.read_bytes(), before)

    def test_authorization_browser_and_no_sensitive_logs(self):
        client = self.path.parent / 'client.json'
        client.write_text(json.dumps({'installed': {'client_secret': 'secret-client'}}))
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        logging.getLogger().addHandler(handler)
        self.addCleanup(logging.getLogger().removeHandler, handler)
        def browser(**kwargs):
            self.assertTrue(kwargs['open_browser'])
            self.assertEqual(kwargs['port'], 0)
            self.assertEqual(kwargs['access_type'], 'offline')
            self.assertEqual(kwargs['prompt'], 'consent')
            self.assertIsNone(kwargs['authorization_prompt_message'])
            for name in ('oauthlib.oauth2.rfc6749', 'google_auth_oauthlib.flow', 'urllib3.connectionpool'):
                logging.getLogger(name).error('secret-client secret-access secret-refresh https://auth.test')
            return self.credentials
        with patch('qifa_smm.google_auth.InstalledAppFlow.from_client_config') as factory:
            factory.return_value.run_local_server.side_effect = browser
            with redirect_stdout(output), redirect_stderr(output):
                authorize(client, self.path)
        self.assertEqual(output.getvalue(), '')
        self.assertTrue(self.path.exists())
        self.assertEqual(factory.call_args.kwargs['scopes'], SCOPES)

    def test_failed_authorization_preserves_existing_token(self):
        client = self.path.parent / 'client.json'
        client.write_text('{"installed": {}}')
        save_token(self.credentials, self.path)
        before = self.path.read_bytes()
        with patch('qifa_smm.google_auth.InstalledAppFlow.from_client_config') as factory:
            factory.return_value.run_local_server.side_effect = ValueError('secret-client')
            with self.assertRaisesRegex(RuntimeError, 'qifa-google-auth'):
                authorize(client, self.path)
        self.assertEqual(self.path.read_bytes(), before)

    def test_no_refresh_token_or_partial_consent_is_rejected(self):
        client = self.path.parent / 'client.json'
        client.write_text('{"installed": {}}')
        with patch('qifa_smm.google_auth.InstalledAppFlow.from_client_config') as factory:
            factory.return_value.run_local_server.return_value = self.credentials
            self.credentials._refresh_token = None
            with self.assertRaises(RuntimeError):
                authorize(client, self.path)
            self.credentials._refresh_token = 'secret-refresh'
            self.credentials._granted_scopes = [SCOPES[0]]
            with self.assertRaises(RuntimeError):
                authorize(client, self.path)
        self.assertFalse(self.path.exists())

    def test_failed_atomic_write_preserves_previous_token(self):
        save_token(self.credentials, self.path)
        before = self.path.read_bytes()
        with patch('qifa_smm.google_auth.os.replace', side_effect=OSError('secret-access')):
            with self.assertRaises(RuntimeError) as error:
                save_token(self.credentials, self.path)
        self.assertNotIn('secret-access', str(error.exception))
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_workspace_interfaces(self):
        save_token(self.credentials, self.path)
        with patch('qifa_smm.google_workspace.build') as build:
            workspace = GoogleWorkspace(str(self.path), 'sheet-id')
        self.assertEqual(workspace.spreadsheet_id, 'sheet-id')
        self.assertEqual([call.args[:2] for call in build.call_args_list], [('drive', 'v3'), ('sheets', 'v4')])
        self.assertIs(build.call_args_list[0].kwargs['credentials'], build.call_args_list[1].kwargs['credentials'])
        SheetRepository(workspace)
