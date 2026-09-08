import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from sens_mms import inputs
from sens_mms.cli import main
from sens_mms.preflight import build_preflight
from sens_mms.results import ResultStore
from tests.test_workflow import tiny_jpeg, config


class Terminal(StringIO):
    def isatty(self):
        return True


class TemplateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.folder = self.root / 'input' / 'notice'
        self.folder.mkdir(parents=True)
        self.body = '[Title]\r\n\r\nHello. \r\n'.encode('utf-8')
        (self.folder / 'message.txt').write_bytes(self.body)
        (self.folder / '02_photo.jpeg').write_bytes(tiny_jpeg())
        (self.root / 'receiving_numbers.csv').write_text('number\n01012345678\n')

    def test_selected_bytes_and_name_bind_approval(self):
        store = ResultStore.for_root(self.root)
        first = build_preflight(self.root, config(), store, template_name='notice')
        self.assertEqual(first.body.encode('utf-8'), self.body)
        self.assertEqual(first.content, first.body)
        self.assertIsNone(first.subject)
        self.assertEqual(first.to_public_dict()['template_name'], 'notice')
        (self.folder / 'message.txt').write_bytes(b'changed\r\n')
        second = build_preflight(self.root, config(), store, template_name='notice')
        self.assertNotEqual(first.approval_token, second.approval_token)
        self.folder.rename(self.root / 'input' / 'renamed')
        third = build_preflight(self.root, config(), store, template_name='renamed')
        self.assertNotEqual(second.approval_token, third.approval_token)

    def test_bom_and_sorted_one_or_two_images(self):
        (self.folder / 'message.txt').write_bytes(b'\xef\xbb\xbf' + self.body)
        (self.folder / '01_photo.JPG').write_bytes(tiny_jpeg(20))
        loaded = inputs.load_template(self.root, 'notice')
        self.assertEqual(loaded.content.encode('utf-8'), self.body)
        self.assertEqual([image.name for image in loaded.images], ['01_photo.JPG', '02_photo.jpeg'])

    def test_invalid_body_is_rejected(self):
        for raw in (b'', b' \r\n\t', b'\xff', '😀'.encode(), b'x' * 2001):
            with self.subTest(raw=raw[:8]):
                (self.folder / 'message.txt').write_bytes(raw)
                with self.assertRaises(inputs.TemplateError):
                    inputs.load_template(self.root, 'notice')

    def test_invalid_names_and_images_are_rejected(self):
        for name in ('../notice', '.', '..', '/notice', 'C:\\notice', 'a/b', 'a\\b', 'bad\nname'):
            with self.subTest(name=name), self.assertRaises(inputs.TemplateError):
                inputs.load_template(self.root, name)
        for raw in (b'bad', tiny_jpeg(0), tiny_jpeg(height=0), tiny_jpeg(1501), tiny_jpeg(height=1441), tiny_jpeg() + b'x' * (300 * 1024)):
            (self.folder / '02_photo.jpeg').write_bytes(raw)
            with self.assertRaises(inputs.TemplateError):
                inputs.load_template(self.root, 'notice')

    def test_selection_is_before_credentials_and_never_defaults(self):
        for command in ('preflight', 'live'):
            with self.subTest(command=command), patch('sens_mms.cli.load_config') as load:
                output = StringIO()
                code = main([command], root=self.root, stdin=StringIO('1\n'), stderr=StringIO(), stdout=output)
                self.assertEqual(code, 2)
                self.assertEqual(json.loads(output.getvalue())['status'], 'BLOCKED')
                load.assert_not_called()

    def test_terminal_selection_sorted_menu_and_retry(self):
        other = self.root / 'input' / 'alpha'
        other.mkdir()
        (other / 'message.txt').write_bytes(b'Alpha')
        (other / 'photo.jpg').write_bytes(tiny_jpeg())
        output, errors = StringIO(), StringIO()
        with patch('sens_mms.cli.load_config', return_value=config()):
            code = main(['preflight'], root=self.root, stdin=Terminal('bad\n0\n2\n'), stderr=errors, stdout=output)
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue())['template_name'], 'notice')
        self.assertLess(errors.getvalue().index('alpha'), errors.getvalue().index('notice'))
        self.assertIn('1. alpha — 이미지 1장', errors.getvalue())
        self.assertIn('사용할 템플릿을 선택하세요.', errors.getvalue())

    def test_flag_skips_input_and_invalid_template_skips_credentials(self):
        class NoRead(StringIO):
            def readline(self, *args):
                raise AssertionError('stdin must not be read')
        with patch('sens_mms.cli.load_config', return_value=config()):
            output = StringIO()
            self.assertEqual(main(['preflight', '--template', 'notice'], root=self.root, stdin=NoRead(), stdout=output, stderr=StringIO()), 0)
        (self.folder / 'message.txt').unlink()
        with patch('sens_mms.cli.load_config') as load:
            self.assertEqual(main(['preflight', '--template', 'notice'], root=self.root, stdin=NoRead(), stdout=StringIO(), stderr=StringIO()), 2)
            load.assert_not_called()

    def test_missing_empty_or_three_image_template_stops_safely(self):
        for count in (0, 3):
            with self.subTest(count=count):
                for path in self.folder.glob('*.jp*'):
                    path.unlink()
                for index in range(count):
                    (self.folder / f'{index}.jpg').write_bytes(tiny_jpeg())
                with self.assertRaises(inputs.TemplateError):
                    inputs.load_template(self.root, 'notice')
        (self.folder / 'message.txt').unlink()
        with self.assertRaises(inputs.TemplateError):
            inputs.load_template(self.root, 'notice')

    def test_terminal_eof_cancel_and_live_never_prompt(self):
        for command, stream in (('preflight', Terminal('')), ('preflight', Terminal('q\n')), ('live', Terminal('1\n'))):
            output, errors = StringIO(), StringIO()
            with self.subTest(command=command), patch('sens_mms.cli.load_config') as load:
                self.assertEqual(main([command], root=self.root, stdin=stream, stdout=output, stderr=errors), 2)
                load.assert_not_called()
                if command == 'live':
                    self.assertEqual(errors.getvalue(), '')
                    self.assertEqual(stream.tell(), 0)

    def test_missing_templates_and_unknown_errors_are_safe(self):
        import shutil
        shutil.rmtree(self.root / 'input')
        output = StringIO()
        self.assertEqual(main(['preflight'], root=self.root, stdin=Terminal(), stdout=output, stderr=StringIO()), 2)
        self.assertIn('input/NAME', json.loads(output.getvalue())['message'])
        output = StringIO()
        with patch('sens_mms.cli._select_template', side_effect=RuntimeError('PRIVATE_MARKER')):
            self.assertEqual(main(['preflight'], root=self.root, stdin=Terminal(), stdout=output, stderr=StringIO()), 2)
        self.assertNotIn('PRIVATE_MARKER', output.getvalue())

    def test_symlink_and_junction_paths_are_rejected_before_read(self):
        candidates = (self.root / 'input', self.folder, self.folder / 'message.txt', self.folder / '02_photo.jpeg')
        for kind in ('is_symlink', 'is_junction'):
            for candidate in candidates:
                read = Path.read_bytes
                reads = []
                def capture(path):
                    reads.append(path)
                    return read(path)
                with self.subTest(kind=kind, path=candidate.name), patch.object(Path, kind, lambda path: path == candidate), patch.object(Path, 'read_bytes', capture):
                    with self.assertRaises(inputs.TemplateError):
                        inputs.load_template(self.root, 'notice')
                self.assertNotIn(candidate, reads)

    def test_resolved_escape_is_rejected_before_read(self):
        original = Path.resolve
        candidate = self.folder / 'message.txt'
        def escaping(path, *args, **kwargs):
            if path == candidate:
                return self.root / 'outside.txt'
            return original(path, *args, **kwargs)
        with patch.object(Path, 'resolve', escaping), patch.object(Path, 'read_bytes') as read:
            with self.assertRaises(inputs.TemplateError):
                inputs.load_template(self.root, 'notice')
            read.assert_not_called()

    def test_menu_rejects_control_character_names_without_echo(self):
        original = Path.iterdir
        def unsafe(path):
            if path == self.root / 'input':
                return iter([path / 'PRIVATE\nMARKER'])
            return original(path)
        output, errors = StringIO(), StringIO()
        with patch.object(Path, 'iterdir', unsafe):
            self.assertEqual(main(['preflight'], root=self.root, stdin=Terminal('1\n'), stdout=output, stderr=errors), 2)
        self.assertNotIn('PRIVATE', output.getvalue() + errors.getvalue())

    def test_exact_selected_content_and_frozen_images_reach_http(self):
        import base64
        import hashlib
        from sens_mms import workflow as workflow_module
        from tests.test_api import client
        from tests.test_cli import ScriptedHttpTransport, success_responses, api_response
        from tests.test_preflight import jpeg
        from tests.test_workflow import make_workflow
        first, second = jpeg(b'A'), jpeg(b'Z')
        (self.folder / '01_intro.jpg').write_bytes(first)
        (self.folder / '02_photo.jpeg').write_bytes(second)
        self.body = '[Title]\r\n\r\n\uc548\ub155\ud558\uc138\uc694. \r\n'.encode('utf-8')
        (self.folder / 'message.txt').write_bytes(b'\xef\xbb\xbf' + self.body)
        transport = ScriptedHttpTransport([api_response(200, {'fileId': 'intro-file'}), *success_responses()])
        workflow = make_workflow(self.root, ResultStore.for_root(self.root), client(transport))
        token = workflow.current_token()
        original = workflow_module.build_preflight
        def mutate_after_validation(*args, **kwargs):
            report = original(*args, **kwargs)
            self.assertEqual(report.approved_message_bytes, b'\xef\xbb\xbf' + self.body)
            for path in self.folder.iterdir():
                path.unlink()
            return report
        with patch('sens_mms.workflow.build_preflight', side_effect=mutate_after_validation), patch('sens_mms.pipeline._log_single_line'):
            summary = workflow.run_live(token)
        self.assertEqual(summary.sent, 1)
        uploads = [json.loads(call[3]) for call in transport.calls if call[0] == 'POST' and call[1].endswith('/files')]
        self.assertEqual([base64.b64decode(item['fileBody']) for item in uploads], [first, second])
        self.assertEqual([item['fileName'] for item in uploads], [hashlib.sha256(raw).hexdigest()[:36] + '.jpg' for raw in (first, second)])
        self.assertEqual(len(first), len(second))
        self.assertNotEqual(uploads[0]['fileName'], uploads[1]['fileName'])
        for item in uploads:
            self.assertTrue(item['fileName'].isascii())
            self.assertLessEqual(len(item['fileName']), 40)
        posts = [json.loads(call[3]) for call in transport.calls if call[0] == 'POST' and call[1].endswith('/messages')]
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0]['content'].encode('utf-8'), self.body)
        self.assertNotIn('subject', posts[0])
        self.assertEqual(posts[0]['files'][0], {'fileId': 'intro-file'})

    def test_old_token_after_body_change_blocks_all_api_calls(self):
        from tests.test_api import client
        from tests.test_cli import ScriptedHttpTransport
        from tests.test_workflow import make_workflow
        from sens_mms.workflow import ApprovalTokenMismatch
        transport = ScriptedHttpTransport()
        workflow = make_workflow(self.root, ResultStore.for_root(self.root), client(transport))
        token = workflow.current_token()
        (self.folder / 'message.txt').write_bytes(b'Unapproved')
        with self.assertRaises(ApprovalTokenMismatch):
            workflow.run_live(token)
        self.assertEqual(transport.calls, [])

    def test_send_apis_require_explicit_content(self):
        from tests.test_api import client
        from tests.test_cli import ScriptedHttpTransport
        transport = ScriptedHttpTransport()
        api = client(transport)
        for method, args in ((api.send_one, ('01012345678', ('file',))), (api.send_mms, ('01012345678', ('file',))), (api.send_lms, ('01012345678',))):
            with self.assertRaises(TypeError):
                method(*args, content_type='COMM')
        self.assertEqual(transport.calls, [])

    def test_template_change_preserves_sent_and_ambiguous_no_repost(self):
        from sens_mms.results import ResultRow
        from tests.test_api import client
        from tests.test_cli import ScriptedHttpTransport
        from tests.test_workflow import make_workflow, RECIPIENT, DELIVERY_ID_1
        for status, sent in (('SENT', 'true'), ('PENDING_CONFIRMATION', '')):
            with self.subTest(status=status):
                store = ResultStore.for_root(self.root)
                row = ResultRow(RECIPIENT, DELIVERY_ID_1, status, sent, 1,
                                'request' if sent else '', 'message' if sent else '', None)
                store.upsert(row)
                store.write_atomic()
                (self.folder / 'message.txt').write_bytes(b'Newly selected content')
                transport = ScriptedHttpTransport()
                workflow = make_workflow(self.root, store, client(transport))
                workflow.run_live(workflow.current_token())
                self.assertEqual(transport.calls, [])
                self.assertEqual(ResultStore.for_root(self.root).load()[RECIPIENT], row)

    def test_exact_euc_kr_byte_limit_and_bom_binding(self):
        raw = ('\uac00' * 1000).encode('utf-8')
        (self.folder / 'message.txt').write_bytes(raw)
        store = ResultStore.for_root(self.root)
        first = build_preflight(self.root, config(), store, template_name='notice')
        self.assertEqual(len(first.content.encode('euc-kr')), 2000)
        (self.folder / 'message.txt').write_bytes(b'\xef\xbb\xbf' + raw)
        second = build_preflight(self.root, config(), store, template_name='notice')
        self.assertEqual(first.body, second.body)
        self.assertNotEqual(first.approval_token, second.approval_token)
        (self.folder / 'message.txt').write_bytes(raw + b'x')
        with self.assertRaises(inputs.TemplateError):
            inputs.load_template(self.root, 'notice')

    def test_terminal_keyboard_interrupt_is_safe(self):
        class Interrupted(Terminal):
            def readline(self):
                raise KeyboardInterrupt
        output = StringIO()
        with patch('sens_mms.cli.load_config') as load:
            self.assertEqual(main(['preflight'], root=self.root, stdin=Interrupted(), stdout=output, stderr=StringIO()), 2)
            load.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())['status'], 'BLOCKED')
