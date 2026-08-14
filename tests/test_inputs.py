import tempfile, unittest
from pathlib import Path
from sens_mms.inputs import load_recipients, MESSAGE_BODY, validate_images

class InputTests(unittest.TestCase):
    def test_tab_dedup(self):
        p=Path(tempfile.mkdtemp())/'r.csv'; p.write_text('number\tname\n010-1234-5678\tA\n01012345678\tB\n',encoding='utf-8')
        self.assertEqual(load_recipients(p).valid_numbers, ('01012345678',))
    def test_invalid_failure(self):
        p=Path(tempfile.mkdtemp())/'r.csv'; p.write_text('number\n010-ABCD-0000\n',encoding='utf-8')
        f=load_recipients(p).failures[0]; self.assertEqual((f.attempts,f.error_status),(0,'VALIDATION_ERROR'))
    def test_body_is_exact_approved_text(self):
        self.assertEqual(MESSAGE_BODY, """[개업소연 안내]

안녕하세요.
세무사 윤성중 입니다.
6월 말일자로 국세청에서 퇴직하고 호연회계법인에서 새로 시작하게 되었습니다.
많은 응원 부탁드립니다.

감사합니다.""")

    def test_image_validation_retains_an_immutable_copy_of_approved_bytes(self):
        root = Path(tempfile.mkdtemp())
        first = b"\xff\xd8\xff\xc0\x00\x11\x08\x00\x01\x00\x01" + b"\x00" * 8 + b"\xff\xd9"
        second = b"\xff\xd8\xff\xc0\x00\x11\x08\x00\x02\x00\x02" + b"\x00" * 8 + b"\xff\xd9"
        (root / "mms_01_intro.jpg").write_bytes(first)
        (root / "mms_02_details.jpg").write_bytes(second)

        infos = validate_images(root)
        (root / "mms_01_intro.jpg").write_bytes(b"mutated-after-validation")

        self.assertEqual(infos[0].data, first)
        self.assertEqual(infos[1].data, second)
