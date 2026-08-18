import tempfile, unittest
from pathlib import Path
from sens_mms.inputs import load_recipients, MESSAGE_BODY, validate_images, split_message_body


EXPECTED_BODY = (
    "[개업소연 안내]\n\n"
    "안녕하세요.\n"
    "국세청에서의 오랜 경험을 바탕으로 호연회계법인에서 새로운 출발을 하게 된 윤성중 세무사입니다.\n\n"
    "그동안 보내주신 관심과 성원에 감사드리며, 앞으로도 많은 관심과 응원 부탁드립니다.\n\n"
    "새로운 시작을 기쁜 마음으로 함께해 주시면 감사하겠습니다.\n"
)


class InputTests(unittest.TestCase):
    def test_tab_dedup(self):
        p=Path(tempfile.mkdtemp())/'r.csv'; p.write_text('number\tname\n010-1234-5678\tA\n01012345678\tB\n',encoding='utf-8')
        self.assertEqual(load_recipients(p).valid_numbers, ('01012345678',))
    def test_invalid_failure(self):
        p=Path(tempfile.mkdtemp())/'r.csv'; p.write_text('number\n010-ABCD-0000\n',encoding='utf-8')
        f=load_recipients(p).failures[0]; self.assertEqual((f.attempts,f.error_status),(0,'VALIDATION_ERROR'))
    def test_message_body_matches_approved_utf8_bytes(self):
        """Catches any approved body byte or newline being changed or omitted."""
        self.assertEqual(MESSAGE_BODY.encode("utf-8"), EXPECTED_BODY.encode("utf-8"))

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

    def test_split_message_body_with_title(self):
        body = "[Test Title]\n\nHello World\nLine 2\n"
        title, rest = split_message_body(body)
        self.assertEqual(title, "[Test Title]")
        self.assertEqual(rest, "Hello World\nLine 2\n")

    def test_split_message_body_without_title(self):
        body = "Hello World\nLine 2\n"
        title, rest = split_message_body(body)
        self.assertEqual(title, " ")
        self.assertEqual(rest, "Hello World\nLine 2\n")

    def test_split_approved_message_body(self):
        title, rest = split_message_body(MESSAGE_BODY)
        self.assertEqual(title, "[개업소연 안내]")
        self.assertTrue(rest.startswith("안녕하세요."))
        self.assertTrue(rest.endswith("새로운 시작을 기쁜 마음으로 함께해 주시면 감사하겠습니다.\n"))

