import tempfile, unittest
from pathlib import Path

from sens_mms.inputs import load_recipients, load_template, validate_images

TEST_BODY = "[Synthetic title]\r\n\r\nSynthetic message. \r\n"
TEST_SUBJECT = None


class InputTests(unittest.TestCase):
    def test_tab_dedup(self):
        p = Path(tempfile.mkdtemp()) / "r.csv"
        p.write_text("number\tname\n010-1234-5678\tA\n01012345678\tB\n", encoding="utf-8")
        self.assertEqual(load_recipients(p).valid_numbers, ("01012345678",))

    def test_invalid_failure(self):
        p = Path(tempfile.mkdtemp()) / "r.csv"
        p.write_text("number\n010-ABCD-0000\n", encoding="utf-8")
        f = load_recipients(p).failures[0]
        self.assertEqual((f.attempts, f.error_status), (0, "VALIDATION_ERROR"))

    def test_message_body_matches_selected_utf8_bytes(self):
        root = Path(tempfile.mkdtemp())
        folder = root / "input" / "notice"
        folder.mkdir(parents=True)
        raw = TEST_BODY.encode("utf-8")
        (folder / "message.txt").write_bytes(raw)
        jpeg = b"\xff\xd8\xff\xc0\x00\x11\x08\x00\x01\x00\x01" + b"\x00" * 8 + b"\xff\xd9"
        (folder / "photo.jpg").write_bytes(jpeg)
        self.assertEqual(load_template(root, "notice").content.encode("utf-8"), raw)

    def test_image_validation_retains_an_immutable_copy_of_approved_bytes(self):
        root = Path(tempfile.mkdtemp())
        first = b"\xff\xd8\xff\xc0\x00\x11\x08\x00\x01\x00\x01" + b"\x00" * 8 + b"\xff\xd9"
        (root / "mms_01_intro.jpg").write_bytes(first)

        infos = validate_images(root)
        (root / "mms_01_intro.jpg").write_bytes(b"mutated-after-validation")

        self.assertEqual(len(infos), 1)
        self.assertEqual(infos[0].data, first)


if __name__ == "__main__":
    unittest.main()
