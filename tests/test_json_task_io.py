from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import json_task_io


class JsonTaskIoTests(unittest.TestCase):
    def test_load_filter_update_and_finish(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_path = Path(tmp_dir)
            pdf_path = temp_path / "notice.pdf"
            image_path = temp_path / "pic.png"
            pdf_path.write_bytes(b"%PDF-1.4")
            image_path.write_bytes(b"\x89PNG\r\n\x1a\n")

            payload = {
                "start_time": "2026-04-07 20:00:00",
                "end_time": "",
                "total_count": 2,
                "template_content": "您好",
                "common_attachments": [{"file_path": str(pdf_path), "file_type": "pdf"}],
                "targets": [
                    {
                        "target_value": "张三",
                        "target_type": "person",
                        "message_mode": "template",
                        "message": "",
                        "attachment_mode": "common",
                        "attachments": [],
                        "send_status": "success",
                    },
                    {
                        "target_value": "高三1班",
                        "target_type": "group",
                        "message_mode": "custom",
                        "message": "请查看附件",
                        "attachment_mode": "custom",
                        "attachments": [{"file_path": str(image_path), "file_type": "image"}],
                    },
                ],
            }

            json_path = temp_path / "task.json"
            json_task_io.dump_json_task_file(json_path, payload, create_backup=False)

            loaded = json_task_io.load_json_task_file(json_path)
            self.assertEqual(loaded["total_count"], 2)
            self.assertEqual(loaded["targets"][1]["attachments"][0]["file_type"], "image")

            pending_targets, skipped_success = json_task_io.filter_pending_targets(loaded)
            self.assertEqual(skipped_success, 1)
            self.assertEqual(len(pending_targets), 1)

            updated_target = json_task_io.update_json_target_status(
                json_path,
                target_index=1,
                send_status=json_task_io.SEND_STATUS_SUCCESS,
                attachment_status=json_task_io.ATTACHMENT_STATUS_SUCCESS,
                send_time="2026-04-07 20:01:00",
            )
            self.assertEqual(updated_target["send_status"], "success")
            self.assertEqual(updated_target["attachment_status"], "success")

            finished = json_task_io.update_json_task_end_time(
                json_path,
                end_time="2026-04-07 20:05:00",
            )
            self.assertEqual(finished["end_time"], "2026-04-07 20:05:00")


if __name__ == "__main__":
    unittest.main()
