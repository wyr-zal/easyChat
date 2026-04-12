from __future__ import annotations

import json
import tempfile
import unittest
import gc
from pathlib import Path

from local_contact_store import (
    CONFLICT_STATUS_WAITING,
    DEFAULT_LOCAL_DB_PATH,
    LocalContactStore,
    SCHEDULE_MODE_CRON,
    SOURCE_MODE_JSON,
    TASK_KIND_JSON,
)


class LocalContactStoreJsonTests(unittest.TestCase):
    def test_create_json_task_and_track_runtime_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_path = Path(tmp_dir)
            db_path = temp_path / DEFAULT_LOCAL_DB_PATH.name
            pdf_path = temp_path / "notice.pdf"
            png_path = temp_path / "notice.png"
            pdf_path.write_bytes(b"%PDF-1.4")
            png_path.write_bytes(b"\x89PNG\r\n\x1a\n")

            payload = {
                "start_time": "2026-04-07 20:00:00",
                "end_time": "",
                "schedule_mode": "cron",
                "schedule_value": "0 9 * * 1-5",
                "total_count": 3,
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
                        "attachments": [{"file_path": str(png_path), "file_type": "image"}],
                    },
                    {
                        "target_value": "李四",
                        "target_type": "person",
                        "message_mode": "template",
                        "message": "",
                        "attachment_mode": "common",
                        "attachments": [],
                    },
                ],
            }
            json_path = temp_path / "task.json"
            json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            store = LocalContactStore(db_path)
            task_id, job_id = store.create_json_task_from_payload(
                source_json_path=json_path,
                payload=payload,
                interval_seconds=5,
                random_delay_min=0,
                random_delay_max=0,
                operator_name="tester",
                report_to="reporter",
                source_mode=SOURCE_MODE_JSON,
            )

            task = store.get_task_details(task_id)
            self.assertIsNotNone(task)
            assert task is not None
            self.assertEqual(task["task_kind"], TASK_KIND_JSON)
            self.assertEqual(task["source_json_path"], str(json_path))

            records = store.load_task_records(task_id)
            self.assertEqual(len(records), 2)
            self.assertEqual({record["source_json_index"] for record in records}, {2, 3})

            first_task_item_id = int(records[0]["__task_item_id"])
            updated_record = store.update_task_item_result(
                first_task_item_id,
                send_status="success",
                send_time="2026-04-07 20:01:00",
                attachment_status="success",
                error_msg="",
                attachment_details=[{"file_path": str(png_path), "attachment_status": "success"}],
            )
            self.assertEqual(updated_record["send_status"], "success")

            store.append_send_event(
                task_id=task_id,
                task_item_id=first_task_item_id,
                scheduled_job_id=job_id,
                target_value="高三1班",
                target_type="group",
                message_mode="custom",
                send_status="success",
                send_time="2026-04-07 20:01:00",
                attachment_status="success",
                source_json_path=str(json_path),
                log_path=str(temp_path / "task.log"),
                event_data={"ok": True},
            )
            events = store.list_task_events(task_id)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].target_value, "高三1班")

            store.mark_job_waiting_conflict(job_id, "等待前序任务完成", notify=True)
            job = store.list_json_jobs(limit=10)[0]
            self.assertEqual(job.conflict_status, CONFLICT_STATUS_WAITING)
            self.assertEqual(job.source_json_path, str(json_path))
            self.assertEqual(job.schedule_mode, SCHEDULE_MODE_CRON)
            self.assertEqual(job.schedule_value, "0 9 * * 1-5")
            self.assertEqual(job.enabled, 1)

            self.assertTrue(store.delete_scheduled_job(job_id))
            self.assertEqual(store.list_json_jobs(limit=10), [])
            self.assertIsNotNone(store.get_task_details(task_id))

            del store
            gc.collect()

    def test_disable_scheduled_job_excludes_it_from_due_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_path = Path(tmp_dir)
            db_path = temp_path / DEFAULT_LOCAL_DB_PATH.name
            store = LocalContactStore(db_path)

            task_id = store.create_task_snapshot(
                rows=[{"__target_value": "张三", "target_type": "person"}],
                filter_fields="",
                filter_pattern="",
                target_column="target_value",
                template_text="您好",
                source_batch_id=None,
                source_mode=SOURCE_MODE_JSON,
                task_kind=TASK_KIND_JSON,
            )
            job_id = store.create_scheduled_job(
                task_id=task_id,
                scheduled_at="2000-01-01 00:00:00",
                interval_seconds=5,
                random_delay_min=0,
                random_delay_max=0,
                operator_name="tester",
                report_to="reporter",
                source_mode=SOURCE_MODE_JSON,
                dataset_type="all",
                template_preview="您好",
                total_count=1,
                task_kind=TASK_KIND_JSON,
            )

            due_jobs = store.get_due_scheduled_jobs("2099-01-01 00:00:00", limit=10)
            self.assertEqual([job.job_id for job in due_jobs], [job_id])

            self.assertTrue(store.set_scheduled_job_enabled(job_id, False))
            refreshed_job = store.list_scheduled_jobs(limit=10)[0]
            self.assertEqual(refreshed_job.enabled, 0)
            self.assertEqual(store.get_due_scheduled_jobs("2099-01-01 00:00:00", limit=10), [])

            self.assertTrue(store.set_scheduled_job_enabled(job_id, True))
            due_jobs = store.get_due_scheduled_jobs("2099-01-01 00:00:00", limit=10)
            self.assertEqual([job.job_id for job in due_jobs], [job_id])

            del store
            gc.collect()


if __name__ == "__main__":
    unittest.main()
