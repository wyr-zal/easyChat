from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from PyQt5.QtWidgets import QApplication

from excel_sender_gui import ExcelSenderGUI
from local_contact_store import LocalContactStore, SOURCE_MODE_JSON


class _RunningThread:
    def isRunning(self) -> bool:
        return True


class ExcelSenderGuiRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = QApplication.instance() or QApplication([])

    def create_window(self, tmp: Path, db_name: str = "gui-runtime.sqlite3") -> ExcelSenderGUI:
        return ExcelSenderGUI(
            config_path=str(tmp / "excel-sender-test-config.json"),
            db_path=str(tmp / db_name),
            start_scheduler=False,
        )

    def test_normalize_attachment_items_accepts_json_text(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            pdf = tmp / "notice.pdf"
            pdf.write_bytes(b"%PDF-1.4")

            window = self.create_window(tmp)
            try:
                items = window.normalize_attachment_items(
                    json.dumps([{"file_path": str(pdf), "file_type": "pdf"}], ensure_ascii=False)
                )
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["file_type"], "pdf")
            finally:
                window.close()

    def test_poll_scheduled_jobs_marks_all_due_jobs_waiting(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            json_path = tmp / "task.json"
            json_path.write_text("{}", encoding="utf-8")

            window = self.create_window(tmp)
            store = window.local_store
            task_one = store.create_task_snapshot(
                rows=[{"__target_value": "A", "target_type": "person"}],
                filter_fields="",
                filter_pattern="",
                target_column="target_value",
                template_text="hello",
                source_batch_id=None,
                source_mode=SOURCE_MODE_JSON,
                task_kind="json",
                source_json_path=str(json_path),
                source_json_name=json_path.name,
                json_start_time="2026-04-07 20:00:00",
            )
            task_two = store.create_task_snapshot(
                rows=[{"__target_value": "B", "target_type": "person"}],
                filter_fields="",
                filter_pattern="",
                target_column="target_value",
                template_text="hello",
                source_batch_id=None,
                source_mode=SOURCE_MODE_JSON,
                task_kind="json",
                source_json_path=str(json_path),
                source_json_name=json_path.name,
                json_start_time="2026-04-07 20:00:00",
            )
            job_one = store.create_scheduled_job(
                task_id=task_one,
                scheduled_at="2000-01-01 00:00:00",
                interval_seconds=1,
                random_delay_min=0,
                random_delay_max=0,
                operator_name="tester",
                report_to="tester",
                source_mode=SOURCE_MODE_JSON,
                dataset_type="all",
                template_preview="hello",
                total_count=1,
                task_kind="json",
                source_json_path=str(json_path),
                source_json_name=json_path.name,
            )
            job_two = store.create_scheduled_job(
                task_id=task_two,
                scheduled_at="2000-01-01 00:00:00",
                interval_seconds=1,
                random_delay_min=0,
                random_delay_max=0,
                operator_name="tester",
                report_to="tester",
                source_mode=SOURCE_MODE_JSON,
                dataset_type="all",
                template_preview="hello",
                total_count=1,
                task_kind="json",
                source_json_path=str(json_path),
                source_json_name=json_path.name,
            )

            try:
                window.send_thread = _RunningThread()
                window.poll_scheduled_jobs()
                jobs = {job.job_id: job for job in store.list_scheduled_jobs(limit=10)}
                self.assertEqual(jobs[job_one].conflict_status, "waiting")
                self.assertEqual(jobs[job_two].conflict_status, "waiting")
            finally:
                window.send_thread = None
                window.close()


if __name__ == "__main__":
    unittest.main()
