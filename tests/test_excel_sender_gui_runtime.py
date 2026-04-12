from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtWidgets import QPushButton

from excel_sender_gui import ExcelSenderGUI
from local_contact_store import LocalContactStore, SOURCE_MODE_JSON, SCHEDULE_STATUS_FAILED


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

    def test_normalize_attachment_items_accepts_generic_file(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            zip_file = tmp / "archive.zip"
            zip_file.write_bytes(b"PK\x03\x04")

            window = self.create_window(tmp)
            try:
                items = window.normalize_attachment_items([{"file_path": str(zip_file), "file_type": "file"}])
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["file_type"], "file")
            finally:
                window.close()

    def test_build_export_json_path_uses_task_date_tree(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                path = Path(window.build_export_json_path(datetime(2026, 4, 7, 9, 5, 0)))
                self.assertEqual(path, tmp / "task" / "202604" / "07" / "09-05.json")
            finally:
                window.close()

    def test_window_minimum_size_is_smaller_than_previous_layout(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                self.assertEqual(window.minimumWidth(), 760)
                self.assertEqual(window.minimumHeight(), 540)
            finally:
                window.close()

    def test_main_tabs_split_send_prepare_and_task_center(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                tab_texts = [window.main_tabs.tabText(index) for index in range(window.main_tabs.count())]
                self.assertEqual(tab_texts, ["数据与模板", "本地库数据", "发送准备", "任务中心"])
                self.assertIs(window.main_tabs.widget(2), window.send_prepare_page)
                self.assertIs(window.main_tabs.widget(3), window.task_center_page)
                headers = [window.schedule_table.horizontalHeaderItem(index).text() for index in range(window.schedule_table.columnCount())]
                self.assertEqual(headers, ["队列ID", "计划时间", "执行状态", "自动调度", "人数", "来源", "内容摘要"])
            finally:
                window.close()

    def test_debug_mode_button_state_persists_after_restart(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                self.assertFalse(window.debug_mode_button.isChecked())
                window.debug_mode_button.click()
                self.assertTrue(window.debug_mode_button.isChecked())
            finally:
                window.close()

            reopened = self.create_window(tmp)
            try:
                self.assertTrue(reopened.debug_mode_button.isChecked())
                self.assertEqual(reopened.debug_mode_button.text(), "调试模式：开")
                self.assertIn("预填消息草稿", reopened.debug_mode_button.toolTip())
            finally:
                reopened.close()

    def test_preview_table_header_font_matches_window_font_size(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                header_font = window.preview_table.horizontalHeader().font()
                self.assertGreaterEqual(header_font.pointSize(), window.font().pointSize())
            finally:
                window.close()

    def test_helper_labels_are_one_point_smaller_than_function_font(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                base_size = window.font().pointSize()
                self.assertEqual(base_size, 12)
                self.assertEqual(window.data_info_label.font().pointSize(), base_size - 1)
                self.assertEqual(window.schedule_status_label.font().pointSize(), base_size - 1)
                self.assertEqual(window.filter_status_label.font().pointSize(), base_size - 1)
                self.assertEqual(window.send_status_label.font().pointSize(), base_size - 1)
            finally:
                window.close()

    def test_compact_ui_shortens_button_labels_after_resize(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.show()
                self.app.processEvents()
                self.assertEqual(window.start_button.text(), "立即开始发送")
                window.resize(1000, 700)
                self.app.processEvents()
                self.assertEqual(window.start_button.text(), "开始发送")
                self.assertEqual(window.preview_button.text(), "刷新")
                self.assertEqual(window.import_json_button.text(), "导入")
                self.assertEqual(window.refresh_schedule_button.text(), "刷新")
            finally:
                window.close()

    def test_import_json_tasks_requires_confirmation(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            pdf = tmp / "notice.pdf"
            pdf.write_bytes(b"%PDF-1.4")
            json_path = tmp / "task.json"
            json_path.write_text(
                json.dumps(
                    {
                        "start_time": "2026-04-07 20:00:00",
                        "end_time": "",
                        "total_count": 1,
                        "template_content": "您好",
                        "common_attachments": [{"file_path": str(pdf), "file_type": "pdf"}],
                        "targets": [
                            {
                                "target_value": "张三",
                                "target_type": "person",
                                "message_mode": "template",
                                "message": "",
                                "attachment_mode": "common",
                                "attachments": [],
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            window = self.create_window(tmp)
            try:
                with mock.patch("excel_sender_gui.QFileDialog.getOpenFileNames", return_value=([str(json_path)], "")), \
                     mock.patch("excel_sender_gui.QMessageBox.question", return_value=QMessageBox.No), \
                     mock.patch("excel_sender_gui.QMessageBox.information"):
                    window.import_json_tasks()
                self.assertEqual(window.local_store.list_scheduled_jobs(limit=10), [])
            finally:
                window.close()

    def test_poll_scheduled_jobs_executes_due_json_jobs_automatically(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            json_path = tmp / "task.json"
            json_path.write_text("{}", encoding="utf-8")

            window = self.create_window(tmp)
            store = window.local_store
            task_id = store.create_task_snapshot(
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
                json_start_time="2000-01-01 00:00:00",
            )
            job_id = store.create_scheduled_job(
                task_id=task_id,
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
                with mock.patch.object(window, "execute_scheduled_job") as execute_mock:
                    window.poll_scheduled_jobs()
                execute_mock.assert_called_once()
                called_job = execute_mock.call_args[0][0]
                self.assertEqual(called_job.job_id, job_id)
            finally:
                window.close()

    def test_start_sending_executes_selected_json_job_manually(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            json_path = tmp / "task.json"
            json_path.write_text("{}", encoding="utf-8")

            window = self.create_window(tmp)
            store = window.local_store
            task_id = store.create_task_snapshot(
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
                json_start_time="2099-01-01 00:00:00",
            )
            job_id = store.create_scheduled_job(
                task_id=task_id,
                scheduled_at="2099-01-01 00:00:00",
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
                window.refresh_scheduled_jobs()
                window.select_scheduled_job(job_id)
                with mock.patch("excel_sender_gui.QMessageBox.question", return_value=QMessageBox.Yes), \
                     mock.patch.object(window, "execute_scheduled_job") as execute_mock:
                    window.start_sending()
                execute_mock.assert_called_once()
                called_job = execute_mock.call_args[0][0]
                self.assertEqual(called_job.job_id, job_id)
            finally:
                window.close()

    def test_start_button_text_matches_current_execution_context(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            json_path = tmp / "task.json"
            json_path.write_text("{}", encoding="utf-8")

            window = self.create_window(tmp)
            store = window.local_store
            task_id = store.create_task_snapshot(
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
                json_start_time="2099-01-01 00:00:00",
            )
            job_id = store.create_scheduled_job(
                task_id=task_id,
                scheduled_at="2099-01-01 00:00:00",
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
                self.assertEqual(window.start_button.text(), "立即开始发送")
                window.scheduled_mode_radio.setChecked(True)
                self.app.processEvents()
                self.assertEqual(window.start_button.text(), "创建普通定时任务")
                window.refresh_scheduled_jobs()
                window.select_scheduled_job(job_id)
                self.app.processEvents()
                self.assertEqual(window.start_button.text(), "立即开始所选任务")
            finally:
                window.close()

    def test_preview_attachment_button_shows_count_and_tooltip(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            pdf = tmp / "a.pdf"
            png = tmp / "b.png"
            pdf.write_bytes(b"%PDF-1.4")
            png.write_bytes(b"\x89PNG\r\n\x1a\n")

            window = self.create_window(tmp)
            try:
                window.common_attachments = [{"file_path": str(pdf), "file_type": "pdf"}]
                window.load_records_into_view(
                    records=[{"微信号": "wxid_1", "显示名称": "张三"}],
                    columns=["微信号", "显示名称"],
                    source_mode="file",
                )

                operation_widget = window.preview_table.cellWidget(0, 3)
                self.assertIsNotNone(operation_widget)
                buttons = operation_widget.findChildren(QPushButton)
                attachment_button = next(button for button in buttons if button.text().startswith("附件"))
                self.assertEqual(attachment_button.text(), "附件")
                self.assertIn("当前使用通用附件", attachment_button.toolTip())

                window.set_row_custom_attachments(
                    0,
                    [
                        {"file_path": str(pdf), "file_type": "pdf"},
                        {"file_path": str(png), "file_type": "image"},
                    ],
                )

                operation_widget = window.preview_table.cellWidget(0, 3)
                buttons = operation_widget.findChildren(QPushButton)
                attachment_button = next(button for button in buttons if button.text().startswith("附件"))
                self.assertEqual(attachment_button.text(), "附件(2)")
                self.assertIn("a.pdf", attachment_button.toolTip())
                self.assertIn("b.png", attachment_button.toolTip())

                window.set_row_custom_attachments(0, [])
                operation_widget = window.preview_table.cellWidget(0, 3)
                buttons = operation_widget.findChildren(QPushButton)
                attachment_button = next(button for button in buttons if button.text().startswith("附件"))
                self.assertEqual(attachment_button.text(), "附件")
                self.assertIn("当前使用通用附件", attachment_button.toolTip())
            finally:
                window.close()

    def test_delete_selected_scheduled_job_only_removes_queue_record(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            store = window.local_store
            task_id = store.create_task_snapshot(
                rows=[{"__target_value": "A", "target_type": "person"}],
                filter_fields="",
                filter_pattern="",
                target_column="target_value",
                template_text="hello",
                source_batch_id=None,
                source_mode="file",
            )
            job_id = store.create_scheduled_job(
                task_id=task_id,
                scheduled_at="2099-01-01 00:00:00",
                interval_seconds=1,
                random_delay_min=0,
                random_delay_max=0,
                operator_name="tester",
                report_to="tester",
                source_mode="file",
                dataset_type="all",
                template_preview="hello",
                total_count=1,
            )
            try:
                window.refresh_scheduled_jobs()
                window.select_scheduled_job(job_id)
                with mock.patch("excel_sender_gui.QMessageBox.question", return_value=QMessageBox.Yes):
                    window.delete_selected_scheduled_job()
                self.assertEqual(store.list_scheduled_jobs(limit=10), [])
                self.assertIsNotNone(store.get_task_details(task_id))
            finally:
                window.close()

    def test_continue_button_enabled_for_failed_job_with_remaining_records(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            json_path = tmp / "task.json"
            json_path.write_text("{}", encoding="utf-8")

            window = self.create_window(tmp)
            store = window.local_store
            task_id = store.create_task_snapshot(
                rows=[
                    {"__target_value": "A", "target_type": "person", "send_status": "success"},
                    {"__target_value": "B", "target_type": "person", "send_status": "failed"},
                    {"__target_value": "C", "target_type": "person", "send_status": ""},
                ],
                filter_fields="",
                filter_pattern="",
                target_column="target_value",
                template_text="hello",
                source_batch_id=None,
                source_mode=SOURCE_MODE_JSON,
                task_kind="json",
                source_json_path=str(json_path),
                source_json_name=json_path.name,
                json_start_time="2099-01-01 00:00:00",
            )
            job_id = store.create_scheduled_job(
                task_id=task_id,
                scheduled_at="2099-01-01 00:00:00",
                interval_seconds=1,
                random_delay_min=0,
                random_delay_max=0,
                operator_name="tester",
                report_to="tester",
                source_mode=SOURCE_MODE_JSON,
                dataset_type="all",
                template_preview="hello",
                total_count=3,
                task_kind="json",
                source_json_path=str(json_path),
                source_json_name=json_path.name,
            )
            store.complete_scheduled_job(job_id, status=SCHEDULE_STATUS_FAILED, result={"failed": 1}, last_error="boom")
            try:
                window.refresh_scheduled_jobs()
                window.select_scheduled_job(job_id)
                self.app.processEvents()
                self.assertTrue(window.continue_button.isEnabled())
            finally:
                window.close()

    def test_preview_selected_scheduled_job_opens_dialog(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            store = window.local_store
            task_id = store.create_task_snapshot(
                rows=[{"__target_value": "A", "target_type": "person"}],
                filter_fields="",
                filter_pattern="",
                target_column="target_value",
                template_text="hello",
                source_batch_id=None,
                source_mode="file",
            )
            job_id = store.create_scheduled_job(
                task_id=task_id,
                scheduled_at="2099-01-01 00:00:00",
                interval_seconds=1,
                random_delay_min=0,
                random_delay_max=0,
                operator_name="tester",
                report_to="tester",
                source_mode="file",
                dataset_type="all",
                template_preview="hello",
                total_count=1,
            )
            captured = {}

            def _fake_exec(dialog_self):
                tables = dialog_self.findChildren(type(window.preview_table))
                captured["row_count"] = tables[0].rowCount() if tables else 0
                return 0

            try:
                window.refresh_scheduled_jobs()
                window.select_scheduled_job(job_id)
                with mock.patch("excel_sender_gui.QDialog.exec_", new=_fake_exec):
                    window.preview_selected_scheduled_job()
                self.assertEqual(captured.get("row_count"), 1)
            finally:
                window.close()

    def test_poll_scheduled_jobs_marks_all_due_jobs_waiting(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            store = window.local_store
            task_one = store.create_task_snapshot(
                rows=[{"__target_value": "A", "target_type": "person"}],
                filter_fields="",
                filter_pattern="",
                target_column="target_value",
                template_text="hello",
                source_batch_id=None,
                source_mode="file",
            )
            task_two = store.create_task_snapshot(
                rows=[{"__target_value": "B", "target_type": "person"}],
                filter_fields="",
                filter_pattern="",
                target_column="target_value",
                template_text="hello",
                source_batch_id=None,
                source_mode="file",
            )
            job_one = store.create_scheduled_job(
                task_id=task_one,
                scheduled_at="2000-01-01 00:00:00",
                interval_seconds=1,
                random_delay_min=0,
                random_delay_max=0,
                operator_name="tester",
                report_to="tester",
                source_mode="file",
                dataset_type="all",
                template_preview="hello",
                total_count=1,
            )
            job_two = store.create_scheduled_job(
                task_id=task_two,
                scheduled_at="2000-01-01 00:00:00",
                interval_seconds=1,
                random_delay_min=0,
                random_delay_max=0,
                operator_name="tester",
                report_to="tester",
                source_mode="file",
                dataset_type="all",
                template_preview="hello",
                total_count=1,
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

    def test_disabled_job_is_not_auto_polled_but_can_start_manually(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            json_path = tmp / "task.json"
            json_path.write_text("{}", encoding="utf-8")

            window = self.create_window(tmp)
            store = window.local_store
            task_id = store.create_task_snapshot(
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
                json_start_time="2000-01-01 00:00:00",
            )
            job_id = store.create_scheduled_job(
                task_id=task_id,
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
                enabled=False,
            )

            try:
                with mock.patch.object(window, "execute_scheduled_job") as execute_mock:
                    window.poll_scheduled_jobs()
                execute_mock.assert_not_called()

                window.refresh_scheduled_jobs()
                window.select_scheduled_job(job_id)
                self.app.processEvents()
                self.assertEqual(window.schedule_table.item(0, 3).text(), "关闭")
                with mock.patch("excel_sender_gui.QMessageBox.question", return_value=QMessageBox.Yes), \
                     mock.patch.object(window, "execute_scheduled_job") as execute_mock:
                    window.start_sending()
                execute_mock.assert_called_once()
                called_job = execute_mock.call_args[0][0]
                self.assertEqual(called_job.job_id, job_id)
            finally:
                window.close()

    def test_toggle_auto_schedule_buttons_update_job_enabled_state(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            store = window.local_store
            task_id = store.create_task_snapshot(
                rows=[{"__target_value": "A", "target_type": "person"}],
                filter_fields="",
                filter_pattern="",
                target_column="target_value",
                template_text="hello",
                source_batch_id=None,
                source_mode="file",
            )
            job_id = store.create_scheduled_job(
                task_id=task_id,
                scheduled_at="2099-01-01 00:00:00",
                interval_seconds=1,
                random_delay_min=0,
                random_delay_max=0,
                operator_name="tester",
                report_to="tester",
                source_mode="file",
                dataset_type="all",
                template_preview="hello",
                total_count=1,
            )

            try:
                window.refresh_scheduled_jobs()
                window.select_scheduled_job(job_id)
                self.app.processEvents()
                self.assertTrue(window.disable_schedule_button.isEnabled())
                self.assertFalse(window.enable_schedule_button.isEnabled())

                window.disable_selected_scheduled_job()
                refreshed_job = store.list_scheduled_jobs(limit=10)[0]
                self.assertEqual(refreshed_job.enabled, 0)

                window.refresh_scheduled_jobs()
                window.select_scheduled_job(job_id)
                self.app.processEvents()
                self.assertTrue(window.enable_schedule_button.isEnabled())
                self.assertFalse(window.disable_schedule_button.isEnabled())

                window.enable_selected_scheduled_job()
                refreshed_job = store.list_scheduled_jobs(limit=10)[0]
                self.assertEqual(refreshed_job.enabled, 1)
            finally:
                window.close()


if __name__ == "__main__":
    unittest.main()
