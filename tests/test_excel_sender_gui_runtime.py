from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from PyQt5.QtCore import QItemSelectionModel, Qt
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QHeaderView
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

    def test_window_minimum_size_targets_compact_redesign_goal(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                self.assertEqual(window.minimumWidth(), 1080)
                self.assertEqual(window.minimumHeight(), 760)
            finally:
                window.close()

    def test_window_starts_with_enlarged_initial_size(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                self.assertEqual(window.width(), 1638)
                self.assertEqual(window.height(), 1092)
            finally:
                window.close()

    def test_navigation_shell_groups_pages_into_workbench_and_task_views(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                tab_texts = [window.main_tabs.tabText(index) for index in range(window.main_tabs.count())]
                self.assertEqual(tab_texts, ["工作台", "数据与模板", "本地库数据", "任务工作区"])
                self.assertFalse(window.main_tabs.tabBar().isVisible())
                self.assertIs(window.main_tabs.widget(0), window.workbench_page)
                self.assertIs(window.workbench_stack.currentWidget(), window.basic_page)
                self.assertEqual([button.text() for button in window.navigation_buttons.values()], ["工作台", "数据与模板", "本地库数据", "任务工作区"])
                headers = [window.schedule_table.horizontalHeaderItem(index).text() for index in range(window.schedule_table.columnCount())]
                self.assertEqual(headers, ["队列ID", "计划时间", "执行状态", "自动调度", "人数", "来源", "内容摘要"])
            finally:
                window.close()

    def test_old_navigation_entrypoints_route_to_new_shell_sections(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.open_send_prepare_page()
                self.assertIs(window.main_tabs.currentWidget(), window.workbench_page)
                self.assertIs(window.workbench_stack.currentWidget(), window.send_prepare_page)
                window.open_task_center_page()
                self.assertIs(window.main_tabs.currentWidget(), window.task_center_page)
                window.open_local_store_page()
                self.assertIs(window.main_tabs.currentWidget(), window.local_store_page)
            finally:
                window.close()

    def test_data_template_page_keeps_horizontal_layout_and_uses_compact_empty_states(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.main_tabs.setCurrentWidget(window.data_template_page)
                window.show()
                self.app.processEvents()
                self.assertEqual(window.data_template_splitter.orientation(), Qt.Horizontal)
                self.assertTrue(window.columns_empty_label.isVisible())
                self.assertFalse(window.columns_view.isVisible())
                self.assertTrue(window.common_attachment_empty_label.isVisible())
                self.assertFalse(window.common_attachment_table.isVisible())
            finally:
                window.close()

    def test_advanced_settings_panel_collapses_and_persists(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.open_send_prepare_page()
                self.assertTrue(window.advanced_settings_panel.isHidden())
                window.advanced_settings_toggle_button.click()
                self.assertFalse(window.advanced_settings_panel.isHidden())
                window.open_task_center_page()
            finally:
                window.close()

            reopened = self.create_window(tmp)
            try:
                self.assertIs(reopened.main_tabs.currentWidget(), reopened.task_center_page)
                self.assertTrue(reopened.advanced_settings_toggle_button.isChecked())
                reopened.open_send_prepare_page()
                self.assertFalse(reopened.advanced_settings_panel.isHidden())
            finally:
                reopened.close()

    def test_theme_mode_persists_after_restart(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                parent = window.theme_mode_combo.parentWidget()
                in_navigation_panel = False
                while parent is not None:
                    if parent is window.navigation_panel:
                        in_navigation_panel = True
                        break
                    parent = parent.parentWidget()
                self.assertTrue(in_navigation_panel)
                self.assertFalse(hasattr(window, "window_subtitle_label"))
                self.assertIsNone(window.findChild(type(window.theme_status_label), "windowTitle"))
                self.assertEqual(window.theme_mode_combo.currentData(), "auto")
                window.theme_mode_combo.setCurrentIndex(window.theme_mode_combo.findData("dark"))
                self.assertEqual(window.theme_mode_combo.currentData(), "dark")
            finally:
                window.close()

            reopened = self.create_window(tmp)
            try:
                self.assertEqual(reopened.theme_mode_combo.currentData(), "dark")
                self.assertIn("当前主题：", reopened.theme_status_label.text())
            finally:
                reopened.close()

    def test_basic_mode_loads_columns_and_inserts_variable_token(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            excel_path = tmp / "contacts.csv"
            excel_path.write_text("微信号,姓名,城市\nwx_1,张三,上海\nwx_2,李四,北京\n", encoding="utf-8")
            window = self.create_window(tmp)
            try:
                window.basic_excel_path_input.setText(str(excel_path))
                with mock.patch("excel_sender_gui.QMessageBox.information"):
                    self.assertTrue(window.load_basic_excel_data())
                self.assertEqual(window.basic_variable_combo.count(), 3)
                self.assertEqual(window.basic_match_field_combo.count(), 3)
                self.assertEqual(window.basic_match_field_combo.currentData(), "微信号")
                self.assertTrue(window.basic_insert_variable_button.isEnabled())
                window.basic_variable_combo.setCurrentIndex(window.basic_variable_combo.findData("姓名"))
                window.insert_basic_variable()
                self.assertIn("{{姓名}}", window.basic_message_input.toPlainText())
            finally:
                window.close()

    def test_data_template_page_shows_columns_and_attachment_list_when_content_exists(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            excel_path = tmp / "contacts.csv"
            excel_path.write_text("微信号,姓名,城市\nwx_1,张三,上海\nwx_2,李四,北京\n", encoding="utf-8")
            pdf = tmp / "notice.pdf"
            pdf.write_bytes(b"%PDF-1.4")
            window = self.create_window(tmp)
            try:
                window.main_tabs.setCurrentWidget(window.data_template_page)
                window.show()
                self.app.processEvents()
                window.excel_path_input.setText(str(excel_path))
                with mock.patch("excel_sender_gui.QMessageBox.information"):
                    self.assertTrue(window.load_excel_data())
                self.app.processEvents()
                self.assertFalse(window.columns_empty_label.isVisible())
                self.assertTrue(window.columns_view.isVisible())
                self.assertIn("微信号", window.columns_view.toPlainText())

                window.common_attachments = [{"file_path": str(pdf), "file_type": "pdf"}]
                window.refresh_common_attachment_table()
                self.app.processEvents()
                self.assertFalse(window.common_attachment_empty_label.isVisible())
                self.assertTrue(window.common_attachment_table.isVisible())
                self.assertIn("已添加 1 个通用附件", window.common_attachment_status_label.text())
            finally:
                window.close()

    def test_basic_mode_match_candidates_use_selected_field_but_keep_wechat_target(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            excel_path = tmp / "contacts.csv"
            excel_path.write_text(
                "微信号,姓名,备注\nwx_1,张三,班主任A\nwx_2,李四,班主任B\n",
                encoding="utf-8",
            )
            window = self.create_window(tmp)
            try:
                window.basic_excel_path_input.setText(str(excel_path))
                with mock.patch("excel_sender_gui.QMessageBox.information"):
                    self.assertTrue(window.load_basic_excel_data())

                window.basic_match_field_combo.setCurrentIndex(window.basic_match_field_combo.findData("姓名"))
                window.basic_match_keyword_input.setText("张")
                matched_rows, raw_total, duplicate_removed = window.build_basic_match_candidates()

                self.assertEqual(raw_total, 1)
                self.assertEqual(duplicate_removed, 0)
                self.assertEqual(len(matched_rows), 1)
                self.assertEqual(matched_rows[0]["姓名"], "张三")
                self.assertEqual(matched_rows[0]["_search_key"], "张三")
                self.assertEqual(matched_rows[0]["__target_value"], "wx_1")

                window.basic_selected_records = [dict(row) for row in matched_rows]
                window.basic_message_input.setPlainText("您好 {{姓名}}")
                task_id = window.create_basic_task_snapshot()
                task_details = window.local_store.get_task_details(task_id)
                assert task_details is not None
                self.assertEqual(task_details["filter_fields"], "姓名")
                self.assertEqual(task_details["target_column"], "微信号")
            finally:
                window.close()

    def test_basic_mode_match_field_falls_back_to_wechat_id_when_saved_field_missing(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            config_path = tmp / "excel-sender-test-config.json"
            config_path.write_text(
                json.dumps({"basic_mode": {"match_field": "客户编号"}}, ensure_ascii=False),
                encoding="utf-8",
            )
            excel_path = tmp / "contacts.csv"
            excel_path.write_text("微信号,姓名\nwx_1,张三\nwx_2,李四\n", encoding="utf-8")

            window = self.create_window(tmp)
            try:
                window.basic_excel_path_input.setText(str(excel_path))
                with mock.patch("excel_sender_gui.QMessageBox.information"):
                    self.assertTrue(window.load_basic_excel_data())
                self.assertEqual(window.basic_match_field_combo.currentData(), "微信号")
                self.assertIn("已自动回退到“微信号”", window.basic_column_status_label.text())
                self.assertIn("已自动回退到“微信号”", window.basic_match_field_status_label.text())
            finally:
                window.close()

    def test_basic_mode_batches_remaining_targets_without_restarting_from_head(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            store = window.local_store
            try:
                window.basic_selected_records = [
                    {"微信号": "wx_1", "显示名称": "张三", "__target_value": "wx_1"},
                    {"微信号": "wx_2", "显示名称": "李四", "__target_value": "wx_2"},
                    {"微信号": "wx_3", "显示名称": "王五", "__target_value": "wx_3"},
                ]
                window.basic_message_input.setPlainText("您好")
                window.basic_batch_limit_spin.setValue(2)
                task_id = window.ensure_basic_task_snapshot()
                records = window.get_basic_pending_records(task_id)
                self.assertEqual([row["__target_value"] for row in records], ["wx_1", "wx_2", "wx_3"])

                task_rows = store.load_task_records(task_id)
                for row in task_rows[:2]:
                    store.update_task_item_result(
                        int(row["__task_item_id"]),
                        send_status="success",
                        send_time="2026-04-16 12:00:00",
                        error_msg="",
                        attachment_status="none",
                        attachments=None,
                        attachment_details=[],
                        raw_updates={"send_status": "success"},
                    )

                remaining = window.get_basic_pending_records(task_id)
                self.assertEqual([row["__target_value"] for row in remaining], ["wx_3"])
                window.update_basic_progress_status()
                self.assertIn("剩余 1 人", window.basic_progress_label.text())
            finally:
                window.close()

    def test_basic_mode_sections_support_manual_collapse(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                for section_key in ("import", "receiver", "message", "attachment", "send"):
                    self.assertIn(section_key, window.basic_section_toggle_buttons)
                    self.assertIn(section_key, window.basic_section_content_widgets)
                    self.assertFalse(window.basic_section_content_widgets[section_key].isHidden())
                window.basic_section_toggle_buttons["attachment"].click()
                self.assertTrue(window.basic_section_content_widgets["attachment"].isHidden())
                self.assertEqual(window.basic_section_toggle_buttons["attachment"].text(), "展开")
                window.basic_section_toggle_buttons["attachment"].click()
                self.assertFalse(window.basic_section_content_widgets["attachment"].isHidden())
                self.assertEqual(window.basic_section_toggle_buttons["attachment"].text(), "收起")
            finally:
                window.close()

    def test_basic_mode_sections_use_inline_header_titles_with_aligned_toggles(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.show()
                self.app.processEvents()
                expected_titles = {
                    "import": "1. 导入数据",
                    "receiver": "2. 选择微信接收人",
                    "message": "3. 消息内容",
                    "attachment": "4. 附件（可多个）",
                    "send": "5. 发送设置",
                }
                for section_key, title_text in expected_titles.items():
                    self.assertEqual(window.basic_section_groups[section_key].title(), "")
                    title_label = window.basic_section_title_labels[section_key]
                    toggle_button = window.basic_section_toggle_buttons[section_key]
                    self.assertEqual(title_label.text(), title_text)
                    self.assertLessEqual(abs(title_label.geometry().center().y() - toggle_button.geometry().center().y()), 10)
            finally:
                window.close()

    def test_basic_receiver_shows_empty_state_prompt_before_selection(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                self.assertIs(window.basic_selected_table_stack.currentWidget(), window.basic_selected_empty_label)
                self.assertIn("暂无接收人", window.basic_selected_empty_label.text())
                self.assertEqual(window.basic_selected_summary_label.text(), "未选择接收人｜去重 0 人")
            finally:
                window.close()

    def test_basic_receiver_summary_compacts_selection_and_dedup_counts(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.basic_last_duplicate_removed = 2
                window.basic_selected_records = [
                    {"微信号": "wx_1", "显示名称": "张三", "__target_value": "wx_1"},
                    {"微信号": "wx_2", "显示名称": "李四", "__target_value": "wx_2"},
                ]
                window.refresh_basic_selected_table()
                headers = [
                    window.basic_selected_table.horizontalHeaderItem(index).text()
                    for index in range(window.basic_selected_table.columnCount())
                ]
                self.assertEqual(headers, ["微信号", "显示名称", "状态"])
                self.assertIs(window.basic_selected_table_stack.currentWidget(), window.basic_selected_table)
                self.assertEqual(window.basic_selected_summary_label.text(), "已确认 2 人｜去重 2 人")
            finally:
                window.close()

    def test_basic_receiver_table_columns_are_all_resizable(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                header = window.basic_selected_table.horizontalHeader()
                modes = [header.sectionResizeMode(index) for index in range(window.basic_selected_table.columnCount())]
                self.assertEqual(
                    modes,
                    [QHeaderView.Interactive, QHeaderView.Interactive, QHeaderView.Interactive],
                )
                self.assertGreaterEqual(window.basic_selected_table.columnWidth(0), 150)
                self.assertGreaterEqual(window.basic_selected_table.columnWidth(1), 300)
                self.assertGreaterEqual(window.basic_selected_table.columnWidth(2), 100)
            finally:
                window.close()

    def test_basic_receiver_table_preserves_manual_column_width_after_refresh(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.show()
                self.app.processEvents()
                window.basic_selected_records = [
                    {"微信号": "wx_1", "显示名称": "张三", "__target_value": "wx_1"},
                    {"微信号": "wx_2", "显示名称": "李四", "__target_value": "wx_2"},
                ]
                window.refresh_basic_selected_table()
                self.app.processEvents()

                window.basic_selected_table.setColumnWidth(0, 230)
                window.basic_selected_table.setColumnWidth(1, 360)
                window.basic_selected_table.setColumnWidth(2, 150)
                self.app.processEvents()

                self.assertEqual(window.basic_selected_table.columnWidth(0), 230)
                self.assertEqual(window.basic_selected_table.columnWidth(1), 360)
                self.assertEqual(window.basic_selected_table.columnWidth(2), 150)

                window.refresh_basic_selected_table()
                self.app.processEvents()

                self.assertEqual(window.basic_selected_table.columnWidth(0), 230)
                self.assertEqual(window.basic_selected_table.columnWidth(1), 360)
                self.assertEqual(window.basic_selected_table.columnWidth(2), 150)
            finally:
                window.close()

    def test_basic_mode_collapse_releases_space_to_visible_sections(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.show()
                window.resize(1280, 860)
                self.app.processEvents()

                receiver_height_before = window.basic_receiver_group.height()
                attachment_y_before = window.basic_attachment_group.y()
                send_y_before = window.basic_send_group.y()
                right_panel_height_before = window.basic_right_layout.parentWidget().height()

                window.basic_section_toggle_buttons["receiver"].click()
                self.app.processEvents()

                self.assertTrue(window.basic_section_content_widgets["receiver"].isHidden())
                self.assertLess(window.basic_receiver_group.height(), receiver_height_before)
                self.assertLess(window.basic_attachment_group.y(), attachment_y_before)
                self.assertLess(window.basic_send_group.y(), send_y_before)
                self.assertLess(window.basic_right_layout.parentWidget().height(), right_panel_height_before)
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

    def test_stop_on_error_checkbox_state_persists_after_restart(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                self.assertTrue(window.stop_on_error_checkbox.isChecked())
                window.stop_on_error_checkbox.click()
                self.assertFalse(window.stop_on_error_checkbox.isChecked())
            finally:
                window.close()

            reopened = self.create_window(tmp)
            try:
                self.assertFalse(reopened.stop_on_error_checkbox.isChecked())
            finally:
                reopened.close()

    def test_handle_target_log_writes_stage_entries_to_runtime_log(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            log_path = tmp / "runtime.easychat.log"
            window = self.create_window(tmp)
            try:
                window.current_runtime_log_path = str(log_path)
                window.handle_target_log(
                    "[1/2] 张三 -> 发送文本消息",
                    {
                        "log_type": "stage",
                        "stage": "发送文本消息",
                        "target_value": "张三",
                        "index": 1,
                        "total": 2,
                        "detail": "",
                        "timestamp": "2026-04-12 21:00:00",
                    },
                )
                window.handle_target_log("发送线程启动：共 2 个目标。", {})
                content = log_path.read_text(encoding="utf-8")
                self.assertIn("stage=发送文本消息", content)
                self.assertIn("target=张三", content)
                self.assertIn("stage=发送线程启动：共 2 个目标。", content)
            finally:
                window.close()

    def test_preview_table_header_font_matches_window_font_size(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.open_send_prepare_page()
                window.show()
                self.app.processEvents()
                window.apply_table_header_font(window.preview_table)
                self.app.processEvents()
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
                self.assertEqual(base_size, 11)
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

    def test_delete_selected_scheduled_job_supports_batch_rows(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            store = window.local_store
            task_ids = [
                store.create_task_snapshot(
                    rows=[{"__target_value": f"A{index}", "target_type": "person"}],
                    filter_fields="",
                    filter_pattern="",
                    target_column="target_value",
                    template_text="hello",
                    source_batch_id=None,
                    source_mode="file",
                )
                for index in range(3)
            ]
            job_ids = [
                store.create_scheduled_job(
                    task_id=task_id,
                    scheduled_at=f"2099-01-01 00:00:0{offset}",
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
                for offset, task_id in enumerate(task_ids, start=1)
            ]
            try:
                window.refresh_scheduled_jobs()
                selection_model = window.schedule_table.selectionModel()
                for row_index in (0, 1):
                    model_index = window.schedule_table.model().index(row_index, 0)
                    selection_model.select(
                        model_index,
                        QItemSelectionModel.Select | QItemSelectionModel.Rows,
                    )
                with mock.patch("excel_sender_gui.QMessageBox.question", return_value=QMessageBox.Yes):
                    window.delete_selected_scheduled_job()
                remaining_job_ids = [job.job_id for job in store.list_scheduled_jobs(limit=10)]
                self.assertEqual(remaining_job_ids, [job_ids[2]])
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
