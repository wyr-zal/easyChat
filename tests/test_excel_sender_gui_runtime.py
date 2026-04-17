from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt5.QtCore import QItemSelectionModel, Qt
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QFrame
from PyQt5.QtWidgets import QHeaderView
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QSizePolicy
from PyQt5.QtWidgets import QSplitter

from excel_sender_gui import (
    CURRENT_SPLITTER_LAYOUT_VERSION,
    PAGE_KEY_DATA_TEMPLATE,
    PAGE_KEY_LOCAL_STORE,
    PAGE_KEY_TASK_CENTER,
    PAGE_KEY_WORKBENCH,
    SPLITTER_STARTUP_DEFAULT_SIZES,
    WORKBENCH_VIEW_BASIC,
    WORKBENCH_VIEW_SEND,
    ExcelSenderGUI,
)
from local_contact_store import LocalContactStore, SOURCE_MODE_JSON, SCHEDULE_STATUS_FAILED

EXPECTED_STARTUP_SPLITTER_SIZES = {
    "workbench.basic.left": [250, 761],
    "workbench.basic.right": [432, 305, 270],
    "workbench.basic.main": [710, 710],
    "data_template.excel": [346, 303, 389],
    "data_template.template": [245, 246, 547],
    "data_template.main": [787, 633],
    "local_store.friend": [126, 775],
    "local_store.group": [126, 775],
    "local_store.dataset_shell": [83, 957],
    "local_store.main": [910, 510],
    "workbench.send.left": [286, 725],
    "workbench.send.main": [844, 576],
    "task_center.schedule": [111, 794],
    "task_center.main": [1116, 304],
}
LEGACY_STARTUP_SPLITTER_SIZES = {
    "workbench.basic.left": [220, 791],
    "workbench.basic.right": [465, 279, 263],
    "workbench.basic.main": [631, 789],
    "data_template.excel": [346, 299, 393],
    "data_template.template": [471, 152, 415],
    "data_template.main": [637, 783],
    "local_store.friend": [153, 702],
    "local_store.group": [153, 702],
    "local_store.dataset_shell": [129, 911],
    "local_store.main": [857, 563],
    "workbench.send.left": [327, 684],
    "workbench.send.main": [897, 523],
    "task_center.schedule": [187, 718],
    "task_center.main": [815, 605],
}


class _RunningThread:
    def isRunning(self) -> bool:
        return True


class ExcelSenderGuiRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = QApplication.instance() or QApplication([])

    def create_window(self, tmp: Path, db_name: str = "gui-runtime.sqlite3") -> ExcelSenderGUI:
        window = ExcelSenderGUI(
            config_path=str(tmp / "excel-sender-test-config.json"),
            db_path=str(tmp / db_name),
            start_scheduler=False,
        )
        window.setAttribute(Qt.WA_DontShowOnScreen, True)
        window.setAttribute(Qt.WA_ShowWithoutActivating, True)
        return window

    def open_page(self, window: ExcelSenderGUI, page_key: str, workbench_view: str | None = None) -> None:
        if workbench_view is None:
            window.navigate_to(page_key, persist=False)
        else:
            window.navigate_to(page_key, workbench_view, persist=False)
        self.app.processEvents()

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
                self.assertEqual(window._registered_splitters["data_template.excel"].orientation(), Qt.Vertical)
                self.assertEqual(window._registered_splitters["data_template.template"].orientation(), Qt.Vertical)
                self.assertTrue(window.columns_empty_label.isVisible())
                self.assertFalse(window.columns_view.isVisible())
                self.assertTrue(window.common_attachment_empty_label.isVisible())
                self.assertFalse(window.common_attachment_table.isVisible())
            finally:
                window.close()

    def test_data_template_page_uses_clear_card_sections(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                self.assertEqual(window.data_template_excel_group.property("themeVariant"), "page-shell")
                self.assertEqual(window.data_template_template_group.property("themeVariant"), "page-shell")
                self.assertEqual(window.data_template_source_section.property("themeStyleRole"), "section-shell")
                self.assertEqual(window.data_template_target_section.property("themeStyleRole"), "section-shell")
                self.assertEqual(window.data_template_columns_section.property("themeStyleRole"), "section-shell")
                self.assertEqual(window.data_template_template_section.property("themeStyleRole"), "section-shell")
                self.assertEqual(window.data_template_placeholder_section.property("themeStyleRole"), "section-shell")
                self.assertEqual(window.data_template_attachment_section.property("themeStyleRole"), "section-shell")
                self.assertEqual(window.columns_empty_label.property("themeStyleRole"), "section-empty")
                self.assertEqual(window.common_attachment_empty_label.property("themeStyleRole"), "section-empty")
                self.assertEqual(window._registered_splitters["data_template.template"].count(), 3)
                for section in (
                    window.data_template_source_section,
                    window.data_template_target_section,
                    window.data_template_columns_section,
                    window.data_template_template_section,
                    window.data_template_placeholder_section,
                    window.data_template_attachment_section,
                ):
                    section_cards = [
                        frame
                        for frame in section.findChildren(QFrame)
                        if frame.property("themeStyleRole") == "section-panel"
                    ]
                    self.assertEqual(len(section_cards), 1)
                    self.assertFalse(
                        any(
                            frame.property("themeStyleRole") == "separator"
                            for frame in section.findChildren(QFrame)
                        )
                    )
                label_texts = {label.text() for label in window.findChildren(type(window.data_info_label))}
                self.assertNotIn("先选择 Excel 或 CSV，再决定是否同步导入本地库。", label_texts)
                self.assertNotIn("支持占位符语法 `{{列名}}`，例如：`您好 {{姓名}}`。", label_texts)
                self.assertNotIn("这里会即时提示当前模板用了哪些变量，以及是否存在缺失字段风险。", label_texts)
            finally:
                window.close()

    def test_workbench_uses_nested_splitters_for_horizontal_and_vertical_resize(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                self.assertIsInstance(window.basic_splitter, QSplitter)
                self.assertEqual(window.basic_splitter.orientation(), Qt.Horizontal)
                self.assertEqual(window.basic_left_splitter.orientation(), Qt.Vertical)
                self.assertEqual(window.basic_right_splitter.orientation(), Qt.Vertical)
                self.assertEqual(window.send_prepare_splitter.orientation(), Qt.Horizontal)
                self.assertEqual(window.send_prepare_left_splitter.orientation(), Qt.Vertical)
                self.assertEqual(window.task_center_splitter.orientation(), Qt.Horizontal)
                self.assertEqual(window._registered_splitters["task_center.schedule"].orientation(), Qt.Vertical)
                self.assertEqual(window._registered_splitters["local_store.dataset_shell"].orientation(), Qt.Vertical)
            finally:
                window.close()

    def test_basic_import_group_keeps_compact_default_height(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.show()
                window.resize(1380, 920)
                self.app.processEvents()
                self.assertLess(window.basic_import_group.height(), window.basic_message_group.height())
                self.assertLessEqual(
                    window.basic_import_group.height(),
                    window.basic_import_group.sizeHint().height() + 80,
                )
            finally:
                window.close()

    def test_startup_splitter_defaults_match_screenshot_layouts(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.show()
                self.app.processEvents()
                expected_defaults = {
                    "workbench.basic.left": [250, 761],
                    "workbench.basic.right": [432, 305, 270],
                    "workbench.basic.main": [701, 701],
                    "data_template.excel": [341, 299, 384],
                    "data_template.template": [242, 242, 540],
                    "data_template.main": [777, 625],
                    "local_store.friend": [124, 764],
                    "local_store.group": [124, 764],
                    "local_store.dataset_shell": [82, 944],
                    "local_store.main": [898, 504],
                    "workbench.send.left": [286, 725],
                    "workbench.send.main": [833, 569],
                    "task_center.schedule": [108, 769],
                    "task_center.main": [1102, 300],
                }
                self.assertEqual(window._splitter_default_sizes, expected_defaults)

                for page_key, workbench_view, splitter_keys in (
                    (
                        PAGE_KEY_WORKBENCH,
                        WORKBENCH_VIEW_BASIC,
                        ("workbench.basic.left", "workbench.basic.right", "workbench.basic.main"),
                    ),
                    (
                        PAGE_KEY_DATA_TEMPLATE,
                        None,
                        ("data_template.excel", "data_template.template", "data_template.main"),
                    ),
                    (
                        PAGE_KEY_LOCAL_STORE,
                        None,
                        ("local_store.friend", "local_store.dataset_shell", "local_store.main"),
                    ),
                    (
                        PAGE_KEY_WORKBENCH,
                        WORKBENCH_VIEW_SEND,
                        ("workbench.send.left", "workbench.send.main"),
                    ),
                    (
                        PAGE_KEY_TASK_CENTER,
                        None,
                        ("task_center.schedule", "task_center.main"),
                    ),
                ):
                    self.open_page(window, page_key, workbench_view)
                    for splitter_key in splitter_keys:
                        with self.subTest(splitter_key=splitter_key):
                            self.assertEqual(window._registered_splitters[splitter_key].sizes(), EXPECTED_STARTUP_SPLITTER_SIZES[splitter_key])
            finally:
                window.close()

    def test_close_without_opening_other_pages_keeps_screenshot_splitter_defaults(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            config_path = tmp / "excel-sender-test-config.json"
            window = self.create_window(tmp)
            try:
                window.show()
                self.app.processEvents()
            finally:
                window.close()

            config_data = json.loads(config_path.read_text(encoding="utf-8"))
            saved_splitters = config_data["ui"]["splitter_sizes"]
            for splitter_key in (
                "data_template.excel",
                "data_template.template",
                "data_template.main",
                "local_store.friend",
                "local_store.group",
                "local_store.dataset_shell",
                "local_store.main",
                "workbench.send.left",
                "workbench.send.main",
                "task_center.schedule",
                "task_center.main",
            ):
                with self.subTest(splitter_key=splitter_key):
                    self.assertEqual(saved_splitters[splitter_key], SPLITTER_STARTUP_DEFAULT_SIZES[splitter_key])

    def test_legacy_splitter_defaults_migrate_to_screenshot_layout(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            config_path = tmp / "excel-sender-test-config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "ui": {
                            "splitter_layout_version": 2,
                            "splitter_sizes": LEGACY_STARTUP_SPLITTER_SIZES,
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            window = self.create_window(tmp)
            try:
                window.show()
                self.app.processEvents()
                migrated_config = json.loads(config_path.read_text(encoding="utf-8"))
                self.assertEqual(migrated_config["ui"]["splitter_layout_version"], CURRENT_SPLITTER_LAYOUT_VERSION)
                for splitter_key, expected_sizes in SPLITTER_STARTUP_DEFAULT_SIZES.items():
                    with self.subTest(splitter_key=splitter_key):
                        self.assertEqual(migrated_config["ui"]["splitter_sizes"][splitter_key], expected_sizes)

                for page_key, workbench_view, splitter_keys in (
                    (
                        PAGE_KEY_WORKBENCH,
                        WORKBENCH_VIEW_BASIC,
                        ("workbench.basic.left", "workbench.basic.right", "workbench.basic.main"),
                    ),
                    (
                        PAGE_KEY_DATA_TEMPLATE,
                        None,
                        ("data_template.excel", "data_template.template", "data_template.main"),
                    ),
                    (
                        PAGE_KEY_LOCAL_STORE,
                        None,
                        ("local_store.friend", "local_store.dataset_shell", "local_store.main"),
                    ),
                    (
                        PAGE_KEY_WORKBENCH,
                        WORKBENCH_VIEW_SEND,
                        ("workbench.send.left", "workbench.send.main"),
                    ),
                    (
                        PAGE_KEY_TASK_CENTER,
                        None,
                        ("task_center.schedule", "task_center.main"),
                    ),
                ):
                    self.open_page(window, page_key, workbench_view)
                    for splitter_key in splitter_keys:
                        with self.subTest(splitter_key=splitter_key):
                            self.assertEqual(window._registered_splitters[splitter_key].sizes(), EXPECTED_STARTUP_SPLITTER_SIZES[splitter_key])
            finally:
                window.close()

    def test_user_saved_splitter_sizes_survive_layout_upgrade(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            config_path = tmp / "excel-sender-test-config.json"
            custom_sizes = {
                "workbench.basic.right": [410, 330, 267],
                "data_template.main": [730, 672],
                "workbench.send.main": [810, 592],
                "task_center.main": [960, 442],
            }
            config_path.write_text(
                json.dumps(
                    {
                        "ui": {
                            "splitter_layout_version": 2,
                            "splitter_sizes": custom_sizes,
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            window = self.create_window(tmp)
            try:
                window.show()
                self.app.processEvents()
                upgraded_config = json.loads(config_path.read_text(encoding="utf-8"))
                self.assertEqual(upgraded_config["ui"]["splitter_layout_version"], CURRENT_SPLITTER_LAYOUT_VERSION)
                for splitter_key, expected_sizes in custom_sizes.items():
                    with self.subTest(splitter_key=splitter_key):
                        self.assertEqual(upgraded_config["ui"]["splitter_sizes"][splitter_key], expected_sizes)
            finally:
                window.close()

    def test_local_store_header_removes_redundant_summary_labels(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.show()
                self.app.processEvents()
                self.assertFalse(hasattr(window, "local_store_summary_label"))
                self.assertFalse(hasattr(window, "local_filter_scope_label"))
                self.assertFalse(hasattr(window, "use_local_store_button"))
                local_store_shell_sizes = window._registered_splitters["local_store.dataset_shell"].sizes()
                self.assertEqual(len(local_store_shell_sizes), 2)
            finally:
                window.close()

    def test_local_store_filter_panel_uses_single_primary_action_and_collapsed_examples(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.show()
                self.app.processEvents()
                self.assertEqual(window.refresh_local_store_button.property("role"), "secondary")
                self.assertEqual(window.reset_filter_button.property("role"), "secondary")
                self.assertEqual(window.apply_filter_button.property("role"), "primary")
                self.assertTrue(window.filter_examples_body_label.isHidden())
                collapsed_height = window.filter_examples_card.height()
                self.assertLessEqual(collapsed_height, 64)
                window.filter_examples_toggle_button.click()
                self.app.processEvents()
                self.assertFalse(window.filter_examples_body_label.isHidden())
                self.assertGreater(window.filter_examples_card.height(), collapsed_height + 40)
                self.assertIn("当前页签：好友库", window.filter_scope_summary_label.text())
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
                self.assertEqual(window.columns_view.sizePolicy().verticalPolicy(), QSizePolicy.Expanding)
                self.assertGreater(window.columns_view.maximumHeight(), 1000)

                window.common_attachments = [{"file_path": str(pdf), "file_type": "pdf"}]
                window.refresh_common_attachment_table()
                self.app.processEvents()
                self.assertFalse(window.common_attachment_empty_label.isVisible())
                self.assertTrue(window.common_attachment_table.isVisible())
                self.assertIn("已添加 1 个通用附件", window.common_attachment_status_label.text())
                type_item = window.common_attachment_table.item(0, 0)
                self.assertIsNotNone(type_item)
                self.assertEqual(type_item.text(), "pdf")
                self.assertEqual(type_item.foreground().color().name().lower(), "#111827")
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

    def test_other_pages_also_use_inline_group_headers_instead_of_native_groupbox_titles(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                expected_titles = {
                    "send_prepare_control": "执行概览与主操作",
                    "send_prepare_settings": "发送设置",
                    "send_prepare_preview": "发送计划预览",
                    "task_center_toolbar": "任务中心操作",
                    "task_center_schedule": "任务队列",
                    "task_center_log": "执行日志",
                    "local_store_data": "本地库数据",
                    "local_store_filter": "本地库筛选条件",
                    "data_template_excel": "Excel 数据",
                    "data_template_template": "消息模板",
                }
                for section_key, title_text in expected_titles.items():
                    self.assertIn(section_key, window.inline_section_groups)
                    self.assertIn(section_key, window.inline_section_title_labels)
                    group = window.inline_section_groups[section_key]
                    title_label = window.inline_section_title_labels[section_key]
                    self.assertEqual(group.title(), "")
                    self.assertTrue(bool(group.property("inlineSectionHeader")))
                    self.assertEqual(group.property("inlineSectionTitle"), title_text)
                    self.assertEqual(title_label.text(), title_text)
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

    def test_preview_and_attachment_tables_columns_are_all_resizable(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                preview_modes = [
                    window.preview_table.horizontalHeader().sectionResizeMode(index)
                    for index in range(window.preview_table.columnCount())
                ]
                self.assertEqual(preview_modes, [QHeaderView.Interactive] * window.preview_table.columnCount())

                attachment_modes = [
                    window.common_attachment_table.horizontalHeader().sectionResizeMode(index)
                    for index in range(window.common_attachment_table.columnCount())
                ]
                self.assertEqual(attachment_modes, [QHeaderView.Interactive] * window.common_attachment_table.columnCount())

                basic_attachment_modes = [
                    window.basic_attachment_table.horizontalHeader().sectionResizeMode(index)
                    for index in range(window.basic_attachment_table.columnCount())
                ]
                self.assertEqual(
                    basic_attachment_modes,
                    [QHeaderView.Interactive] * window.basic_attachment_table.columnCount(),
                )
            finally:
                window.close()

    def test_action_buttons_use_compact_width_globally(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                buttons = [
                    window.basic_load_button,
                    window.basic_match_button,
                    window.basic_insert_variable_button,
                    window.basic_select_attachment_button,
                    window.basic_add_attachment_button,
                    window.basic_remove_attachment_button,
                    window.basic_clear_attachment_button,
                    window.basic_start_button,
                    window.basic_stop_button,
                    window.import_json_button,
                    window.refresh_schedule_button,
                    window.preview_schedule_button,
                    window.preview_button,
                    window.start_button,
                    window.stop_button,
                    window.continue_button,
                    window.export_json_button,
                    window.debug_mode_button,
                ]
                for button in buttons:
                    self.assertEqual(button.sizePolicy().horizontalPolicy(), QSizePolicy.Fixed)
                self.assertEqual(window.excel_choose_button.minimumWidth(), 0)
                self.assertEqual(window.load_excel_button.minimumWidth(), 0)
                self.assertEqual(window.import_local_button.minimumWidth(), 0)
            finally:
                window.close()

    def test_task_queue_buttons_use_two_row_three_column_grid(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                expected_positions = {
                    window.refresh_schedule_button: (0, 0),
                    window.preview_schedule_button: (0, 1),
                    window.enable_schedule_button: (0, 2),
                    window.disable_schedule_button: (1, 0),
                    window.delete_schedule_button: (1, 1),
                    window.cancel_schedule_button: (1, 2),
                }
                for button, (row, column) in expected_positions.items():
                    item_index = window.task_action_layout.indexOf(button)
                    self.assertGreaterEqual(item_index, 0)
                    item_row, item_column, row_span, column_span = window.task_action_layout.getItemPosition(item_index)
                    self.assertEqual((item_row, item_column), (row, column))
                    self.assertEqual((row_span, column_span), (1, 1))
            finally:
                window.close()

    def test_schedule_table_columns_are_all_resizable(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                header = window.schedule_table.horizontalHeader()
                modes = [header.sectionResizeMode(index) for index in range(window.schedule_table.columnCount())]
                self.assertEqual(modes, [QHeaderView.Interactive] * window.schedule_table.columnCount())
                self.assertGreaterEqual(window.schedule_table.columnWidth(0), 80)
                self.assertGreaterEqual(window.schedule_table.columnWidth(1), 160)
                self.assertGreaterEqual(window.schedule_table.columnWidth(6), 320)
            finally:
                window.close()

    def test_schedule_table_preserves_manual_column_width_after_refresh(self) -> None:
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
            store.create_scheduled_job(
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
                window.show()
                window.refresh_scheduled_jobs()
                self.app.processEvents()

                window.schedule_table.setColumnWidth(0, 130)
                window.schedule_table.setColumnWidth(1, 260)
                window.schedule_table.setColumnWidth(6, 500)
                self.app.processEvents()

                window.refresh_scheduled_jobs()
                self.app.processEvents()

                self.assertEqual(window.schedule_table.columnWidth(0), 130)
                self.assertEqual(window.schedule_table.columnWidth(1), 260)
                self.assertEqual(window.schedule_table.columnWidth(6), 500)
            finally:
                window.close()

    def test_local_store_table_columns_are_resizable_and_preserve_manual_width(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            source_path = tmp / "contacts.csv"
            source_path.write_text("显示名称,备注,微信号,类型\n张三,重要客户,wx_1,好友\n", encoding="utf-8")

            window = self.create_window(tmp)
            try:
                window.local_store.import_contacts(
                    source_path=source_path,
                    records=[{"显示名称": "张三", "备注": "重要客户", "微信号": "wx_1", "类型": "好友"}],
                    columns=["显示名称", "备注", "微信号", "类型"],
                )
                window.show()
                window.refresh_local_store_page()
                self.app.processEvents()

                friend_table = window.local_store_views["friend"]["table"]
                self.assertIsNotNone(friend_table)
                modes = [friend_table.horizontalHeader().sectionResizeMode(index) for index in range(friend_table.columnCount())]
                self.assertEqual(modes, [QHeaderView.Interactive] * friend_table.columnCount())

                friend_table.setColumnWidth(0, 220)
                friend_table.setColumnWidth(1, 260)
                self.app.processEvents()

                window.refresh_local_store_page()
                self.app.processEvents()

                self.assertEqual(friend_table.columnWidth(0), 220)
                self.assertEqual(friend_table.columnWidth(1), 260)
            finally:
                window.close()

    def test_local_store_column_menu_controls_visible_columns_and_persists_after_refresh(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            source_path = tmp / "contacts.csv"
            source_path.write_text("显示名称,备注,微信号,类型\n张三,重要客户,wx_1,好友\n", encoding="utf-8")

            window = self.create_window(tmp)
            try:
                window.local_store.import_contacts(
                    source_path=source_path,
                    records=[{"显示名称": "张三", "备注": "重要客户", "微信号": "wx_1", "类型": "好友"}],
                    columns=["显示名称", "备注", "微信号", "类型"],
                )
                window.show()
                window.refresh_local_store_page()
                self.app.processEvents()

                friend_table = window.local_store_views["friend"]["table"]
                columns_button = window.local_store_views["friend"]["columns_button"]
                columns_menu = window.local_store_views["friend"]["columns_menu"]
                columns_summary_label = window.local_store_views["friend"]["columns_summary_label"]

                self.assertTrue(columns_button.isEnabled())
                window.populate_local_store_column_menu("friend")
                checkable_actions = [action for action in columns_menu.actions() if action.isCheckable()]
                note_action = next(action for action in checkable_actions if action.text() == "备注")
                note_action.setChecked(False)
                self.app.processEvents()

                self.assertTrue(friend_table.isColumnHidden(1))
                self.assertIn("已显示 3/4 列", columns_summary_label.text())

                window.refresh_local_store_page()
                self.app.processEvents()

                self.assertTrue(friend_table.isColumnHidden(1))
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
                right_sizes_before = window.basic_right_splitter.sizes()

                window.basic_section_toggle_buttons["receiver"].click()
                self.app.processEvents()
                right_sizes_after = window.basic_right_splitter.sizes()

                self.assertTrue(window.basic_section_content_widgets["receiver"].isHidden())
                self.assertLess(window.basic_receiver_group.height(), receiver_height_before)
                self.assertLess(right_sizes_after[0], right_sizes_before[0])
                self.assertGreater(right_sizes_after[1] + right_sizes_after[2], right_sizes_before[1] + right_sizes_before[2])
            finally:
                window.close()

    def test_vertical_splitter_resize_changes_basic_section_heights(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.show()
                window.resize(1380, 920)
                self.app.processEvents()

                initial_heights = (
                    window.basic_receiver_group.height(),
                    window.basic_attachment_group.height(),
                    window.basic_send_group.height(),
                )
                window.basic_right_splitter.setSizes([430, 150, 120])
                self.app.processEvents()
                first_resize_heights = (
                    window.basic_receiver_group.height(),
                    window.basic_attachment_group.height(),
                    window.basic_send_group.height(),
                )
                window.basic_right_splitter.setSizes([220, 260, 220])
                self.app.processEvents()
                second_resize_heights = (
                    window.basic_receiver_group.height(),
                    window.basic_attachment_group.height(),
                    window.basic_send_group.height(),
                )

                self.assertNotEqual(initial_heights, first_resize_heights)
                self.assertNotEqual(first_resize_heights, second_resize_heights)
                self.assertGreater(first_resize_heights[0], first_resize_heights[1])
                self.assertGreater(second_resize_heights[1], first_resize_heights[1])
            finally:
                window.close()

    def test_splitter_sizes_persist_after_restart(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp = Path(tmp_dir)
            window = self.create_window(tmp)
            try:
                window.show()
                window.resize(1460, 960)
                self.app.processEvents()
                window.basic_right_splitter.setSizes([420, 160, 120])
                window.send_prepare_left_splitter.setSizes([200, 520])
                window.save_registered_splitter_states()
            finally:
                window.close()

            config_data = json.loads((tmp / "excel-sender-test-config.json").read_text(encoding="utf-8"))
            saved_splitters = config_data["ui"]["splitter_sizes"]
            self.assertIn("workbench.basic.right", saved_splitters)
            self.assertIn("workbench.send.left", saved_splitters)

            reopened = self.create_window(tmp)
            try:
                reopened.show()
                reopened.resize(1460, 960)
                reopened.open_send_prepare_page()
                self.app.processEvents()

                basic_sizes = reopened.basic_right_splitter.sizes()
                send_sizes = reopened.send_prepare_left_splitter.sizes()
                self.assertEqual(len(basic_sizes), 3)
                self.assertEqual(len(send_sizes), 2)
                self.assertGreater(basic_sizes[0], basic_sizes[1])
                self.assertGreater(basic_sizes[1], basic_sizes[2])
                self.assertLess(send_sizes[0], send_sizes[1])
            finally:
                reopened.close()

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
