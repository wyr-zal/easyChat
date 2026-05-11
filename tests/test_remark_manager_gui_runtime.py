# -*- coding: utf-8 -*-

import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication

import remark_manager.remark_manager_gui as gui


class RemarkManagerGuiRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    def create_window(self):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        config_path = Path(temp_dir.name) / "remark_manager_config.json"
        window = gui.RemarkManagerGUI(config_path=config_path)
        window.setAttribute(Qt.WA_DontShowOnScreen, True)
        window.setAttribute(Qt.WA_ShowWithoutActivating, True)
        self.addCleanup(window.close)
        return window

    def test_defaults_match_two_column_remark_excel(self):
        window = self.create_window()

        self.assertEqual(window.original_col_input.text(), "原始名")
        self.assertEqual(window.remark_col_input.text(), "新备注")
        self.assertEqual(window.interval_spin.value(), 2)
        self.assertIn("原始名", window.format_hint.text())
        self.assertIn("新备注", window.format_hint.text())

    def test_refresh_table_shows_original_and_new_remark(self):
        window = self.create_window()
        window.tasks = [
            {
                "original_name": "科学-陈老师",
                "new_remark": "科学-陈老师-新",
                "status": "",
            }
        ]

        window._refresh_table()

        self.assertEqual(window.task_table.rowCount(), 1)
        self.assertEqual(window.task_table.item(0, 0).text(), "科学-陈老师")
        self.assertEqual(window.task_table.item(0, 1).text(), "科学-陈老师-新")

    def test_window_uses_larger_readable_font_scale(self):
        window = self.create_window()

        self.assertEqual(gui.PRIMARY_FONT_SIZE, 13)
        self.assertEqual(gui.HELPER_FONT_SIZE, 12)
        self.assertGreaterEqual(window.font().pointSize(), 13)
        self.assertGreaterEqual(window.format_hint.font().pointSize(), 12)
        self.assertGreaterEqual(window.log_text.font().pointSize(), 12)
        self.assertGreaterEqual(window.task_table.verticalHeader().defaultSectionSize(), 38)
        self.assertGreaterEqual(window.start_btn.minimumHeight(), 38)


if __name__ == "__main__":
    unittest.main()
