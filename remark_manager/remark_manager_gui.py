# -*- coding: utf-8 -*-
"""
备注批量修改独立窗口。
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QTextCursor
from PyQt5.QtWidgets import (
    QApplication,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QComboBox,
)

from remark_manager.remark_manager_service import (
    RemarkManagerThread,
    build_remark_tasks,
)


CONFIG_PATH = Path(__file__).with_name("remark_manager_config.json")
STATUS_TEXT = {
    "": "待执行",
    "success": "成功",
    "failed": "失败",
}
STATUS_COLORS = {
    "": ("#F3F4F6", "#374151"),
    "success": ("#DCFCE7", "#166534"),
    "failed": ("#FEE2E2", "#991B1B"),
}


class RemarkManagerGUI(QWidget):
    """备注批量修改 GUI。"""

    def __init__(self, config_path: str | os.PathLike = CONFIG_PATH):
        super().__init__()
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.tasks: list[dict] = []
        self.thread: RemarkManagerThread | None = None

        self.setWindowTitle("EasyChat 备注批量修改")
        self.resize(980, 720)
        self.setMinimumSize(860, 620)
        self._build_ui()
        self._apply_config_to_ui()

    # ── config ──────────────────────────────────────────────

    def _load_config(self) -> dict:
        if self.config_path.exists():
            try:
                with self.config_path.open("r", encoding="utf-8") as f:
                    config = json.load(f)
            except Exception:
                config = {}
        else:
            config = {}

        config.setdefault("settings", {})
        config["settings"].setdefault("language", "zh-CN")
        config["settings"].setdefault("operation_interval", 2)
        config.setdefault("import", {})
        config["import"].setdefault("last_excel_path", "")
        config["import"].setdefault("original_column", "原始名")
        config["import"].setdefault("remark_column", "新备注")
        return config

    def _save_config(self):
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with self.config_path.open("w", encoding="utf-8") as f:
                json.dump(self.config, f, indent=4, ensure_ascii=False)
        except OSError:
            pass

    # ── UI ──────────────────────────────────────────────────

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        title = QLabel("备注批量修改")
        title.setStyleSheet("font-size: 22px; font-weight: 700; color: #111827;")
        root.addWidget(title)

        desc = QLabel("导入 Excel/CSV：第 1 列为原始名，第 2 列为要修改成的新备注。")
        desc.setStyleSheet("color: #6B7280; font-size: 12px;")
        root.addWidget(desc)

        import_box = QGroupBox("1. 导入数据")
        import_layout = QVBoxLayout(import_box)
        import_layout.setSpacing(10)

        path_row = QHBoxLayout()
        self.path_input = QLineEdit()
        self.path_input.setPlaceholderText("选择或拖入 Excel / CSV 文件")
        choose_btn = QPushButton("选择文件")
        choose_btn.clicked.connect(self._select_excel)
        path_row.addWidget(QLabel("文件:"))
        path_row.addWidget(self.path_input, 1)
        path_row.addWidget(choose_btn)
        import_layout.addLayout(path_row)

        col_row = QHBoxLayout()
        self.original_col_input = QLineEdit("原始名")
        self.original_col_input.setMaximumWidth(160)
        self.remark_col_input = QLineEdit("新备注")
        self.remark_col_input.setMaximumWidth(160)
        load_btn = QPushButton("导入数据")
        load_btn.clicked.connect(self._load_excel)
        col_row.addWidget(QLabel("原始名列:"))
        col_row.addWidget(self.original_col_input)
        col_row.addWidget(QLabel("新备注列:"))
        col_row.addWidget(self.remark_col_input)
        col_row.addStretch(1)
        col_row.addWidget(load_btn)
        import_layout.addLayout(col_row)

        self.format_hint = QLabel(
            "Excel 格式：原始名 / 新备注。若未找到这两个列名，将自动按前两列读取。"
        )
        self.format_hint.setStyleSheet("color: #6B7280; font-size: 12px;")
        import_layout.addWidget(self.format_hint)
        root.addWidget(import_box)

        settings_box = QGroupBox("2. 执行设置")
        settings_layout = QGridLayout(settings_box)
        settings_layout.setHorizontalSpacing(12)
        settings_layout.setVerticalSpacing(8)

        self.interval_spin = QSpinBox()
        self.interval_spin.setRange(1, 60)
        self.interval_spin.setSuffix(" 秒")
        self.language_combo = QComboBox()
        self.language_combo.addItems(["zh-CN", "zh-TW", "en-US"])
        self.start_btn = QPushButton("开始修改")
        self.start_btn.clicked.connect(self._start_tasks)
        self.stop_btn = QPushButton("停止")
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self._stop_task)

        settings_layout.addWidget(QLabel("每条间隔:"), 0, 0)
        settings_layout.addWidget(self.interval_spin, 0, 1)
        settings_layout.addWidget(QLabel("微信语言:"), 0, 2)
        settings_layout.addWidget(self.language_combo, 0, 3)
        settings_layout.addWidget(self.start_btn, 0, 4)
        settings_layout.addWidget(self.stop_btn, 0, 5)
        settings_layout.setColumnStretch(6, 1)
        root.addWidget(settings_box)

        self.task_table = QTableWidget(0, 3)
        self.task_table.setHorizontalHeaderLabels(["原始名", "新备注", "状态"])
        self.task_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.task_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.task_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.task_table.verticalHeader().setVisible(False)
        self.task_table.setAlternatingRowColors(True)
        root.addWidget(self.task_table, 1)

        self.progress_label = QLabel("进度: 0/0")
        self.progress_label.setStyleSheet("color: #374151;")
        root.addWidget(self.progress_label)

        log_box = QGroupBox("执行日志")
        log_layout = QVBoxLayout(log_box)
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMinimumHeight(140)
        log_layout.addWidget(self.log_text)
        root.addWidget(log_box)

    def _apply_config_to_ui(self):
        import_config = self.config.get("import", {})
        settings = self.config.get("settings", {})
        self.path_input.setText(str(import_config.get("last_excel_path", "")))
        self.original_col_input.setText(str(import_config.get("original_column", "原始名")))
        self.remark_col_input.setText(str(import_config.get("remark_column", "新备注")))
        self.interval_spin.setValue(int(settings.get("operation_interval", 2)))
        language = str(settings.get("language", "zh-CN"))
        index = self.language_combo.findText(language)
        if index >= 0:
            self.language_combo.setCurrentIndex(index)

    # ── import ──────────────────────────────────────────────

    def _select_excel(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "选择 Excel 文件", "", "Excel 文件 (*.xlsx *.xls *.csv)"
        )
        if path:
            self.path_input.setText(path)

    def _read_dataframe(self, path: str) -> pd.DataFrame:
        path = path.strip()
        if not path or not os.path.exists(path):
            raise FileNotFoundError(f"文件不存在: {path}")
        ext = Path(path).suffix.lower()
        if ext == ".csv":
            return pd.read_csv(path, dtype=str).fillna("")
        return pd.read_excel(path, dtype=str).fillna("")

    def _load_excel(self):
        try:
            path = self.path_input.text().strip()
            source_col = self.original_col_input.text().strip()
            target_col = self.remark_col_input.text().strip()
            df = self._read_dataframe(path)

            try:
                self.tasks = build_remark_tasks(df, source_col, target_col)
            except ValueError:
                if source_col == "原始名" and target_col == "新备注":
                    self.tasks = build_remark_tasks(df)
                else:
                    raise

            self._refresh_table()
            self.config["import"]["last_excel_path"] = path
            self.config["import"]["original_column"] = source_col
            self.config["import"]["remark_column"] = target_col
            self._save_config()
            self._append_log(f"[导入] 加载 {len(self.tasks)} 条备注修改任务 from {path}")
        except Exception as exc:
            QMessageBox.critical(self, "导入失败", str(exc))

    # ── table/log ───────────────────────────────────────────

    def _refresh_table(self):
        self.task_table.setRowCount(len(self.tasks))
        for row, task in enumerate(self.tasks):
            self.task_table.setItem(row, 0, QTableWidgetItem(task.get("original_name", "")))
            self.task_table.setItem(row, 1, QTableWidgetItem(task.get("new_remark", "")))
            self._set_status_cell(row, task.get("status", ""))

    def _set_status_cell(self, row: int, status: str):
        text = STATUS_TEXT.get(status, status)
        item = QTableWidgetItem(text)
        item.setTextAlignment(Qt.AlignCenter)
        bg, fg = STATUS_COLORS.get(status, STATUS_COLORS[""])
        from PyQt5.QtGui import QColor

        item.setBackground(QColor(bg))
        item.setForeground(QColor(fg))
        self.task_table.setItem(row, 2, item)

    def _append_log(self, msg: str):
        self.log_text.append(msg)
        self.log_text.moveCursor(QTextCursor.End)

    # ── execution ───────────────────────────────────────────

    def _start_tasks(self):
        if not self.tasks:
            QMessageBox.warning(self, "无任务", "请先导入数据")
            return

        for task in self.tasks:
            task["status"] = ""
            task.pop("error", None)
        self._refresh_table()

        self.config["settings"]["operation_interval"] = self.interval_spin.value()
        self.config["settings"]["language"] = self.language_combo.currentText()
        self._save_config()

        self.thread = RemarkManagerThread(
            tasks=self.tasks,
            locale=self.language_combo.currentText(),
            interval=self.interval_spin.value(),
        )
        self.thread.progress.connect(self._on_progress)
        self.thread.log.connect(self._append_log)
        self.thread.completed.connect(self._on_completed)
        self.thread.error.connect(self._on_error)
        self.thread.start()

        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self._append_log("[执行] 开始批量修改备注")

    def _stop_task(self):
        if self.thread and self.thread.isRunning():
            self.thread.request_stop()
            self._append_log("[操作] 用户请求停止...")

    def _on_progress(self, current: int, total: int, message: str):
        self.progress_label.setText(f"进度: {current}/{total}  {message}")
        self._refresh_table()

    def _on_completed(self, summary: dict):
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self._refresh_table()
        total = summary.get("total", 0)
        success = summary.get("success", 0)
        failed = summary.get("failed", 0)
        stopped = "（用户中止）" if summary.get("stopped", False) else ""
        self._append_log(
            f"\n====== 执行完成{stopped} ======\n"
            f"  总计: {total}  成功: {success}  失败: {failed}\n"
        )

    def _on_error(self, msg: str):
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self._append_log(f"[错误] {msg}")
        QMessageBox.critical(self, "执行异常", msg)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = RemarkManagerGUI()
    window.show()
    sys.exit(app.exec_())
