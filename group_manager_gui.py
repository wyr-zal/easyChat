# -*- coding: utf-8 -*-
"""
EasyChat 群聊管理 GUI
功能：批量创建群聊、移除成员、退出群聊
界面风格对齐 excel_sender_gui.py
"""

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QApplication,
    QButtonGroup,
    QComboBox,
    QFileDialog,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSizePolicy,
    QSpinBox,
    QSplitter,
    QScrollArea,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from module import FileDropLineEdit, build_ui_font, style_helper_label

from group_manager_service import GroupManagerThread

CONFIG_PATH = "group_manager_config.json"
PRIMARY_FONT_SIZE = 11
HELPER_FONT_SIZE = 10

PAGE_CREATE = "create"
PAGE_DELETE = "delete"
PAGE_LOG = "log"

THEME_MODE_AUTO = "auto"
THEME_MODE_LIGHT = "light"
THEME_MODE_DARK = "dark"
AUTO_THEME_DARK_START_HOUR = 18
AUTO_THEME_LIGHT_START_HOUR = 7

THEME_PALETTES = {
    THEME_MODE_LIGHT: {
        "window_bg": "#f3f6fb",
        "panel_bg": "#ffffff",
        "panel_alt_bg": "#eef4ff",
        "text_primary": "#101828",
        "text_secondary": "#475467",
        "text_muted": "#667085",
        "text_inverse": "#ffffff",
        "border": "#d0d5dd",
        "border_strong": "#98a2b3",
        "separator": "#e4e7ec",
        "input_bg": "#ffffff",
        "input_border": "#cbd5e1",
        "tab_bg": "#e8eefb",
        "tab_active_bg": "#ffffff",
        "tab_active_border": "#3b82f6",
        "primary": "#1677ff",
        "primary_hover": "#0f5fd6",
        "primary_soft": "#dbeafe",
        "danger_bg": "#fff1f0",
        "danger_text": "#b42318",
        "danger_border": "#f2b8b5",
        "secondary_bg": "#f8fafc",
        "secondary_text": "#1f2937",
        "secondary_border": "#d0d5dd",
        "neutral_bg": "#eef2ff",
        "neutral_text": "#1d4ed8",
        "neutral_border": "#c7d2fe",
        "success": "#027a48",
        "warning": "#b54708",
        "danger": "#b42318",
        "badge_bg": "#f8fafc",
        "badge_border": "#d0d5dd",
        "table_header_bg": "#eef2f7",
        "table_row_alt": "#f8fafc",
        "table_row_hover": "#f0f4f8",
        "selection_bg": "#1677ff",
        "selection_text": "#ffffff",
        "disabled_bg": "#dbe5f0",
        "disabled_text": "#98a2b3",
        "splitter_handle": "#d0d5dd",
        "splitter_handle_hover": "#98a2b3",
    },
    THEME_MODE_DARK: {
        "window_bg": "#0f172a",
        "panel_bg": "#111827",
        "panel_alt_bg": "#172554",
        "text_primary": "#f8fafc",
        "text_secondary": "#cbd5e1",
        "text_muted": "#94a3b8",
        "text_inverse": "#ffffff",
        "border": "#334155",
        "border_strong": "#475569",
        "separator": "#1e293b",
        "input_bg": "#0b1220",
        "input_border": "#334155",
        "tab_bg": "#162033",
        "tab_active_bg": "#111827",
        "tab_active_border": "#3b82f6",
        "primary": "#3b82f6",
        "primary_hover": "#2563eb",
        "primary_soft": "#1d4ed8",
        "danger_bg": "#3a1b1b",
        "danger_text": "#fca5a5",
        "danger_border": "#7f1d1d",
        "secondary_bg": "#1f2937",
        "secondary_text": "#e5e7eb",
        "secondary_border": "#334155",
        "neutral_bg": "#1e293b",
        "neutral_text": "#bfdbfe",
        "neutral_border": "#3b82f6",
        "success": "#32d583",
        "warning": "#fdb022",
        "danger": "#f97066",
        "badge_bg": "#17212f",
        "badge_border": "#334155",
        "table_header_bg": "#162033",
        "table_row_alt": "#0c1424",
        "table_row_hover": "#1a2535",
        "selection_bg": "#3b82f6",
        "selection_text": "#ffffff",
        "disabled_bg": "#1e293b",
        "disabled_text": "#64748b",
        "splitter_handle": "#334155",
        "splitter_handle_hover": "#475569",
    },
}

STATUS_COLORS = {
    "": ("#f59e0b", "#ffffff"),
    "pending": ("#f59e0b", "#ffffff"),
    "success": ("#10b981", "#ffffff"),
    "failed": ("#ef4444", "#ffffff"),
}
STATUS_TEXT = {
    "": "待执行",
    "pending": "待执行",
    "success": "成功",
    "failed": "失败",
}


class GroupManagerGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.config = self._load_config()
        self.create_tasks: list[dict] = []
        self.delete_tasks: list[dict] = []
        self.thread: GroupManagerThread | None = None
        self._theme_mode = str(self.config.get("settings", {}).get("theme_mode", THEME_MODE_AUTO))
        self._resolved_theme = THEME_MODE_LIGHT
        self._theme_tokens = dict(THEME_PALETTES[THEME_MODE_LIGHT])
        self._init_ui()
        self._apply_theme()

    # ── config ───────────────────────────────────────────────

    def _load_config(self) -> dict:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                config = json.load(f)
        else:
            config = {}

        config.setdefault("settings", {})
        config["settings"].setdefault("language", "zh-CN")
        config["settings"].setdefault("operation_interval", 2)
        config["settings"].setdefault("theme_mode", THEME_MODE_AUTO)
        config.setdefault("create", {})
        config["create"].setdefault("last_excel_path", "")
        config["create"].setdefault("member_separator", ",")
        config["create"].setdefault("name_column", "群名称")
        config["create"].setdefault("member_column", "成员")
        config.setdefault("delete", {})
        config["delete"].setdefault("last_excel_path", "")
        config["delete"].setdefault("name_column", "群名称")
        config["delete"].setdefault("member_column", "移除成员")
        return config

    def _save_config(self):
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(self.config, f, indent=4, ensure_ascii=False)
        except OSError:
            pass

    # ── theme ────────────────────────────────────────────────

    def _resolve_theme(self) -> str:
        if self._theme_mode == THEME_MODE_AUTO:
            hour = datetime.now().hour
            if AUTO_THEME_DARK_START_HOUR <= hour or hour < AUTO_THEME_LIGHT_START_HOUR:
                return THEME_MODE_DARK
            return THEME_MODE_LIGHT
        return self._theme_mode

    def _apply_theme(self):
        self._resolved_theme = self._resolve_theme()
        self._theme_tokens = dict(THEME_PALETTES[self._resolved_theme])
        self.setStyleSheet(self._build_stylesheet(self._theme_tokens))

    def _build_stylesheet(self, t: dict) -> str:
        return f"""
            QWidget {{
                background-color: {t['window_bg']};
                color: {t['text_primary']};
            }}
            QWidget#navigationPanel {{
                background-color: {t['window_bg']};
                border: 1px solid {t['separator']};
                border-radius: 14px;
            }}
            QTabWidget::pane {{
                border: none;
                background: {t['panel_bg']};
                border-radius: 14px;
            }}
            QPushButton {{
                font-size: 11pt; font-weight: 500; min-height: 38px;
                padding: 4px 12px; border-radius: 10px;
                border: 1px solid {t['secondary_border']};
                background: {t['secondary_bg']}; color: {t['secondary_text']};
            }}
            QPushButton:hover {{ border-color: {t['border_strong']}; }}
            QPushButton:disabled {{
                background: {t['disabled_bg']}; color: {t['disabled_text']};
                border-color: {t['disabled_bg']};
            }}
            QPushButton[role="primary"] {{
                background-color: {t['primary']}; color: {t['text_inverse']};
                border: 1px solid {t['primary']}; font-weight: 600;
            }}
            QPushButton[role="primary"]:hover {{
                background-color: {t['primary_hover']}; border-color: {t['primary_hover']};
            }}
            QPushButton[role="danger"] {{
                background-color: {t['danger_bg']}; color: {t['danger_text']};
                border: 1px solid {t['danger_border']};
            }}
            QPushButton[role="secondary"] {{
                background-color: {t['secondary_bg']}; color: {t['secondary_text']};
                border: 1px solid {t['secondary_border']};
            }}
            QPushButton[role="nav"] {{
                background-color: transparent; color: {t['text_secondary']};
                border: 1px solid transparent; text-align: left;
                padding: 8px 12px; font-weight: 600;
            }}
            QPushButton[role="nav"]:hover {{
                background-color: {t['tab_bg']}; border-color: {t['border']};
            }}
            QPushButton[role="nav"]:checked {{
                background-color: {t['tab_active_bg']}; color: {t['text_primary']};
                border-color: {t['tab_active_border']};
            }}
            QSpinBox, QLineEdit, QComboBox, QPlainTextEdit {{
                min-height: 34px; background: {t['input_bg']}; color: {t['text_primary']};
                border: 1px solid {t['input_border']}; border-radius: 10px;
                selection-background-color: {t['selection_bg']};
                selection-color: {t['selection_text']};
            }}
            QGroupBox {{
                font-weight: 600; border: 1px solid {t['border']};
                border-radius: 14px; margin-top: 12px; padding: 12px;
                background: {t['panel_bg']};
            }}
            QGroupBox::title {{
                subcontrol-origin: margin; left: 14px; padding: 0 6px;
                color: {t['text_primary']}; background: {t['panel_bg']};
            }}
            QTableWidget {{
                background: {t['panel_bg']}; border: 1px solid {t['border']};
                border-radius: 10px; gridline-color: {t['separator']};
            }}
            QTableWidget QHeaderView::section {{
                background: {t['table_header_bg']}; color: {t['text_primary']};
                border: none; padding: 6px 8px; font-weight: 600;
            }}
            QPlainTextEdit#logArea {{
                background: {t['panel_bg']}; border: 1px solid {t['border']};
                border-radius: 10px; padding: 8px; font-family: Consolas, monospace;
            }}
            QSplitter::handle {{
                background: {t['splitter_handle']}; margin: 2px;
            }}
            QSplitter::handle:hover {{
                background: {t['splitter_handle_hover']};
            }}
            QScrollArea {{ border: none; background: transparent; }}
            QFrame[themeRole="separator"] {{
                color: {t['separator']};
            }}
        """

    # ── UI helpers ───────────────────────────────────────────

    def _font(self, size: int = PRIMARY_FONT_SIZE, bold: bool = False) -> QFont:
        f = QFont(self.font())
        f.setPointSize(size)
        f.setBold(bold)
        return f

    def _title_label(self, text: str) -> QLabel:
        label = QLabel(text)
        label.setFont(self._font(11, bold=True))
        return label

    def _helper_label(self, text: str, color: str = "#667085") -> QLabel:
        label = QLabel(text)
        label.setWordWrap(True)
        label.setFont(self._font(HELPER_FONT_SIZE))
        label.setStyleSheet(f"color: {color};")
        return label

    def _set_button_role(self, btn: QPushButton, role: str, min_w: int = 0, min_h: int = 0):
        btn.setProperty("role", role)
        if min_w:
            btn.setMinimumWidth(min_w)
        if min_h:
            btn.setMinimumHeight(min_h)
        btn.style().unpolish(btn)
        btn.style().polish(btn)

    def _separator(self) -> QFrame:
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setFrameShadow(QFrame.Sunken)
        line.setProperty("themeRole", "separator")
        return line

    def _scroll_wrap(self, widget: QWidget) -> QScrollArea:
        area = QScrollArea()
        area.setWidgetResizable(True)
        area.setWidget(widget)
        area.setFrameShape(QFrame.NoFrame)
        return area

    # ── init UI ──────────────────────────────────────────────

    def _init_ui(self):
        self.setWindowTitle("EasyChat 群聊管理")
        self.setWindowFlag(Qt.WindowMinimizeButtonHint, True)
        self.setWindowFlag(Qt.WindowMaximizeButtonHint, True)
        self.resize(1200, 800)
        self.setMinimumSize(900, 600)

        base_font = QFont(self.font())
        base_font.setPointSize(PRIMARY_FONT_SIZE)
        self.setFont(base_font)

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        body = QHBoxLayout()
        body.setSpacing(12)

        self.nav_panel = self._build_navigation()
        body.addWidget(self.nav_panel)

        self.page_stack = QTabWidget()
        self.page_stack.setDocumentMode(True)
        self.page_stack.tabBar().hide()

        self.create_page = self._build_create_page()
        self.delete_page = self._build_delete_page()
        self.log_page = self._build_log_page()

        self.page_stack.addTab(self.create_page, "创建群聊")
        self.page_stack.addTab(self.delete_page, "删除群聊")
        self.page_stack.addTab(self.log_page, "执行日志")

        body.addWidget(self.page_stack, stretch=1)
        root.addLayout(body, stretch=1)

        self._navigate(PAGE_CREATE)

    # ── navigation ───────────────────────────────────────────

    def _build_navigation(self) -> QWidget:
        container = QWidget()
        container.setObjectName("navigationPanel")
        container.setMinimumWidth(148)
        container.setMaximumWidth(180)
        layout = QVBoxLayout(container)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        title = self._title_label("导航")
        layout.addWidget(title)

        self.nav_group = QButtonGroup(self)
        self.nav_group.setExclusive(True)
        self.nav_buttons: dict[str, QPushButton] = {}

        for key, label in (
            (PAGE_CREATE, "创建群聊"),
            (PAGE_DELETE, "删除群聊"),
            (PAGE_LOG, "执行日志"),
        ):
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setProperty("role", "nav")
            btn.clicked.connect(lambda _, k=key: self._navigate(k))
            self.nav_group.addButton(btn)
            self.nav_buttons[key] = btn
            layout.addWidget(btn)

        layout.addWidget(self._separator())

        theme_row = QHBoxLayout()
        theme_label = QLabel("主题")
        theme_label.setFont(self._font(HELPER_FONT_SIZE))
        theme_row.addWidget(theme_label)
        self.theme_combo = QComboBox()
        self.theme_combo.addItems(["自动", "浅色", "深色"])
        theme_map = {THEME_MODE_AUTO: 0, THEME_MODE_LIGHT: 1, THEME_MODE_DARK: 2}
        self.theme_combo.setCurrentIndex(theme_map.get(self._theme_mode, 0))
        self.theme_combo.currentIndexChanged.connect(self._on_theme_changed)
        theme_row.addWidget(self.theme_combo)
        layout.addLayout(theme_row)

        layout.addStretch(1)
        return container

    def _navigate(self, key: str):
        pages = {PAGE_CREATE: 0, PAGE_DELETE: 1, PAGE_LOG: 2}
        self.page_stack.setCurrentIndex(pages.get(key, 0))
        if key in self.nav_buttons:
            self.nav_buttons[key].setChecked(True)

    def _on_theme_changed(self, index: int):
        modes = [THEME_MODE_AUTO, THEME_MODE_LIGHT, THEME_MODE_DARK]
        self._theme_mode = modes[index]
        self.config["settings"]["theme_mode"] = self._theme_mode
        self._save_config()
        self._apply_theme()

    # ── create page ──────────────────────────────────────────

    def _build_create_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        title = self._title_label("批量创建群聊")
        layout.addWidget(title)

        import_group = QGroupBox("1. 导入数据")
        ig_layout = QVBoxLayout(import_group)
        ig_layout.setSpacing(8)

        path_row = QHBoxLayout()
        self.create_path_input = FileDropLineEdit(
            suffixes=[".xlsx", ".xls", ".csv"], parent=self,
        )
        self.create_path_input.setPlaceholderText("选择或拖入 Excel 文件")
        path_row.addWidget(self.create_path_input)
        choose_btn = QPushButton("选择文件")
        choose_btn.clicked.connect(self._select_create_excel)
        self._set_button_role(choose_btn, "secondary", min_w=100)
        path_row.addWidget(choose_btn)
        ig_layout.addLayout(path_row)

        col_row = QHBoxLayout()
        col_row.addWidget(QLabel("群名称列:"))
        self.create_name_col = QLineEdit(self.config["create"]["name_column"])
        self.create_name_col.setMaximumWidth(150)
        col_row.addWidget(self.create_name_col)
        col_row.addWidget(QLabel("成员列:"))
        self.create_member_col = QLineEdit(self.config["create"]["member_column"])
        self.create_member_col.setMaximumWidth(150)
        col_row.addWidget(self.create_member_col)
        col_row.addWidget(QLabel("分隔符:"))
        self.create_separator = QLineEdit(self.config["create"]["member_separator"])
        self.create_separator.setMaximumWidth(60)
        col_row.addWidget(self.create_separator)
        col_row.addStretch(1)

        load_btn = QPushButton("导入数据")
        load_btn.clicked.connect(self._load_create_excel)
        self._set_button_role(load_btn, "primary", min_w=120)
        col_row.addWidget(load_btn)
        ig_layout.addLayout(col_row)

        hint = self._helper_label(
            "Excel 格式：每行一个群，群名称列（可选，留空自动生成）+ 成员列（逗号分隔的联系人名称）"
        )
        ig_layout.addWidget(hint)

        preview_group = QGroupBox("2. 预览")
        pg_layout = QVBoxLayout(preview_group)

        self.create_info_label = self._helper_label("")
        pg_layout.addWidget(self.create_info_label)

        self.create_table = QTableWidget(0, 4)
        self.create_table.setHorizontalHeaderLabels(["群名称", "成员列表", "人数", "状态"])
        self.create_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Interactive)
        self.create_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.create_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.create_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.create_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.create_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.create_table.setAlternatingRowColors(True)
        pg_layout.addWidget(self.create_table, stretch=1)

        exec_group = QGroupBox("3. 执行")
        eg_layout = QVBoxLayout(exec_group)
        eg_layout.setSpacing(8)

        settings_row = QHBoxLayout()
        settings_row.addWidget(QLabel("操作间隔(秒):"))
        self.create_interval_spin = QSpinBox()
        self.create_interval_spin.setRange(1, 60)
        self.create_interval_spin.setValue(self.config["settings"]["operation_interval"])
        settings_row.addWidget(self.create_interval_spin)
        settings_row.addStretch(1)
        eg_layout.addLayout(settings_row)

        self.create_progress_label = self._title_label("")
        self.create_progress_label.setVisible(False)
        eg_layout.addWidget(self.create_progress_label)

        btn_row = QHBoxLayout()
        self.create_start_btn = QPushButton("开始创建")
        self.create_start_btn.clicked.connect(self._start_create)
        self._set_button_role(self.create_start_btn, "primary", min_w=160, min_h=44)
        btn_row.addWidget(self.create_start_btn)

        self.create_stop_btn = QPushButton("停止")
        self.create_stop_btn.setEnabled(False)
        self.create_stop_btn.clicked.connect(self._stop_task)
        self._set_button_role(self.create_stop_btn, "danger", min_w=120, min_h=44)
        btn_row.addWidget(self.create_stop_btn)
        btn_row.addStretch(1)
        eg_layout.addLayout(btn_row)

        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(import_group)
        splitter.addWidget(preview_group)
        splitter.addWidget(exec_group)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 3)
        splitter.setStretchFactor(2, 1)
        layout.addWidget(splitter, stretch=1)

        return page

    # ── delete page ──────────────────────────────────────────

    def _build_delete_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        title = self._title_label("删除群聊 / 移除成员")
        layout.addWidget(title)

        mode_row = QHBoxLayout()
        mode_row.addWidget(QLabel("操作模式:"))
        self.delete_mode_combo = QComboBox()
        self.delete_mode_combo.addItems(["移除成员", "退出群聊"])
        self.delete_mode_combo.currentIndexChanged.connect(self._on_delete_mode_changed)
        mode_row.addWidget(self.delete_mode_combo)
        mode_row.addStretch(1)
        layout.addLayout(mode_row)

        import_group = QGroupBox("1. 导入数据")
        ig_layout = QVBoxLayout(import_group)
        ig_layout.setSpacing(8)

        path_row = QHBoxLayout()
        self.delete_path_input = FileDropLineEdit(
            suffixes=[".xlsx", ".xls", ".csv"], parent=self,
        )
        self.delete_path_input.setPlaceholderText("选择或拖入 Excel 文件")
        path_row.addWidget(self.delete_path_input)
        choose_btn = QPushButton("选择文件")
        choose_btn.clicked.connect(self._select_delete_excel)
        self._set_button_role(choose_btn, "secondary", min_w=100)
        path_row.addWidget(choose_btn)
        ig_layout.addLayout(path_row)

        col_row = QHBoxLayout()
        col_row.addWidget(QLabel("群名称列:"))
        self.delete_name_col = QLineEdit(self.config["delete"]["name_column"])
        self.delete_name_col.setMaximumWidth(150)
        col_row.addWidget(self.delete_name_col)

        self.delete_member_label = QLabel("移除成员列:")
        col_row.addWidget(self.delete_member_label)
        self.delete_member_col = QLineEdit(self.config["delete"]["member_column"])
        self.delete_member_col.setMaximumWidth(150)
        col_row.addWidget(self.delete_member_col)
        col_row.addStretch(1)

        load_btn = QPushButton("导入数据")
        load_btn.clicked.connect(self._load_delete_excel)
        self._set_button_role(load_btn, "primary", min_w=120)
        col_row.addWidget(load_btn)
        ig_layout.addLayout(col_row)

        self.delete_hint = self._helper_label(
            "移除成员模式：Excel 需包含群名称列和移除成员列（逗号分隔）\n"
            "退出群聊模式：Excel 只需群名称列"
        )
        ig_layout.addWidget(self.delete_hint)

        preview_group = QGroupBox("2. 预览")
        pg_layout = QVBoxLayout(preview_group)

        self.delete_info_label = self._helper_label("")
        pg_layout.addWidget(self.delete_info_label)

        self.delete_table = QTableWidget(0, 4)
        self.delete_table.setHorizontalHeaderLabels(["群名称", "操作详情", "数量", "状态"])
        self.delete_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Interactive)
        self.delete_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.delete_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.delete_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.delete_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.delete_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.delete_table.setAlternatingRowColors(True)
        pg_layout.addWidget(self.delete_table, stretch=1)

        exec_group = QGroupBox("3. 执行")
        eg_layout = QVBoxLayout(exec_group)
        eg_layout.setSpacing(8)

        settings_row = QHBoxLayout()
        settings_row.addWidget(QLabel("操作间隔(秒):"))
        self.delete_interval_spin = QSpinBox()
        self.delete_interval_spin.setRange(1, 60)
        self.delete_interval_spin.setValue(self.config["settings"]["operation_interval"])
        settings_row.addWidget(self.delete_interval_spin)
        settings_row.addStretch(1)
        eg_layout.addLayout(settings_row)

        self.delete_progress_label = self._title_label("")
        self.delete_progress_label.setVisible(False)
        eg_layout.addWidget(self.delete_progress_label)

        btn_row = QHBoxLayout()
        self.delete_start_btn = QPushButton("开始执行")
        self.delete_start_btn.clicked.connect(self._start_delete)
        self._set_button_role(self.delete_start_btn, "primary", min_w=160, min_h=44)
        btn_row.addWidget(self.delete_start_btn)

        self.delete_stop_btn = QPushButton("停止")
        self.delete_stop_btn.setEnabled(False)
        self.delete_stop_btn.clicked.connect(self._stop_task)
        self._set_button_role(self.delete_stop_btn, "danger", min_w=120, min_h=44)
        btn_row.addWidget(self.delete_stop_btn)
        btn_row.addStretch(1)
        eg_layout.addLayout(btn_row)

        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(import_group)
        splitter.addWidget(preview_group)
        splitter.addWidget(exec_group)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 3)
        splitter.setStretchFactor(2, 1)
        layout.addWidget(splitter, stretch=1)

        return page

    # ── log page ─────────────────────────────────────────────

    def _build_log_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        header = QHBoxLayout()
        title = self._title_label("执行日志")
        header.addWidget(title)
        header.addStretch(1)
        clear_btn = QPushButton("清空日志")
        clear_btn.clicked.connect(lambda: self.log_area.clear())
        self._set_button_role(clear_btn, "secondary", min_w=100)
        header.addWidget(clear_btn)
        layout.addLayout(header)

        self.log_area = QPlainTextEdit()
        self.log_area.setObjectName("logArea")
        self.log_area.setReadOnly(True)
        self.log_area.setFont(self._font(10))
        layout.addWidget(self.log_area, stretch=1)

        return page

    # ── Excel loading ────────────────────────────────────────

    def _select_create_excel(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "选择 Excel 文件", "", "Excel 文件 (*.xlsx *.xls *.csv)"
        )
        if path:
            self.create_path_input.setText(path)

    def _select_delete_excel(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "选择 Excel 文件", "", "Excel 文件 (*.xlsx *.xls *.csv)"
        )
        if path:
            self.delete_path_input.setText(path)

    def _read_excel(self, path: str) -> pd.DataFrame:
        path = path.strip()
        if not path or not os.path.exists(path):
            raise FileNotFoundError(f"文件不存在: {path}")
        ext = Path(path).suffix.lower()
        if ext == ".csv":
            return pd.read_csv(path, dtype=str).fillna("")
        return pd.read_excel(path, dtype=str).fillna("")

    @staticmethod
    def _split_members(members_str: str, extra_sep: str = "") -> list:
        """同时识别中英文逗号、分号、顿号；可叠加用户自定义分隔符。"""
        if not members_str:
            return []
        # 默认分隔符集合：英文逗号、中文逗号、英文分号、中文分号、顿号
        default_seps = [",", "，", ";", "；", "、"]
        seps = list(default_seps)
        if extra_sep and extra_sep not in seps:
            seps.append(extra_sep)
        pattern = "|".join(re.escape(s) for s in seps)
        return [m.strip() for m in re.split(pattern, members_str) if m.strip()]

    def _load_create_excel(self):
        try:
            path = self.create_path_input.text().strip()
            df = self._read_excel(path)
            name_col = self.create_name_col.text().strip()
            member_col = self.create_member_col.text().strip()
            sep = self.create_separator.text() or ","

            if member_col not in df.columns:
                QMessageBox.warning(self, "列不存在", f"Excel 中未找到列 '{member_col}'")
                return

            self.create_tasks.clear()
            for _, row in df.iterrows():
                group_name = str(row.get(name_col, "")).strip() if name_col in df.columns else ""
                members_str = str(row.get(member_col, "")).strip()
                members = self._split_members(members_str, sep)
                if members:
                    self.create_tasks.append({
                        "group_name": group_name,
                        "members": members,
                        "status": "",
                    })

            self._refresh_create_table()
            self.create_info_label.setText(f"已导入 {len(self.create_tasks)} 个群聊任务")
            self.config["create"]["last_excel_path"] = path
            self.config["create"]["name_column"] = name_col
            self.config["create"]["member_column"] = member_col
            self.config["create"]["member_separator"] = sep
            self._save_config()
            self._append_log(f"[导入] 创建群聊: 加载 {len(self.create_tasks)} 条记录 from {path}")

        except Exception as e:
            QMessageBox.critical(self, "导入失败", str(e))

    def _load_delete_excel(self):
        try:
            path = self.delete_path_input.text().strip()
            df = self._read_excel(path)
            name_col = self.delete_name_col.text().strip()
            member_col = self.delete_member_col.text().strip()
            is_remove_mode = self.delete_mode_combo.currentIndex() == 0

            if name_col not in df.columns:
                QMessageBox.warning(self, "列不存在", f"Excel 中未找到列 '{name_col}'")
                return

            if is_remove_mode and member_col not in df.columns:
                QMessageBox.warning(self, "列不存在", f"Excel 中未找到列 '{member_col}'")
                return

            self.delete_tasks.clear()
            for _, row in df.iterrows():
                group_name = str(row.get(name_col, "")).strip()
                if not group_name:
                    continue
                if is_remove_mode:
                    members_str = str(row.get(member_col, "")).strip()
                    members = self._split_members(members_str)
                    self.delete_tasks.append({
                        "group_name": group_name,
                        "members": members,
                        "status": "",
                    })
                else:
                    self.delete_tasks.append({
                        "group_name": group_name,
                        "status": "",
                    })

            self._refresh_delete_table()
            self.delete_info_label.setText(f"已导入 {len(self.delete_tasks)} 条任务")
            self.config["delete"]["last_excel_path"] = path
            self.config["delete"]["name_column"] = name_col
            self.config["delete"]["member_column"] = member_col
            self._save_config()
            mode_text = "移除成员" if is_remove_mode else "退出群聊"
            self._append_log(f"[导入] {mode_text}: 加载 {len(self.delete_tasks)} 条记录 from {path}")

        except Exception as e:
            QMessageBox.critical(self, "导入失败", str(e))

    # ── table refresh ────────────────────────────────────────

    def _refresh_create_table(self):
        self.create_table.setRowCount(len(self.create_tasks))
        for i, task in enumerate(self.create_tasks):
            self.create_table.setItem(i, 0, QTableWidgetItem(task.get("group_name", "")))
            self.create_table.setItem(i, 1, QTableWidgetItem(", ".join(task.get("members", []))))
            self.create_table.setItem(i, 2, QTableWidgetItem(str(len(task.get("members", [])))))
            self._set_status_cell(self.create_table, i, 3, task.get("status", ""))

    def _refresh_delete_table(self):
        is_remove = self.delete_mode_combo.currentIndex() == 0
        self.delete_table.setRowCount(len(self.delete_tasks))
        for i, task in enumerate(self.delete_tasks):
            self.delete_table.setItem(i, 0, QTableWidgetItem(task.get("group_name", "")))
            if is_remove:
                members = task.get("members", [])
                self.delete_table.setItem(i, 1, QTableWidgetItem(", ".join(members)))
                self.delete_table.setItem(i, 2, QTableWidgetItem(str(len(members))))
            else:
                self.delete_table.setItem(i, 1, QTableWidgetItem("退出群聊"))
                self.delete_table.setItem(i, 2, QTableWidgetItem("-"))
            self._set_status_cell(self.delete_table, i, 3, task.get("status", ""))

    def _set_status_cell(self, table: QTableWidget, row: int, col: int, status: str):
        text = STATUS_TEXT.get(status, status)
        item = QTableWidgetItem(text)
        item.setTextAlignment(Qt.AlignCenter)
        bg_color, fg_color = STATUS_COLORS.get(status, STATUS_COLORS[""])
        from PyQt5.QtGui import QColor
        item.setBackground(QColor(bg_color))
        item.setForeground(QColor(fg_color))
        table.setItem(row, col, item)

    # ── delete mode toggle ───────────────────────────────────

    def _on_delete_mode_changed(self, index: int):
        is_remove = index == 0
        self.delete_member_label.setVisible(is_remove)
        self.delete_member_col.setVisible(is_remove)
        if is_remove:
            self.delete_hint.setText(
                "移除成员模式：Excel 需包含群名称列和移除成员列（逗号分隔）"
            )
        else:
            self.delete_hint.setText(
                "退出群聊模式：Excel 只需群名称列，将逐个退出指定群聊"
            )

    # ── execution ────────────────────────────────────────────

    def _start_create(self):
        if not self.create_tasks:
            QMessageBox.warning(self, "无任务", "请先导入数据")
            return
        for t in self.create_tasks:
            t["status"] = ""
        self._refresh_create_table()

        self._run_thread(
            GroupManagerThread.MODE_CREATE,
            self.create_tasks,
            self.create_interval_spin.value(),
        )
        self.create_start_btn.setEnabled(False)
        self.create_stop_btn.setEnabled(True)

    def _start_delete(self):
        if not self.delete_tasks:
            QMessageBox.warning(self, "无任务", "请先导入数据")
            return
        for t in self.delete_tasks:
            t["status"] = ""
        self._refresh_delete_table()

        is_remove = self.delete_mode_combo.currentIndex() == 0
        mode = GroupManagerThread.MODE_REMOVE if is_remove else GroupManagerThread.MODE_EXIT

        self._run_thread(mode, self.delete_tasks, self.delete_interval_spin.value())
        self.delete_start_btn.setEnabled(False)
        self.delete_stop_btn.setEnabled(True)

    def _run_thread(self, mode: str, tasks: list[dict], interval: float):
        self.thread = GroupManagerThread(
            mode=mode,
            tasks=tasks,
            locale=self.config["settings"]["language"],
            interval=interval,
        )
        self.thread.progress.connect(self._on_progress)
        self.thread.log.connect(self._on_log)
        self.thread.completed.connect(self._on_completed)
        self.thread.error.connect(self._on_error)
        self.thread.start()
        self._navigate(PAGE_LOG)

    def _stop_task(self):
        if self.thread and self.thread.isRunning():
            self.thread.request_stop()
            self._append_log("[操作] 用户请求停止...")

    def _on_progress(self, current: int, total: int, message: str):
        text = f"进度: {current}/{total}  {message}"
        self.create_progress_label.setText(text)
        self.create_progress_label.setVisible(True)
        self.delete_progress_label.setText(text)
        self.delete_progress_label.setVisible(True)
        self._refresh_create_table()
        self._refresh_delete_table()

    def _on_log(self, msg: str):
        self._append_log(msg)

    def _on_completed(self, summary: dict):
        self.create_start_btn.setEnabled(True)
        self.create_stop_btn.setEnabled(False)
        self.delete_start_btn.setEnabled(True)
        self.delete_stop_btn.setEnabled(False)
        self._refresh_create_table()
        self._refresh_delete_table()

        total = summary.get("total", 0)
        success = summary.get("success", 0)
        failed = summary.get("failed", 0)
        stopped = summary.get("stopped", False)
        status_text = "（用户中止）" if stopped else ""
        self._append_log(
            f"\n====== 执行完成{status_text} ======\n"
            f"  总计: {total}  成功: {success}  失败: {failed}\n"
        )

    def _on_error(self, msg: str):
        self.create_start_btn.setEnabled(True)
        self.create_stop_btn.setEnabled(False)
        self.delete_start_btn.setEnabled(True)
        self.delete_stop_btn.setEnabled(False)
        self._append_log(f"[ERROR] {msg}")
        QMessageBox.critical(self, "执行错误", msg)

    def _append_log(self, text: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_area.appendPlainText(f"[{timestamp}] {text}")
        self.log_area.verticalScrollBar().setValue(
            self.log_area.verticalScrollBar().maximum()
        )


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = GroupManagerGUI()
    window.show()
    sys.exit(app.exec_())
