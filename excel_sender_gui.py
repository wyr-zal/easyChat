import json
import os
import sys
import re
import inspect
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from PyQt5.QtCore import QDateTime, Qt, QTimer
from PyQt5.QtGui import QColor, QFont
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QRadioButton,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QStackedWidget,
    QSplitter,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from excel_reader import DEFAULT_SEND_TARGET_COLUMN, load_contact_records, validate_contact_records
from excel_sender_service import (
    CUSTOM_MESSAGE_OVERRIDE_KEY,
    DEFAULT_REPORT_TARGET,
    PersonalizedSendThread,
)
from excel_template import extract_placeholders, find_missing_fields, render_template
from local_contact_store import (
    DATASET_ALL,
    DATASET_FRIEND,
    DATASET_GROUP,
    DATASET_LABELS,
    DEFAULT_LOCAL_DB_PATH,
    FRIEND_SEARCH_PRIORITY,
    GROUP_SEARCH_PRIORITY,
    LocalContactStore,
    SCHEDULE_STATUS_CANCELLED,
    SCHEDULE_STATUS_COMPLETED,
    SCHEDULE_STATUS_FAILED,
    SCHEDULE_MODE_CRON,
    SCHEDULE_MODE_DAILY,
    SCHEDULE_MODE_ONCE,
    SCHEDULE_MODE_WEEKLY,
    SCHEDULE_STATUS_PENDING,
    SCHEDULE_STATUS_RUNNING,
    ScheduledSendJob,
    SOURCE_MODE_FILE as STORE_SOURCE_MODE_FILE,
    SOURCE_MODE_LOCAL_DB as STORE_SOURCE_MODE_LOCAL_DB,
)
from module import AttachmentManageDialog, ContactConfirmDialog, FileDropLineEdit

try:
    import json_task_io as json_task_helper
except Exception:
    json_task_helper = None


DISPLAY_NAME_OVERRIDE_KEY = "__display_name_override"
RECORD_ID_KEY = "__record_id"
TASK_ITEM_ID_KEY = "__task_item_id"
TARGET_VALUE_KEY = "__target_value"
ROW_ATTACHMENTS_KEY = "attachments"
ROW_ATTACHMENT_MODE_KEY = "attachment_mode"
ROW_TARGET_TYPE_KEY = "target_type"
ROW_MESSAGE_MODE_KEY = "message_mode"
ROW_SEND_STATUS_KEY = "send_status"
ROW_ATTACHMENT_STATUS_KEY = "attachment_status"
ROW_ERROR_MSG_KEY = "error_msg"
ROW_SEND_TIME_KEY = "send_time"
SOURCE_MODE_FILE = STORE_SOURCE_MODE_FILE
SOURCE_MODE_LOCAL_DB = STORE_SOURCE_MODE_LOCAL_DB
SOURCE_MODE_JSON = "json"
DEFAULT_LOCAL_FILTER_FIELDS = ("显示名称", "备注", "昵称", "标签", "详细描述")
LOCAL_DB_HEADER_TITLE = "微信搜索关键词"
JSON_HEADER_TITLE = "发送对象"
JSON_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
PRIMARY_UI_FONT_SIZE = 11
HELPER_UI_FONT_SIZE = 10
TERMINAL_SEND_STATUSES = {"success", "failed", "partial_success", "skipped"}
THEME_MODE_AUTO = "auto"
THEME_MODE_LIGHT = "light"
THEME_MODE_DARK = "dark"
AUTO_THEME_DARK_START_HOUR = 18
AUTO_THEME_LIGHT_START_HOUR = 7
PAGE_KEY_WORKBENCH = "workbench"
PAGE_KEY_DATA_TEMPLATE = "data_template"
PAGE_KEY_LOCAL_STORE = "local_store"
PAGE_KEY_TASK_CENTER = "task_center"
WORKBENCH_VIEW_BASIC = "basic"
WORKBENCH_VIEW_SEND = "send_prepare"
CURRENT_SPLITTER_LAYOUT_VERSION = 2
BASIC_SEND_STATUS_TEXT = {
    "": "待发送",
    "pending": "待发送",
    "success": "已发送",
    "failed": "失败",
    "partial_success": "部分成功",
    "skipped": "已跳过",
}
BASIC_SEND_STATUS_COLORS = {
    "": ("#f59e0b", "#ffffff"),
    "pending": ("#f59e0b", "#ffffff"),
    "success": ("#10b981", "#ffffff"),
    "failed": ("#ef4444", "#ffffff"),
    "partial_success": ("#8b5cf6", "#ffffff"),
    "skipped": ("#6b7280", "#ffffff"),
}
ATTACHMENT_TYPE_COLORS = {
    "image": ("#3b82f6", "#ffffff"),
    "图片": ("#3b82f6", "#ffffff"),
    "file": ("#6b7280", "#ffffff"),
    "文件": ("#6b7280", "#ffffff"),
    "video": ("#8b5cf6", "#ffffff"),
    "视频": ("#8b5cf6", "#ffffff"),
    "document": ("#10b981", "#ffffff"),
    "文档": ("#10b981", "#ffffff"),
}
SEMANTIC_COLOR_MAP = {
    "#555": "muted",
    "#027a48": "success",
    "#b54708": "warning",
    "#b42318": "danger",
    "#1f2937": "title",
    "#111827": "title",
}
THEME_PALETTES: dict[str, dict[str, str]] = {
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
        "table_header_hover": "#e0e8f0",
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
        "table_header_hover": "#1a2a40",
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


class ExcelSenderGUI(QWidget):
    def __init__(
        self,
        *,
        config_path: str = "excel_sender_config.json",
        db_path: str | None = None,
        start_scheduler: bool = True,
    ):
        super().__init__()
        self.config_path = config_path
        self.config = self.load_config()
        resolved_db_path = db_path or self.config["local_store"]["db_path"]
        self.local_store = LocalContactStore(resolved_db_path)
        self.records: list[dict[str, str]] = []
        self.source_records: list[dict[str, str]] = []
        self.filtered_records: list[dict[str, str]] = []
        self.columns: list[str] = []
        self.send_thread: PersonalizedSendThread | None = None
        self.records_loaded = False
        self.loaded_excel_path = ""
        self.active_source_mode = SOURCE_MODE_FILE
        self.current_batch_id: int | None = None
        self.current_task_id: int | None = None
        self.current_batch_ids: dict[str, int] = {}
        self.active_scheduled_job: ScheduledSendJob | None = None
        self.common_attachments: list[dict[str, str]] = []
        self.json_job_source_paths: dict[int, str] = {}
        self.json_conflict_warned_job_ids: set[int] = set()
        self.current_runtime_task_id: int | None = None
        self.current_runtime_records: list[dict[str, Any]] = []
        self.current_runtime_source_json_path = ""
        self.current_runtime_log_path = ""
        self.last_runtime_summary: dict[str, Any] = {}
        self.basic_source_records: list[dict[str, str]] = []
        self.basic_columns: list[str] = []
        self.basic_selected_records: list[dict[str, str]] = []
        self.basic_attachments: list[dict[str, str]] = []
        self.basic_task_id: int | None = None
        self.basic_match_field = DEFAULT_SEND_TARGET_COLUMN
        self.basic_match_keyword = ""
        self.basic_last_match_total = 0
        self.basic_last_duplicate_removed = 0
        self.basic_last_loaded_path = ""
        self.current_send_origin = "classic"
        self.current_send_batch_limit: int | None = None
        self.current_send_remaining_before_start = 0
        self._compact_ui_mode = False
        self._startup_layout_refreshed = False
        self._updating_preview_table = False
        self._is_restoring_state = False
        self.basic_section_groups: dict[str, QGroupBox] = {}
        self.basic_section_title_labels: dict[str, QLabel] = {}
        self.basic_section_toggle_buttons: dict[str, QToolButton] = {}
        self.basic_section_content_widgets: dict[str, QWidget] = {}
        self._registered_splitters: dict[str, QSplitter] = {}
        self._splitter_default_sizes: dict[str, list[int]] = {}
        self._theme_mode = str(self.config.get("settings", {}).get("theme_mode") or THEME_MODE_AUTO)
        self._resolved_theme = THEME_MODE_LIGHT
        self._theme_tokens = dict(THEME_PALETTES[THEME_MODE_LIGHT])
        self.template_change_timer = QTimer(self)
        self.template_change_timer.setSingleShot(True)
        self.template_change_timer.timeout.connect(self.apply_template_changes)
        self.scheduler_timer = QTimer(self)
        self.scheduler_timer.setInterval(5000)
        self.scheduler_timer.timeout.connect(self.poll_scheduled_jobs)
        self.theme_timer = QTimer(self)
        self.theme_timer.setInterval(60_000)
        self.theme_timer.timeout.connect(self.on_theme_timer_timeout)
        self.splitter_state_save_timer = QTimer(self)
        self.splitter_state_save_timer.setSingleShot(True)
        self.splitter_state_save_timer.setInterval(250)
        self.splitter_state_save_timer.timeout.connect(self.save_registered_splitter_states)

        self.init_ui()
        self.restore_initial_state()
        if start_scheduler:
            self.scheduler_timer.start()
        self.theme_timer.start()

    def load_config(self) -> dict:
        config_exists = os.path.exists(self.config_path)
        if config_exists:
            with open(self.config_path, "r", encoding="utf-8") as file:
                config = json.load(file)
        else:
            config = {}

        changed = False
        settings = config.setdefault("settings", {})
        if "language" not in settings:
            settings["language"] = "zh-CN"
            changed = True
        if "send_interval" not in settings:
            settings["send_interval"] = 1
            changed = True
        if "theme_mode" not in settings:
            settings["theme_mode"] = THEME_MODE_AUTO
            changed = True

        excel_config = config.setdefault("excel", {})
        if "path" not in excel_config:
            excel_config["path"] = ""
            changed = True
        if "send_target_column" not in excel_config:
            excel_config["send_target_column"] = DEFAULT_SEND_TARGET_COLUMN
            changed = True

        template_config = config.setdefault("template", {})
        if "text" not in template_config:
            template_config["text"] = ""
            changed = True
        if "common_attachments" not in template_config:
            template_config["common_attachments"] = []
            changed = True

        filter_config = config.setdefault("filter", {})
        if "fields" not in filter_config:
            filter_config["fields"] = "场景"
            changed = True
        if "pattern" not in filter_config:
            filter_config["pattern"] = ""
            changed = True
        if "ignore_case" not in filter_config:
            filter_config["ignore_case"] = True
            changed = True

        local_store_config = config.setdefault("local_store", {})
        if "db_path" not in local_store_config:
            local_store_config["db_path"] = str(DEFAULT_LOCAL_DB_PATH)
            changed = True
        elif str(local_store_config["db_path"]).replace("\\", "/") == "build/easychat_local.sqlite3":
            local_store_config["db_path"] = str(DEFAULT_LOCAL_DB_PATH)
            changed = True

        bulk_send_config = config.setdefault("bulk_send", {})
        if "random_delay_min" not in bulk_send_config:
            bulk_send_config["random_delay_min"] = 30
            changed = True
        if "random_delay_max" not in bulk_send_config:
            bulk_send_config["random_delay_max"] = 180
            changed = True
        if "operator_name" not in bulk_send_config:
            bulk_send_config["operator_name"] = ""
            changed = True
        if "report_to" not in bulk_send_config:
            bulk_send_config["report_to"] = DEFAULT_REPORT_TARGET
            changed = True
        if "auto_report_enabled" not in bulk_send_config:
            bulk_send_config["auto_report_enabled"] = True
            changed = True
        if "send_mode" not in bulk_send_config:
            bulk_send_config["send_mode"] = "immediate"
            changed = True
        if "debug_mode_enabled" not in bulk_send_config:
            bulk_send_config["debug_mode_enabled"] = False
            changed = True
        if "stop_on_error" not in bulk_send_config:
            bulk_send_config["stop_on_error"] = True
            changed = True
        if "schedule_mode" not in bulk_send_config:
            bulk_send_config["schedule_mode"] = SCHEDULE_MODE_ONCE
            changed = True
        if "schedule_value" not in bulk_send_config:
            bulk_send_config["schedule_value"] = ""
            changed = True

        json_task_config = config.setdefault("json_tasks", {})
        if "last_import_dir" not in json_task_config:
            json_task_config["last_import_dir"] = ""
            changed = True
        if "last_export_dir" not in json_task_config:
            json_task_config["last_export_dir"] = ""
            changed = True
        if "last_attachment_dir" not in json_task_config:
            json_task_config["last_attachment_dir"] = ""
            changed = True

        basic_mode_config = config.setdefault("basic_mode", {})
        if "message_text" not in basic_mode_config:
            basic_mode_config["message_text"] = ""
            changed = True
        if "attachments" not in basic_mode_config:
            basic_mode_config["attachments"] = []
            changed = True
        if "match_keyword" not in basic_mode_config:
            basic_mode_config["match_keyword"] = ""
            changed = True
        if "match_field" not in basic_mode_config:
            basic_mode_config["match_field"] = DEFAULT_SEND_TARGET_COLUMN
            changed = True
        if "batch_limit" not in basic_mode_config:
            basic_mode_config["batch_limit"] = 50
            changed = True

        ui_config = config.setdefault("ui", {})
        if "nav_page" not in ui_config:
            ui_config["nav_page"] = PAGE_KEY_WORKBENCH
            changed = True
        if "workbench_view" not in ui_config:
            ui_config["workbench_view"] = WORKBENCH_VIEW_BASIC
            changed = True
        if "advanced_settings_expanded" not in ui_config:
            ui_config["advanced_settings_expanded"] = False
            changed = True
        if "splitter_sizes" not in ui_config or not isinstance(ui_config.get("splitter_sizes"), dict):
            ui_config["splitter_sizes"] = {}
            changed = True
        if "splitter_layout_version" not in ui_config:
            ui_config["splitter_layout_version"] = 1 if config_exists else CURRENT_SPLITTER_LAYOUT_VERSION
            changed = True

        if changed:
            try:
                with open(self.config_path, "w", encoding="utf-8") as file:
                    json.dump(config, file, indent=4, ensure_ascii=False)
            except OSError:
                pass

        return config

    def save_config(self) -> None:
        try:
            with open(self.config_path, "w", encoding="utf-8") as file:
                json.dump(self.config, file, indent=4, ensure_ascii=False)
        except OSError:
            pass

    def save_config_if_ready(self) -> None:
        if self._is_restoring_state:
            return
        self.save_config()

    def init_ui(self) -> None:
        self.setWindowTitle("EasyChat 精准群发")
        self.setWindowFlag(Qt.WindowMinimizeButtonHint, True)
        self.setWindowFlag(Qt.WindowMaximizeButtonHint, True)
        self.resize(1638, 1092)
        self.setMinimumSize(1080, 760)
        self.apply_font_scaling()

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(12, 12, 12, 12)
        root_layout.setSpacing(10)

        self.basic_page = self.build_basic_page()
        self.data_template_page = self.build_data_template_page()
        self.local_store_page = self.build_local_store_page()
        self.send_prepare_page = self.build_send_prepare_page()
        self.task_center_page = self.build_task_center_page()
        self.workbench_page = self.build_workbench_page()

        self.main_tabs = QTabWidget(self)
        self.main_tabs.setDocumentMode(True)
        self.main_tabs.tabBar().hide()
        self.main_tabs.addTab(self.workbench_page, "工作台")
        self.main_tabs.addTab(self.data_template_page, "数据与模板")
        self.main_tabs.addTab(self.local_store_page, "本地库数据")
        self.main_tabs.addTab(self.task_center_page, "任务工作区")

        body_layout = QHBoxLayout()
        body_layout.setSpacing(12)
        self.navigation_panel = self.build_navigation_panel()
        body_layout.addWidget(self.navigation_panel)
        body_layout.addWidget(self.main_tabs, stretch=1)
        root_layout.addLayout(body_layout, stretch=1)
        self.apply_theme()
        self.update_compact_ui_mode()
        self.navigate_to(PAGE_KEY_WORKBENCH, WORKBENCH_VIEW_BASIC, persist=False)

    def apply_font_scaling(self) -> None:
        base_font = QFont(self.font())
        base_font.setPointSize(PRIMARY_UI_FONT_SIZE)
        app = QApplication.instance()
        if app is not None:
            app.setFont(base_font)
        self.setFont(base_font)

    def build_helper_font(self, point_size: int = HELPER_UI_FONT_SIZE) -> QFont:
        helper_font = QFont(self.font())
        helper_font.setPointSize(point_size)
        return helper_font

    def style_helper_label(
        self,
        label: QLabel,
        *,
        color: str | None = None,
        point_size: int = HELPER_UI_FONT_SIZE,
    ) -> QLabel:
        label.setWordWrap(True)
        label.setFont(self.build_helper_font(point_size))
        label.setProperty("themeStyleRole", "helper")
        label.setProperty("themeTone", self.resolve_semantic_tone(color))
        self.apply_semantic_widget_style(label)
        return label

    def build_emphasis_font(self, point_size: int = PRIMARY_UI_FONT_SIZE, *, bold: bool = True) -> QFont:
        emphasis_font = QFont(self.font())
        emphasis_font.setPointSize(point_size)
        emphasis_font.setBold(bold)
        return emphasis_font

    def style_section_title_label(self, label: QLabel) -> QLabel:
        label.setFont(self.build_emphasis_font(point_size=11, bold=True))
        label.setProperty("themeStyleRole", "section-title")
        label.setProperty("themeTone", "title")
        self.apply_semantic_widget_style(label)
        return label

    def style_overview_label(self, label: QLabel) -> QLabel:
        label.setWordWrap(True)
        label.setFont(self.build_emphasis_font(point_size=11, bold=True))
        label.setProperty("themeStyleRole", "overview")
        label.setProperty("themeTone", "title")
        self.apply_semantic_widget_style(label)
        return label

    def style_status_badge(self, label: QLabel) -> QLabel:
        label.setAlignment(Qt.AlignCenter)
        label.setMinimumWidth(72)
        label.setMinimumHeight(40)
        label.setFont(self.build_helper_font())
        label.setProperty("themeStyleRole", "status-badge")
        label.setProperty("themeTone", "default")
        self.apply_semantic_widget_style(label)
        return label

    def style_empty_state_label(self, label: QLabel, *, tone: str = "muted", role: str = "empty-state") -> QLabel:
        label.setWordWrap(True)
        label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        label.setProperty("themeStyleRole", role)
        label.setProperty("themeTone", tone)
        self.apply_semantic_widget_style(label)
        return label

    def build_panel_card(self, parent: QWidget | None = None) -> QFrame:
        card = QFrame(parent or self)
        card.setFrameShape(QFrame.NoFrame)
        card.setProperty("themeStyleRole", "panel-card")
        card.setProperty("themeTone", "default")
        self.apply_semantic_widget_style(card)
        return card

    def set_button_role(
        self,
        button: QPushButton,
        role: str,
        *,
        min_width: int = 0,
        min_height: int = 0,
        compact: bool = True,
    ) -> QPushButton:
        button.setProperty("role", role)
        if min_width > 0 and not compact:
            button.setMinimumWidth(min_width)
        if min_height > 0:
            button.setMinimumHeight(min_height)
        button.setSizePolicy(QSizePolicy.Fixed if compact else QSizePolicy.Expanding, QSizePolicy.Fixed)
        button.style().unpolish(button)
        button.style().polish(button)
        if compact:
            button.setMinimumWidth(0)
            button.adjustSize()
        button.update()
        return button

    def build_separator(self) -> QFrame:
        line = QFrame(self)
        line.setFrameShape(QFrame.HLine)
        line.setFrameShadow(QFrame.Sunken)
        line.setProperty("themeStyleRole", "separator")
        self.apply_semantic_widget_style(line)
        return line

    def configure_splitter_pane(
        self,
        widget: QWidget,
        *,
        min_height: int = 0,
        min_width: int = 0,
        vertical_policy: QSizePolicy.Policy = QSizePolicy.Expanding,
    ) -> QWidget:
        widget.setProperty("splitterMinHeight", max(0, int(min_height)))
        widget.setProperty("splitterMinWidth", max(0, int(min_width)))
        if min_height > 0:
            widget.setMinimumHeight(min_height)
        if min_width > 0:
            widget.setMinimumWidth(min_width)
        widget.setSizePolicy(QSizePolicy.Preferred, vertical_policy)
        return widget

    def build_section_panel(
        self,
        *,
        parent: QWidget,
        title: str,
        hint: str | None = None,
        content: QWidget | None = None,
        spacing: int = 8,
    ) -> QWidget:
        panel = QFrame(parent)
        panel.setFrameShape(QFrame.NoFrame)
        panel.setProperty("themeStyleRole", "section-panel")
        panel.setProperty("themeTone", "default")
        self.apply_semantic_widget_style(panel)

        layout = QVBoxLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(spacing)

        title_label = QLabel(title, panel)
        self.style_section_title_label(title_label)
        layout.addWidget(title_label)

        divider = QFrame(panel)
        divider.setFrameShape(QFrame.HLine)
        divider.setFrameShadow(QFrame.Sunken)
        divider.setProperty("themeStyleRole", "separator")
        self.apply_semantic_widget_style(divider)
        layout.addWidget(divider)

        if hint:
            hint_label = QLabel(hint, panel)
            hint_label.setWordWrap(True)
            self.style_helper_label(hint_label, color="#555")
            layout.addWidget(hint_label)

        if content is not None:
            layout.addWidget(content, stretch=1)
        return panel

    def build_splitter(
        self,
        orientation: Qt.Orientation,
        widgets: list[QWidget],
        *,
        parent: QWidget,
        stretch_factors: list[int] | None = None,
        splitter_key: str | None = None,
        default_sizes: list[int] | None = None,
    ) -> QSplitter:
        splitter = QSplitter(orientation, parent)
        splitter.setHandleWidth(6)
        splitter.setOpaqueResize(True)
        splitter.setChildrenCollapsible(False)
        for index, widget in enumerate(widgets):
            splitter.addWidget(widget)
            if stretch_factors and index < len(stretch_factors):
                splitter.setStretchFactor(index, stretch_factors[index])
        if splitter_key:
            self.register_splitter_state(splitter_key, splitter, default_sizes=default_sizes)
        elif default_sizes:
            splitter.setProperty("defaultSizes", list(default_sizes))
        return splitter

    def register_splitter_state(
        self,
        splitter_key: str,
        splitter: QSplitter,
        *,
        default_sizes: list[int] | None = None,
    ) -> None:
        self._registered_splitters[splitter_key] = splitter
        if default_sizes:
            self._splitter_default_sizes[splitter_key] = [max(0, int(size)) for size in default_sizes]
        splitter.splitterMoved.connect(lambda _pos, _index, key=splitter_key: self.on_splitter_moved(key))

    def on_splitter_moved(self, _splitter_key: str) -> None:
        if self._is_restoring_state:
            return
        self.splitter_state_save_timer.start()

    def save_registered_splitter_states(self) -> None:
        ui_config = self.config.setdefault("ui", {})
        splitter_sizes = ui_config.setdefault("splitter_sizes", {})
        for splitter_key, splitter in self._registered_splitters.items():
            sizes = [max(0, int(size)) for size in splitter.sizes()]
            if len(sizes) == splitter.count():
                splitter_sizes[splitter_key] = sizes
        self.save_config_if_ready()

    def restore_registered_splitter_states(self) -> None:
        ui_config = self.config.setdefault("ui", {})
        raw_states = ui_config.get("splitter_sizes") if isinstance(ui_config.get("splitter_sizes"), dict) else {}
        layout_version = int(ui_config.get("splitter_layout_version") or 0)
        migrated = False
        for splitter_key, splitter in self._registered_splitters.items():
            target_sizes: list[int] | None = None
            raw_sizes = raw_states.get(splitter_key) if isinstance(raw_states, dict) else None
            if isinstance(raw_sizes, list) and len(raw_sizes) == splitter.count():
                try:
                    target_sizes = [max(0, int(size)) for size in raw_sizes]
                except (TypeError, ValueError):
                    target_sizes = None
            if splitter_key == "workbench.basic.left" and layout_version < CURRENT_SPLITTER_LAYOUT_VERSION:
                default_sizes = self._splitter_default_sizes.get(splitter_key)
                if default_sizes and len(default_sizes) == splitter.count():
                    target_sizes = list(default_sizes)
                    migrated = True
            if target_sizes is None:
                default_sizes = self._splitter_default_sizes.get(splitter_key)
                if default_sizes and len(default_sizes) == splitter.count():
                    target_sizes = list(default_sizes)
            if target_sizes:
                splitter.setSizes(target_sizes)
        if layout_version < CURRENT_SPLITTER_LAYOUT_VERSION:
            ui_config["splitter_layout_version"] = CURRENT_SPLITTER_LAYOUT_VERSION
            if migrated:
                self.save_registered_splitter_states()
            else:
                self.save_config_if_ready()

    def resolve_semantic_tone(self, color: str | None) -> str:
        if not color:
            return "default"
        return SEMANTIC_COLOR_MAP.get(str(color).strip().lower(), "muted")

    def set_label_tone(self, label: QLabel, tone: str) -> None:
        label.setProperty("themeTone", tone)
        self.apply_semantic_widget_style(label)

    def apply_semantic_widget_style(self, widget: QWidget) -> None:
        role = str(widget.property("themeStyleRole") or "")
        tone = str(widget.property("themeTone") or "default")
        tokens = self._theme_tokens
        if role == "helper":
            color = {
                "muted": tokens["text_muted"],
                "success": tokens["success"],
                "warning": tokens["warning"],
                "danger": tokens["danger"],
                "title": tokens["text_primary"],
                "default": tokens["text_secondary"],
            }.get(tone, tokens["text_secondary"])
            widget.setStyleSheet(f"color:{color}; background: transparent;")
            return
        if role == "section-title":
            widget.setStyleSheet(f"color:{tokens['text_primary']}; background: transparent;")
            return
        if role == "overview":
            widget.setStyleSheet(f"color:{tokens['text_primary']}; background: transparent;")
            return
        if role == "status-badge":
            widget.setStyleSheet(
                f"color:{tokens['text_primary']};"
                f"background:{tokens['badge_bg']};"
                f"border:1px solid {tokens['badge_border']};"
                "border-radius:8px;"
                "padding:6px 12px;"
            )
            return
        if role == "panel-card":
            widget.setStyleSheet(
                f"background:{tokens['panel_alt_bg']};"
                f"border:1px solid {tokens['border']};"
                "border-radius:12px;"
            )
            return
        if role == "section-panel":
            widget.setStyleSheet(
                f"background:{tokens['panel_bg']};"
                f"border:1px solid {tokens['border_strong']};"
                "border-radius:12px;"
            )
            return
        if role == "empty-state":
            color = {
                "muted": tokens["text_muted"],
                "warning": tokens["warning"],
                "danger": tokens["danger"],
                "success": tokens["success"],
                "default": tokens["text_secondary"],
            }.get(tone, tokens["text_secondary"])
            border_color = {
                "warning": tokens["warning"],
                "danger": tokens["danger"],
                "success": tokens["success"],
            }.get(tone, tokens["border"])
            widget.setStyleSheet(
                f"color:{color};"
                f"background:{tokens['panel_alt_bg']};"
                f"border:1px dashed {border_color};"
                "border-radius:10px;"
                "padding:18px 16px;"
            )
            return
        if role == "section-empty":
            color = {
                "muted": tokens["text_muted"],
                "warning": tokens["warning"],
                "danger": tokens["danger"],
                "success": tokens["success"],
                "default": tokens["text_secondary"],
            }.get(tone, tokens["text_secondary"])
            border_color = {
                "warning": tokens["warning"],
                "danger": tokens["danger"],
                "success": tokens["success"],
                "default": tokens["border"],
            }.get(tone, tokens["border"])
            widget.setStyleSheet(
                f"color:{color};"
                "background: transparent;"
                f"border:1px dashed {border_color};"
                "border-radius:10px;"
                "padding:12px 14px;"
            )
            return
        if role == "separator":
            widget.setStyleSheet(f"color:{tokens['separator']}; background:{tokens['separator']};")
            return

    def apply_table_header_font(self, table: QTableWidget | None) -> None:
        if table is None:
            return
        header = table.horizontalHeader()
        header_font = QFont(self.font())
        header_font.setPointSize(max(self.font().pointSize(), 11))
        header_font.setBold(True)
        header.setFont(header_font)
        header.style().unpolish(header)
        header.style().polish(header)
        header.update()

    def configure_resizable_table_columns(
        self,
        table: QTableWidget | None,
        *,
        initial_widths: list[int] | tuple[int, ...] | None = None,
        signature: str | None = None,
        min_section_size: int = 56,
        auto_fit_on_signature_change: bool = False,
        max_auto_width: int = 360,
        auto_fit_padding: int = 18,
    ) -> None:
        if table is None:
            return
        header = table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setMinimumSectionSize(max(40, int(min_section_size)))
        for index in range(table.columnCount()):
            header.setSectionResizeMode(index, QHeaderView.Interactive)

        resolved_signature = signature or "|".join(
            table.horizontalHeaderItem(index).text() if table.horizontalHeaderItem(index) else str(index)
            for index in range(table.columnCount())
        )
        previous_signature = str(table.property("columnWidthSignature") or "")
        should_apply_initial_widths = previous_signature != resolved_signature

        if should_apply_initial_widths:
            if initial_widths:
                for index, width in enumerate(initial_widths):
                    if index >= table.columnCount():
                        break
                    if width:
                        table.setColumnWidth(index, max(int(width), header.minimumSectionSize()))
            elif auto_fit_on_signature_change and table.columnCount() > 0:
                table.resizeColumnsToContents()
                for index in range(table.columnCount()):
                    fitted_width = min(
                        max(table.columnWidth(index) + int(auto_fit_padding), header.minimumSectionSize()),
                        int(max_auto_width),
                    )
                    table.setColumnWidth(index, fitted_width)

        table.setProperty("columnWidthSignature", resolved_signature)

    def resolve_theme_mode(self) -> str:
        mode = str(self._theme_mode or THEME_MODE_AUTO).strip().lower()
        if mode not in {THEME_MODE_AUTO, THEME_MODE_LIGHT, THEME_MODE_DARK}:
            mode = THEME_MODE_AUTO
        if mode == THEME_MODE_LIGHT:
            return THEME_MODE_LIGHT
        if mode == THEME_MODE_DARK:
            return THEME_MODE_DARK
        hour = datetime.now().hour
        if hour >= AUTO_THEME_DARK_START_HOUR or hour < AUTO_THEME_LIGHT_START_HOUR:
            return THEME_MODE_DARK
        return THEME_MODE_LIGHT

    def build_app_stylesheet(self, tokens: dict[str, str]) -> str:
        return f"""
            QWidget {{
                background-color: {tokens['window_bg']};
                color: {tokens['text_primary']};
            }}
            QWidget#navigationPanel {{
                background-color: {tokens['panel_bg']};
                border: 1px solid {tokens['border']};
                border-radius: 14px;
            }}
            QTabWidget::pane {{
                border: 1px solid {tokens['border']};
                background: {tokens['panel_bg']};
                border-radius: 14px;
                top: -1px;
            }}
            QTabBar::tab {{
                background: {tokens['tab_bg']};
                color: {tokens['text_secondary']};
                min-height: 40px;
                padding: 6px 18px;
                border: 1px solid {tokens['border']};
                border-bottom: none;
                border-top-left-radius: 10px;
                border-top-right-radius: 10px;
                margin-right: 4px;
            }}
            QTabBar::tab:selected {{
                background: {tokens['tab_active_bg']};
                color: {tokens['text_primary']};
                border-color: {tokens['tab_active_border']};
            }}
            QPushButton {{
                font-size: 11pt;
                font-weight: 500;
                min-height: 38px;
                padding: 4px 12px;
                border-radius: 10px;
                border: 1px solid {tokens['secondary_border']};
                background: {tokens['secondary_bg']};
                color: {tokens['secondary_text']};
            }}
            QPushButton:hover {{
                border-color: {tokens['border_strong']};
            }}
            QPushButton:disabled {{
                background: {tokens['disabled_bg']};
                color: {tokens['disabled_text']};
                border-color: {tokens['disabled_bg']};
            }}
            QPushButton[role="primary"] {{
                background-color: {tokens['primary']};
                color: {tokens['text_inverse']};
                border: 1px solid {tokens['primary']};
                font-weight: 600;
            }}
            QPushButton[role="primary"]:hover {{
                background-color: {tokens['primary_hover']};
                border-color: {tokens['primary_hover']};
            }}
            QPushButton[role="danger"] {{
                background-color: {tokens['danger_bg']};
                color: {tokens['danger_text']};
                border: 1px solid {tokens['danger_border']};
            }}
            QPushButton[role="secondary"] {{
                background-color: {tokens['secondary_bg']};
                color: {tokens['secondary_text']};
                border: 1px solid {tokens['secondary_border']};
            }}
            QPushButton[role="neutral"] {{
                background-color: {tokens['neutral_bg']};
                color: {tokens['neutral_text']};
                border: 1px solid {tokens['neutral_border']};
            }}
            QPushButton[role="nav"] {{
                background-color: transparent;
                color: {tokens['text_secondary']};
                border: 1px solid transparent;
                text-align: left;
                padding: 8px 12px;
                font-weight: 600;
            }}
            QPushButton[role="nav"]:hover {{
                background-color: {tokens['tab_bg']};
                border-color: {tokens['border']};
            }}
            QPushButton[role="nav"]:checked {{
                background-color: {tokens['tab_active_bg']};
                color: {tokens['text_primary']};
                border-color: {tokens['tab_active_border']};
            }}
            QPushButton[role="subnav"] {{
                background-color: {tokens['secondary_bg']};
                color: {tokens['secondary_text']};
                border: 1px solid {tokens['secondary_border']};
                min-height: 34px;
                padding: 4px 10px;
            }}
            QPushButton[role="subnav"]:checked {{
                background-color: {tokens['primary']};
                color: {tokens['text_inverse']};
                border-color: {tokens['primary']};
            }}
            QSpinBox, QLineEdit, QDateTimeEdit, QComboBox, QPlainTextEdit {{
                min-height: 34px;
                background: {tokens['input_bg']};
                color: {tokens['text_primary']};
                border: 1px solid {tokens['input_border']};
                border-radius: 10px;
                selection-background-color: {tokens['selection_bg']};
                selection-color: {tokens['selection_text']};
            }}
            QComboBox QAbstractItemView {{
                background: {tokens['panel_bg']};
                color: {tokens['text_primary']};
                selection-background-color: {tokens['selection_bg']};
                selection-color: {tokens['selection_text']};
                border: 1px solid {tokens['input_border']};
            }}
            QGroupBox {{
                font-weight: 600;
                border: 1px solid {tokens['border']};
                border-radius: 14px;
                margin-top: 12px;
                padding: 12px;
                background: {tokens['panel_bg']};
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: 14px;
                padding: 0 6px;
                color: {tokens['text_primary']};
                background: {tokens['panel_bg']};
            }}
            QGroupBox[collapsibleSection="true"] {{
                margin-top: 0px;
            }}
            QGroupBox[collapsibleSection="true"]::title {{
                width: 0px;
                height: 0px;
                padding: 0px;
                margin: 0px;
            }}
            QToolButton[role="section-toggle"] {{
                background: transparent;
                color: {tokens['text_secondary']};
                border: 1px solid {tokens['border']};
                border-radius: 10px;
                padding: 4px 10px;
            }}
            QToolButton[role="section-toggle"]:hover {{
                background: {tokens['secondary_bg']};
                color: {tokens['text_primary']};
            }}
            QToolButton[role="section-toggle"]:checked {{
                background: {tokens['secondary_bg']};
                color: {tokens['text_primary']};
                border-color: {tokens['border_strong']};
            }}
            QHeaderView::section {{
                background: {tokens['table_header_bg']};
                color: {tokens['text_secondary']};
                padding: 8px 12px;
                border: none;
                border-bottom: 1px solid {tokens['border']};
                font-weight: 600;
            }}
            QHeaderView::section:hover {{
                background: {tokens['table_header_hover']};
            }}
            QTableWidget {{
                background: {tokens['input_bg']};
                color: {tokens['text_primary']};
                border: 1px solid {tokens['input_border']};
                border-radius: 10px;
                gridline-color: {tokens['separator']};
                alternate-background-color: {tokens['table_row_alt']};
                selection-background-color: {tokens['selection_bg']};
                selection-color: {tokens['selection_text']};
                outline: none;
            }}
            QTableWidget::item {{
                padding: 6px 10px;
                border: none;
                border-bottom: 1px solid {tokens['separator']};
            }}
            QTableWidget::item:selected {{
                background: {tokens['selection_bg']};
                color: {tokens['selection_text']};
            }}
            QTableWidget QTableCornerButton::section {{
                background: {tokens['table_header_bg']};
                border: none;
            }}
            QSplitter::handle {{
                background: {tokens['splitter_handle']};
            }}
            QSplitter::handle:hover {{
                background: {tokens['splitter_handle_hover']};
            }}
            QSplitter {{}}
            QScrollArea {{
                border: none;
                background: transparent;
            }}
            QCheckBox, QRadioButton {{
                color: {tokens['text_primary']};
            }}
            QMessageBox {{
                background: {tokens['panel_bg']};
            }}
        """

    def build_navigation_panel(self) -> QWidget:
        container = QWidget(self)
        container.setObjectName("navigationPanel")
        container.setMinimumWidth(168)
        container.setMaximumWidth(196)
        layout = QVBoxLayout(container)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        title = QLabel("导航", container)
        self.style_section_title_label(title)
        layout.addWidget(title)

        self.navigation_button_group = QButtonGroup(self)
        self.navigation_button_group.setExclusive(True)
        self.navigation_buttons: dict[str, QPushButton] = {}
        for page_key, label in (
            (PAGE_KEY_WORKBENCH, "工作台"),
            (PAGE_KEY_DATA_TEMPLATE, "数据与模板"),
            (PAGE_KEY_LOCAL_STORE, "本地库数据"),
            (PAGE_KEY_TASK_CENTER, "任务工作区"),
        ):
            button = QPushButton(label, container)
            button.setCheckable(True)
            button.setProperty("role", "nav")
            button.clicked.connect(lambda _checked=False, key=page_key: self.navigate_to(key))
            self.navigation_button_group.addButton(button)
            self.navigation_buttons[page_key] = button
            layout.addWidget(button)

        layout.addWidget(self.build_separator())
        layout.addWidget(self.build_theme_switcher_panel(container))
        layout.addStretch(1)
        return container

    def build_workbench_page(self) -> QWidget:
        page = QWidget(self)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        header_row = QHBoxLayout()
        header_row.setSpacing(8)
        title = QLabel("工作台")
        self.style_section_title_label(title)
        header_row.addWidget(title)
        header_row.addSpacing(8)

        self.workbench_button_group = QButtonGroup(self)
        self.workbench_button_group.setExclusive(True)
        self.workbench_buttons: dict[str, QPushButton] = {}
        for view_key, text in (
            (WORKBENCH_VIEW_BASIC, "快速发送"),
            (WORKBENCH_VIEW_SEND, "发送准备"),
        ):
            button = QPushButton(text, page)
            button.setCheckable(True)
            button.setProperty("role", "subnav")
            button.clicked.connect(lambda _checked=False, key=view_key: self.set_workbench_view(key))
            self.workbench_button_group.addButton(button)
            self.workbench_buttons[view_key] = button
            header_row.addWidget(button)
        header_row.addStretch(1)
        layout.addLayout(header_row)

        self.workbench_stack = QStackedWidget(page)
        self.workbench_stack.addWidget(self.basic_page)
        self.workbench_stack.addWidget(self.send_prepare_page)
        layout.addWidget(self.workbench_stack, stretch=1)
        return page

    def build_scroll_area(self, content: QWidget) -> QScrollArea:
        scroll_area = QScrollArea(self)
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.NoFrame)
        scroll_area.setWidget(content)
        return scroll_area

    def init_basic_collapsible_group(self, group: QGroupBox, section_key: str) -> QVBoxLayout:
        title_text = group.title().strip()
        group.setProperty("collapsibleSection", True)
        group.setTitle("")
        layout = QVBoxLayout(group)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(8)

        title_label = QLabel(title_text, group)
        self.style_section_title_label(title_label)
        header_row.addWidget(title_label, 0, Qt.AlignVCenter)
        header_row.addStretch(1)

        toggle_button = QToolButton(group)
        toggle_button.setProperty("role", "section-toggle")
        toggle_button.setAutoRaise(True)
        toggle_button.setCheckable(True)
        toggle_button.setChecked(True)
        toggle_button.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        toggle_button.toggled.connect(lambda checked, key=section_key: self.set_basic_section_expanded(key, checked))
        header_row.addWidget(toggle_button, 0, Qt.AlignVCenter)
        layout.addLayout(header_row)

        content_widget = QWidget(group)
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(10)
        layout.addWidget(content_widget)

        self.basic_section_groups[section_key] = group
        self.basic_section_title_labels[section_key] = title_label
        self.basic_section_toggle_buttons[section_key] = toggle_button
        self.basic_section_content_widgets[section_key] = content_widget
        self.apply_basic_section_state(section_key, True)
        return content_layout

    def apply_basic_section_state(self, section_key: str, expanded: bool) -> None:
        toggle_button = self.basic_section_toggle_buttons.get(section_key)
        if toggle_button is not None:
            toggle_button.blockSignals(True)
            toggle_button.setChecked(expanded)
            toggle_button.blockSignals(False)
            toggle_button.setText("收起" if expanded else "展开")
            toggle_button.setArrowType(Qt.DownArrow if expanded else Qt.RightArrow)
        content_widget = self.basic_section_content_widgets.get(section_key)
        if content_widget is not None:
            content_widget.setVisible(expanded)
        group = self.basic_section_groups.get(section_key)
        if group is not None:
            default_min_height = max(0, int(group.property("splitterMinHeight") or 0))
            if expanded:
                group.setMinimumHeight(default_min_height)
                group.setMaximumHeight(16777215)
            else:
                group.adjustSize()
                collapsed_height = max(group.sizeHint().height(), 52)
                group.setMinimumHeight(collapsed_height)
                group.setMaximumHeight(collapsed_height)

    def set_basic_section_expanded(self, section_key: str, expanded: bool) -> None:
        self.apply_basic_section_state(section_key, expanded)
        self.refresh_basic_section_layout()

    def refresh_basic_section_layout(self) -> None:
        if not hasattr(self, "basic_left_splitter") or not hasattr(self, "basic_right_splitter"):
            return
        self.apply_basic_splitter_sizes(
            self.basic_left_splitter,
            [
                ("import", getattr(self, "basic_import_group", None), 0),
                ("message", getattr(self, "basic_message_group", None), 2),
            ],
        )
        self.apply_basic_splitter_sizes(
            self.basic_right_splitter,
            [
                ("receiver", getattr(self, "basic_receiver_group", None), 3),
                ("attachment", getattr(self, "basic_attachment_group", None), 2),
                ("send", getattr(self, "basic_send_group", None), 1),
            ],
        )

        for section_key in ("import", "message", "receiver", "attachment", "send"):
            group = self.basic_section_groups.get(section_key)
            if group is not None:
                group.updateGeometry()
        if hasattr(self, "basic_page"):
            self.basic_page.updateGeometry()
            self.basic_page.adjustSize()

    def apply_basic_splitter_sizes(
        self,
        splitter: QSplitter,
        sections: list[tuple[str, QGroupBox | None, int]],
    ) -> None:
        available_height = splitter.size().height()
        if available_height <= 0:
            available_height = sum(max(0, int(group.property("splitterMinHeight") or 0)) for _, group, _ in sections if group is not None)
        sizes: list[int] = []
        expandable_indices: list[tuple[int, int]] = []
        for index, (section_key, group, expanded_stretch) in enumerate(sections):
            if group is None:
                sizes.append(0)
                continue
            content_widget = self.basic_section_content_widgets.get(section_key)
            expanded = content_widget is None or not content_widget.isHidden()
            if expanded:
                minimum = max(int(group.property("splitterMinHeight") or 0), group.sizeHint().height())
                sizes.append(minimum)
                if expanded_stretch > 0:
                    expandable_indices.append((index, expanded_stretch))
            else:
                collapsed_height = max(group.sizeHint().height(), 52)
                sizes.append(collapsed_height)

        extra_height = max(0, available_height - sum(sizes))
        if expandable_indices and extra_height > 0:
            total_weight = sum(weight for _, weight in expandable_indices)
            for index, weight in expandable_indices:
                sizes[index] += int(extra_height * weight / total_weight)
            remainder = max(0, available_height - sum(sizes))
            for index, _weight in expandable_indices:
                if remainder <= 0:
                    break
                sizes[index] += 1
                remainder -= 1
        splitter.setSizes(sizes)

    def navigate_to(self, page_key: str, workbench_view: str | None = None, *, persist: bool = True) -> None:
        page_map = {
            PAGE_KEY_WORKBENCH: self.workbench_page,
            PAGE_KEY_DATA_TEMPLATE: self.data_template_page,
            PAGE_KEY_LOCAL_STORE: self.local_store_page,
            PAGE_KEY_TASK_CENTER: self.task_center_page,
        }
        resolved_key = page_key if page_key in page_map else PAGE_KEY_WORKBENCH
        self.main_tabs.setCurrentWidget(page_map[resolved_key])
        if resolved_key == PAGE_KEY_WORKBENCH:
            self.set_workbench_view(workbench_view or WORKBENCH_VIEW_BASIC, persist=persist)
        elif resolved_key == PAGE_KEY_TASK_CENTER and hasattr(self, "schedule_table"):
            self.apply_table_header_font(self.schedule_table)
        self.sync_navigation_buttons(resolved_key)
        if persist:
            self.config["ui"]["nav_page"] = resolved_key
            self.save_config_if_ready()

    def sync_navigation_buttons(self, page_key: str) -> None:
        for key, button in getattr(self, "navigation_buttons", {}).items():
            button.blockSignals(True)
            button.setChecked(key == page_key)
            button.blockSignals(False)

    def set_workbench_view(self, view_key: str, *, persist: bool = True) -> None:
        resolved_view = view_key if view_key in {WORKBENCH_VIEW_BASIC, WORKBENCH_VIEW_SEND} else WORKBENCH_VIEW_BASIC
        if hasattr(self, "workbench_stack"):
            self.workbench_stack.setCurrentWidget(
                self.basic_page if resolved_view == WORKBENCH_VIEW_BASIC else self.send_prepare_page
            )
        if resolved_view == WORKBENCH_VIEW_SEND and hasattr(self, "preview_table"):
            self.apply_table_header_font(self.preview_table)
        for key, button in getattr(self, "workbench_buttons", {}).items():
            button.blockSignals(True)
            button.setChecked(key == resolved_view)
            button.blockSignals(False)
        if persist:
            self.config["ui"]["workbench_view"] = resolved_view
            self.save_config_if_ready()

    def build_theme_switcher_panel(self, parent: QWidget) -> QWidget:
        container = QWidget(parent)
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 2, 0, 0)
        layout.setSpacing(6)

        theme_label = QLabel("主题颜色", container)
        self.style_helper_label(theme_label, color="#555")
        layout.addWidget(theme_label)

        self.theme_mode_combo = QComboBox(container)
        self.theme_mode_combo.addItem("自动", THEME_MODE_AUTO)
        self.theme_mode_combo.addItem("浅色", THEME_MODE_LIGHT)
        self.theme_mode_combo.addItem("深色", THEME_MODE_DARK)
        self.theme_mode_combo.currentIndexChanged.connect(self.on_theme_mode_changed)
        layout.addWidget(self.theme_mode_combo)

        self.theme_status_label = QLabel("当前主题：浅色", container)
        self.style_helper_label(self.theme_status_label, color="#555")
        layout.addWidget(self.theme_status_label)
        return container

    def apply_theme(self) -> None:
        self._resolved_theme = self.resolve_theme_mode()
        self._theme_tokens = dict(THEME_PALETTES[self._resolved_theme])
        app = QApplication.instance()
        stylesheet = self.build_app_stylesheet(self._theme_tokens)
        if app is not None:
            app.setStyleSheet(stylesheet)
        else:
            self.setStyleSheet(stylesheet)
        self.refresh_theme_dependent_widgets()

    def refresh_theme_dependent_widgets(self) -> None:
        for child in self.findChildren(QWidget):
            if child.property("themeStyleRole"):
                self.apply_semantic_widget_style(child)
        for table_name in (
            "preview_table",
            "schedule_table",
            "basic_selected_table",
            "basic_attachment_table",
            "common_attachment_table",
        ):
            table = getattr(self, table_name, None)
            if isinstance(table, QTableWidget):
                self.apply_table_header_font(table)
        if hasattr(self, "local_store_views"):
            for view_refs in self.local_store_views.values():
                table = view_refs.get("table")
                if isinstance(table, QTableWidget):
                    self.apply_table_header_font(table)
        if hasattr(self, "theme_status_label"):
            resolved_label = "深色" if self._resolved_theme == THEME_MODE_DARK else "浅色"
            mode_label = {
                THEME_MODE_AUTO: "自动",
                THEME_MODE_LIGHT: "浅色",
                THEME_MODE_DARK: "深色",
            }.get(self._theme_mode, "自动")
            self.theme_status_label.setText(f"当前主题：{resolved_label}（{mode_label}）")

    def on_theme_mode_changed(self, _index: int) -> None:
        if not hasattr(self, "theme_mode_combo"):
            return
        self._theme_mode = str(self.theme_mode_combo.currentData() or THEME_MODE_AUTO)
        self.config["settings"]["theme_mode"] = self._theme_mode
        self.save_config_if_ready()
        self.apply_theme()

    def on_theme_timer_timeout(self) -> None:
        if self._theme_mode == THEME_MODE_AUTO:
            resolved = self.resolve_theme_mode()
            if resolved != self._resolved_theme:
                self.apply_theme()

    def is_debug_mode_enabled(self) -> bool:
        return bool(hasattr(self, "debug_mode_button") and self.debug_mode_button.isChecked())

    def is_stop_on_error_enabled(self) -> bool:
        return bool(hasattr(self, "stop_on_error_checkbox") and self.stop_on_error_checkbox.isChecked())

    def update_debug_mode_button_text(self) -> None:
        if not hasattr(self, "debug_mode_button"):
            return
        if self._compact_ui_mode:
            self.debug_mode_button.setText("调试：开" if self.debug_mode_button.isChecked() else "调试：关")
        else:
            self.debug_mode_button.setText("调试模式：开" if self.debug_mode_button.isChecked() else "调试模式：关")
        self.debug_mode_button.setToolTip("开启后会自动定位联系人并预填消息草稿，但不会按回车发送；附件与自动汇报也不会真实发送。")

    def on_debug_mode_toggled(self, checked: bool) -> None:
        self.config["bulk_send"]["debug_mode_enabled"] = bool(checked)
        self.update_debug_mode_button_text()
        self.save_config_if_ready()

    def on_advanced_settings_toggled(self, checked: bool) -> None:
        self.update_advanced_settings_panel(bool(checked))
        self.config["ui"]["advanced_settings_expanded"] = bool(checked)
        self.save_config_if_ready()

    def update_advanced_settings_panel(self, expanded: bool) -> None:
        if not hasattr(self, "advanced_settings_toggle_button") or not hasattr(self, "advanced_settings_panel"):
            return
        self.advanced_settings_toggle_button.blockSignals(True)
        self.advanced_settings_toggle_button.setChecked(expanded)
        self.advanced_settings_toggle_button.blockSignals(False)
        self.advanced_settings_toggle_button.setText("收起高级设置" if expanded else "展开高级设置")
        self.advanced_settings_toggle_button.setArrowType(Qt.DownArrow if expanded else Qt.RightArrow)
        self.advanced_settings_panel.setVisible(expanded)

    def update_compact_ui_mode(self) -> None:
        compact = self.width() < 1260
        self._compact_ui_mode = compact
        if hasattr(self, "theme_status_label"):
            self.theme_status_label.setVisible(self.width() >= 1180)
        if hasattr(self, "preview_button"):
            self.preview_button.setText("刷新" if compact else "刷新发送计划")
        if hasattr(self, "import_json_button"):
            self.import_json_button.setText("导入" if compact else "导入 JSON（多选）")
        if hasattr(self, "export_json_button"):
            self.export_json_button.setText("导出" if compact else "导出 JSON")
        if hasattr(self, "continue_button"):
            self.continue_button.setText("续发" if compact else "继续发送")
        if hasattr(self, "stop_button"):
            self.stop_button.setText("停止")
        if hasattr(self, "refresh_schedule_button"):
            self.refresh_schedule_button.setText("刷新" if compact else "刷新任务列表")
        if hasattr(self, "preview_schedule_button"):
            self.preview_schedule_button.setText("预览" if compact else "预览选中任务")
        if hasattr(self, "enable_schedule_button"):
            self.enable_schedule_button.setText("开启" if compact else "开启自动调度")
        if hasattr(self, "disable_schedule_button"):
            self.disable_schedule_button.setText("关闭" if compact else "关闭自动调度")
        if hasattr(self, "delete_schedule_button"):
            self.delete_schedule_button.setText("删除" if compact else "删除队列记录")
        if hasattr(self, "cancel_schedule_button"):
            self.cancel_schedule_button.setText("取消" if compact else "取消选中任务")
        if hasattr(self, "basic_match_button"):
            self.basic_match_button.setText("预览" if compact else "预览结果")
        if hasattr(self, "basic_start_button") and self.current_send_origin != "basic":
            self.update_basic_progress_status()
        if hasattr(self, "navigation_panel"):
            self.navigation_panel.setMaximumWidth(180 if compact else 196)
        if hasattr(self, "send_status_label"):
            self.send_status_label.setMinimumWidth(64 if compact else 72)
        self.update_debug_mode_button_text()
        if hasattr(self, "start_button"):
            self.update_action_button_state()

    def build_settings_group(self) -> QGroupBox:
        group = QGroupBox("基础设置")
        layout = QVBoxLayout(group)
        layout.setSpacing(12)

        self.wechat_notice_btn = QPushButton("查看微信启动说明", group)
        self.wechat_notice_btn.clicked.connect(self.show_wechat_open_notice)
        layout.addWidget(self.wechat_notice_btn)
        layout.addLayout(self.init_language_choose())

        tip_label = QLabel("建议先确认微信快捷键为 Ctrl+Alt+W，再进行 Excel 读取和批量发送。")
        self.style_helper_label(tip_label, color="#555")
        layout.addWidget(tip_label)
        return group

    def build_data_template_page(self) -> QWidget:
        page = QWidget(self)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        self.data_template_splitter = self.build_splitter(
            Qt.Horizontal,
            [self.build_excel_group(), self.build_template_group()],
            parent=page,
            stretch_factors=[4, 5],
            splitter_key="data_template.main",
            default_sizes=[520, 640],
        )
        layout.addWidget(self.data_template_splitter, stretch=1)
        return page

    def build_basic_page(self) -> QWidget:
        page = QWidget(self)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        left_panel = QWidget(page)
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(10)
        self.basic_import_group = self.build_basic_import_group()
        self.basic_message_group = self.build_basic_message_group()
        self.basic_left_splitter = self.build_splitter(
            Qt.Vertical,
            [self.basic_import_group, self.basic_message_group],
            parent=left_panel,
            stretch_factors=[0, 1],
            splitter_key="workbench.basic.left",
            default_sizes=[220, 760],
        )
        left_layout.addWidget(self.basic_left_splitter, stretch=1)

        right_panel = QWidget(page)
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(10)
        self.basic_receiver_group = self.build_basic_receiver_group()
        self.basic_attachment_group = self.build_basic_attachment_group()
        self.basic_send_group = self.build_basic_send_group()
        self.basic_right_splitter = self.build_splitter(
            Qt.Vertical,
            [self.basic_receiver_group, self.basic_attachment_group, self.basic_send_group],
            parent=right_panel,
            stretch_factors=[3, 2, 1],
            splitter_key="workbench.basic.right",
            default_sizes=[300, 180, 170],
        )
        right_layout.addWidget(self.basic_right_splitter, stretch=1)

        self.basic_splitter = self.build_splitter(
            Qt.Horizontal,
            [self.build_scroll_area(left_panel), self.build_scroll_area(right_panel)],
            parent=page,
            stretch_factors=[4, 5],
            splitter_key="workbench.basic.main",
            default_sizes=[520, 650],
        )
        layout.addWidget(self.basic_splitter, stretch=1)
        self.refresh_basic_section_layout()
        return page

    def build_basic_intro_group(self) -> QGroupBox:
        group = QGroupBox("普通用户快捷入口")
        layout = QVBoxLayout(group)
        layout.setSpacing(8)
        title = QLabel("按步骤完成一次安全、可暂停的微信群发。")
        self.style_overview_label(title)
        layout.addWidget(title)
        return group

    def build_basic_import_group(self) -> QGroupBox:
        group = QGroupBox("1. 导入数据")
        layout = self.init_basic_collapsible_group(group, "import")
        self.configure_splitter_pane(group, min_height=150, vertical_policy=QSizePolicy.Preferred)

        path_layout = QHBoxLayout()
        self.basic_excel_path_input = FileDropLineEdit(
            suffixes=[".xlsx", ".xls", ".csv"],
            parent=self,
        )
        self.basic_excel_path_input.setPlaceholderText("选择或拖入 Excel 文件（支持 .xlsx / .xls / .csv）")
        self.basic_excel_path_input.textChanged.connect(self.on_basic_excel_path_changed)
        path_layout.addWidget(self.basic_excel_path_input)

        choose_button = QPushButton("选择文件")
        choose_button.clicked.connect(self.select_basic_excel_file)
        self.set_button_role(choose_button, "secondary", min_width=120)
        path_layout.addWidget(choose_button)

        self.basic_load_button = QPushButton("导入数据")
        self.basic_load_button.clicked.connect(self.load_basic_excel_data)
        self.set_button_role(self.basic_load_button, "primary", min_width=140)
        path_layout.addWidget(self.basic_load_button)
        layout.addLayout(path_layout)

        self.basic_data_status_label = QLabel("尚未导入数据。")
        self.style_helper_label(self.basic_data_status_label, color="#555")
        layout.addWidget(self.basic_data_status_label)

        self.basic_column_status_label = QLabel("导入 Excel 后可从列名中选择匹配字段；发送仍要求存在“微信号”列。")
        self.style_helper_label(self.basic_column_status_label, color="#555")
        layout.addWidget(self.basic_column_status_label)
        return group

    def build_basic_receiver_group(self) -> QGroupBox:
        group = QGroupBox("2. 选择微信接收人")
        group.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)
        layout = self.init_basic_collapsible_group(group, "receiver")
        self.configure_splitter_pane(group, min_height=240)

        filter_row = QHBoxLayout()
        filter_row.setSpacing(8)
        match_prefix_label = QLabel("按")
        self.style_helper_label(match_prefix_label, color="#555")
        filter_row.addWidget(match_prefix_label)
        self.basic_match_field_combo = QComboBox(self)
        self.basic_match_field_combo.setEnabled(False)
        self.basic_match_field_combo.setMinimumWidth(144)
        self.basic_match_field_combo.currentIndexChanged.connect(self.on_basic_match_field_changed)
        filter_row.addWidget(self.basic_match_field_combo)
        match_suffix_label = QLabel("字段匹配")
        self.style_helper_label(match_suffix_label, color="#555")
        filter_row.addWidget(match_suffix_label)
        self.basic_match_keyword_input = QLineEdit(self)
        self.basic_match_keyword_input.setClearButtonEnabled(True)
        self.basic_match_keyword_input.setPlaceholderText("输入关键词，如：陈 或 abc001,abc002")
        self.basic_match_keyword_input.textChanged.connect(self.on_basic_match_keyword_changed)
        filter_row.addWidget(self.basic_match_keyword_input, stretch=1)

        self.basic_match_button = QPushButton("预览结果")
        self.basic_match_button.clicked.connect(self.preview_basic_match_results)
        self.set_button_role(self.basic_match_button, "secondary", min_width=132)
        filter_row.addWidget(self.basic_match_button)
        layout.addLayout(filter_row)

        info_row = QHBoxLayout()
        info_row.setSpacing(8)
        self.basic_match_field_status_label = QLabel("发送仍以“微信号”列为准。")
        self.style_helper_label(self.basic_match_field_status_label, color="#555")
        info_row.addWidget(self.basic_match_field_status_label, stretch=1)

        self.basic_selected_summary_label = QLabel("未选择接收人｜去重 0 人")
        self.style_helper_label(self.basic_selected_summary_label, color="#555")
        self.basic_selected_summary_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        info_row.addWidget(self.basic_selected_summary_label)
        layout.addLayout(info_row)

        self.basic_selected_table = QTableWidget(0, 3, self)
        self.basic_selected_table.setHorizontalHeaderLabels(["微信号", "显示名称", "状态"])
        self.basic_selected_table.verticalHeader().setVisible(False)
        self.basic_selected_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.basic_selected_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.basic_selected_table.setTextElideMode(Qt.ElideMiddle)
        self.basic_selected_table.setAlternatingRowColors(True)
        self.configure_resizable_table_columns(
            self.basic_selected_table,
            initial_widths=[170, 420, 120],
            signature="basic_selected_table",
            min_section_size=72,
        )
        self.basic_selected_table.verticalHeader().setDefaultSectionSize(36)
        self.basic_selected_table.verticalHeader().setMinimumSectionSize(32)
        self.basic_selected_table.setMinimumHeight(150)

        self.basic_selected_empty_label = QLabel(self)
        self.basic_selected_empty_label.setAlignment(Qt.AlignCenter)
        self.basic_selected_empty_label.setWordWrap(True)
        self.basic_selected_empty_label.setMinimumHeight(150)
        self.basic_selected_empty_label.setProperty("themeStyleRole", "empty-state")

        self.basic_selected_table_stack = QStackedWidget(self)
        self.basic_selected_table_stack.addWidget(self.basic_selected_empty_label)
        self.basic_selected_table_stack.addWidget(self.basic_selected_table)
        layout.addWidget(self.basic_selected_table_stack, stretch=1)
        self.set_basic_receiver_overview(
            "未选择接收人",
            duplicate_removed=0,
            tone="muted",
            empty_message=self.get_basic_receiver_empty_prompt(),
        )
        return group

    def build_basic_message_group(self) -> QGroupBox:
        group = QGroupBox("3. 消息内容")
        group.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)
        layout = self.init_basic_collapsible_group(group, "message")
        self.configure_splitter_pane(group, min_height=220)

        variable_row = QHBoxLayout()
        variable_row.addWidget(QLabel("可插入变量"))
        self.basic_variable_combo = QComboBox(self)
        self.basic_variable_combo.setEnabled(False)
        variable_row.addWidget(self.basic_variable_combo, stretch=1)
        self.basic_insert_variable_button = QPushButton("插入变量")
        self.basic_insert_variable_button.clicked.connect(self.insert_basic_variable)
        self.set_button_role(self.basic_insert_variable_button, "secondary", min_width=120)
        self.basic_insert_variable_button.setEnabled(False)
        variable_row.addWidget(self.basic_insert_variable_button)
        layout.addLayout(variable_row)

        self.basic_variable_status_label = QLabel("导入 Excel 后会在这里显示可插入的变量。")
        self.style_helper_label(self.basic_variable_status_label, color="#555")
        layout.addWidget(self.basic_variable_status_label)

        self.basic_message_input = QPlainTextEdit(self)
        self.basic_message_input.setPlaceholderText("请输入本次要发送的消息内容。")
        self.basic_message_input.setMinimumHeight(140)
        self.basic_message_input.textChanged.connect(self.on_basic_message_changed)
        layout.addWidget(self.basic_message_input, stretch=1)
        return group

    def build_basic_attachment_group(self) -> QGroupBox:
        group = QGroupBox("4. 附件（可多个）")
        group.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)
        layout = self.init_basic_collapsible_group(group, "attachment")
        self.configure_splitter_pane(group, min_height=170)

        input_row = QHBoxLayout()
        self.basic_attachment_input = FileDropLineEdit(allow_multiple=True, parent=self)
        self.basic_attachment_input.setPlaceholderText("拖入附件或输入路径（多文件用分号分隔）")
        input_row.addWidget(self.basic_attachment_input)

        self.basic_select_attachment_button = QPushButton("选择附件")
        self.basic_select_attachment_button.clicked.connect(self.select_basic_attachments)
        self.set_button_role(self.basic_select_attachment_button, "secondary", compact=True)
        input_row.addWidget(self.basic_select_attachment_button)

        self.basic_add_attachment_button = QPushButton("添加附件")
        self.basic_add_attachment_button.clicked.connect(self.import_basic_attachments_from_input)
        self.set_button_role(self.basic_add_attachment_button, "secondary", compact=True)
        input_row.addWidget(self.basic_add_attachment_button)
        layout.addLayout(input_row)

        action_row = QHBoxLayout()
        self.basic_remove_attachment_button = QPushButton("删除选中附件")
        self.basic_remove_attachment_button.clicked.connect(self.remove_selected_basic_attachments)
        self.set_button_role(self.basic_remove_attachment_button, "secondary", compact=True)
        self.basic_clear_attachment_button = QPushButton("清空附件")
        self.basic_clear_attachment_button.clicked.connect(self.clear_basic_attachments)
        self.set_button_role(self.basic_clear_attachment_button, "secondary", compact=True)
        action_row.addWidget(self.basic_remove_attachment_button)
        action_row.addWidget(self.basic_clear_attachment_button)
        action_row.addStretch(1)
        layout.addLayout(action_row)

        self.basic_attachment_table = QTableWidget(0, 2, self)
        self.basic_attachment_table.setHorizontalHeaderLabels(["类型", "路径"])
        self.basic_attachment_table.verticalHeader().setVisible(False)
        self.basic_attachment_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.basic_attachment_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.basic_attachment_table.setTextElideMode(Qt.ElideMiddle)
        self.configure_resizable_table_columns(
            self.basic_attachment_table,
            initial_widths=[110, 520],
            signature="basic_attachment_table",
            min_section_size=60,
        )
        self.basic_attachment_table.verticalHeader().setDefaultSectionSize(36)
        self.basic_attachment_table.verticalHeader().setMinimumSectionSize(32)
        self.basic_attachment_table.setMinimumHeight(120)
        layout.addWidget(self.basic_attachment_table, stretch=1)
        return group

    def build_basic_send_group(self) -> QGroupBox:
        group = QGroupBox("5. 发送设置")
        layout = self.init_basic_collapsible_group(group, "send")
        self.configure_splitter_pane(group, min_height=150, vertical_policy=QSizePolicy.Preferred)

        settings_grid = QGridLayout()
        settings_grid.setHorizontalSpacing(10)
        settings_grid.setVerticalSpacing(8)
        settings_grid.addWidget(QLabel("发送间隔（秒）"), 0, 0)
        self.basic_interval_spin = QSpinBox(self)
        self.basic_interval_spin.setRange(0, 3600)
        self.basic_interval_spin.valueChanged.connect(self.on_basic_interval_changed)
        settings_grid.addWidget(self.basic_interval_spin, 0, 1)
        settings_grid.addWidget(QLabel("本次发送人数"), 0, 2)
        self.basic_batch_limit_spin = QSpinBox(self)
        self.basic_batch_limit_spin.setRange(1, 9999)
        self.basic_batch_limit_spin.valueChanged.connect(self.on_basic_batch_limit_changed)
        settings_grid.addWidget(self.basic_batch_limit_spin, 0, 3)
        settings_grid.setColumnStretch(4, 1)
        layout.addLayout(settings_grid)

        self.basic_progress_label = QLabel("当前没有可发送任务。")
        self.style_overview_label(self.basic_progress_label)
        layout.addWidget(self.basic_progress_label)

        self.basic_runtime_status_label = QLabel("等待发送。")
        self.style_helper_label(self.basic_runtime_status_label, color="#555")
        layout.addWidget(self.basic_runtime_status_label)

        action_row = QHBoxLayout()
        self.basic_start_button = QPushButton("发送")
        self.basic_start_button.clicked.connect(self.start_basic_send)
        self.set_button_role(self.basic_start_button, "primary", min_width=160, min_height=44)
        action_row.addWidget(self.basic_start_button)

        self.basic_stop_button = QPushButton("停止")
        self.basic_stop_button.setEnabled(False)
        self.basic_stop_button.clicked.connect(self.stop_sending)
        self.set_button_role(self.basic_stop_button, "danger", min_width=140, min_height=44)
        action_row.addWidget(self.basic_stop_button)
        action_row.addStretch(1)
        layout.addLayout(action_row)
        return group

    def build_local_store_page(self) -> QWidget:
        page = QWidget(self)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        self.local_store_page_splitter = self.build_splitter(
            Qt.Horizontal,
            [self.build_local_store_group(), self.build_filter_group()],
            parent=page,
            stretch_factors=[3, 2],
            splitter_key="local_store.main",
            default_sizes=[640, 420],
        )
        layout.addWidget(self.local_store_page_splitter, stretch=1)
        return page

    def build_send_prepare_page(self) -> QWidget:
        page = QWidget(self)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        left_panel = QWidget(page)
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(10)
        control_group = self.configure_splitter_pane(self.build_control_group(), min_height=180, vertical_policy=QSizePolicy.Preferred)
        preview_group = self.configure_splitter_pane(self.build_preview_group(), min_height=240)
        self.send_prepare_left_splitter = self.build_splitter(
            Qt.Vertical,
            [control_group, preview_group],
            parent=left_panel,
            stretch_factors=[2, 5],
            splitter_key="workbench.send.left",
            default_sizes=[220, 460],
        )
        left_layout.addWidget(self.send_prepare_left_splitter, stretch=1)

        settings_group = self.build_execution_settings_group()
        self.send_prepare_splitter = self.build_splitter(
            Qt.Horizontal,
            [left_panel, self.build_scroll_area(settings_group)],
            parent=page,
            stretch_factors=[5, 3],
            splitter_key="workbench.send.main",
            default_sizes=[720, 420],
        )
        layout.addWidget(self.send_prepare_splitter, stretch=1)
        return page

    def build_task_center_page(self) -> QWidget:
        page = QWidget(self)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)
        layout.addWidget(self.build_task_center_toolbar())

        self.task_center_splitter = self.build_splitter(
            Qt.Horizontal,
            [self.build_schedule_group(), self.build_log_group()],
            parent=page,
            stretch_factors=[4, 3],
            splitter_key="task_center.main",
            default_sizes=[620, 460],
        )
        layout.addWidget(self.task_center_splitter, stretch=1)
        return page

    def build_task_center_toolbar(self) -> QGroupBox:
        group = QGroupBox("任务中心操作")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        action_layout = QHBoxLayout()
        self.import_json_button = QPushButton("导入 JSON（多选）")
        self.import_json_button.clicked.connect(self.import_json_tasks)
        self.set_button_role(self.import_json_button, "secondary", min_width=120, min_height=38)
        action_layout.addWidget(self.import_json_button)

        open_send_prepare_button = QPushButton("返回工作台")
        open_send_prepare_button.clicked.connect(self.open_send_prepare_page)
        self.set_button_role(open_send_prepare_button, "secondary", min_width=120, min_height=38)
        action_layout.addWidget(open_send_prepare_button)
        action_layout.addStretch(1)
        layout.addLayout(action_layout)

        return group

    def build_control_group(self) -> QGroupBox:
        group = QGroupBox("执行概览与主操作")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        summary_row = QHBoxLayout()
        summary_row.setSpacing(12)

        summary_text_layout = QVBoxLayout()
        summary_text_layout.setSpacing(6)

        self.execution_overview_label = QLabel("发送计划尚未准备好，请先读取数据或从本地库导入发送计划。")
        self.style_overview_label(self.execution_overview_label)
        summary_text_layout.addWidget(self.execution_overview_label)

        self.schedule_status_label = QLabel("当前为立即发送模式。")
        self.style_helper_label(self.schedule_status_label, color="#555")
        summary_text_layout.addWidget(self.schedule_status_label)

        summary_row.addLayout(summary_text_layout, stretch=1)

        self.send_status_label = QLabel("等待发送。")
        self.style_status_badge(self.send_status_label)
        summary_row.addWidget(self.send_status_label, alignment=Qt.AlignTop)

        layout.addLayout(summary_row)
        layout.addLayout(self.build_action_bar())

        return group

    def build_execution_settings_group(self) -> QGroupBox:
        group = QGroupBox("发送设置")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        common_card = QWidget(group)
        common_layout = QVBoxLayout(common_card)
        common_layout.setContentsMargins(0, 0, 0, 0)
        common_layout.setSpacing(10)

        mode_layout = QHBoxLayout()
        mode_title = QLabel("启动方式")
        self.style_section_title_label(mode_title)
        mode_layout.addWidget(mode_title)
        mode_layout.addSpacing(8)
        self.send_mode_group = QButtonGroup(self)
        self.immediate_mode_radio = QRadioButton("立即发送")
        self.scheduled_mode_radio = QRadioButton("定时发送")
        self.send_mode_group.addButton(self.immediate_mode_radio)
        self.send_mode_group.addButton(self.scheduled_mode_radio)
        self.immediate_mode_radio.toggled.connect(self.on_send_mode_changed)
        mode_layout.addWidget(self.immediate_mode_radio)
        mode_layout.addWidget(self.scheduled_mode_radio)
        mode_layout.addStretch(1)
        common_layout.addLayout(mode_layout)

        plan_time_layout = QHBoxLayout()
        plan_time_layout.addWidget(QLabel("计划时间"))
        self.scheduled_time_edit = QDateTimeEdit(self)
        self.scheduled_time_edit.setCalendarPopup(True)
        self.scheduled_time_edit.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.scheduled_time_edit.setDateTime(QDateTime.currentDateTime().addSecs(60))
        self.scheduled_time_edit.dateTimeChanged.connect(self.on_send_mode_changed)
        plan_time_layout.addWidget(self.scheduled_time_edit)
        plan_time_layout.addStretch(1)
        common_layout.addLayout(plan_time_layout)

        interval_layout = QHBoxLayout()
        interval_layout.addWidget(QLabel("发送间隔（秒）"))
        self.interval_spin = QSpinBox(self)
        self.interval_spin.setRange(0, 3600)
        self.interval_spin.valueChanged.connect(self.on_interval_changed)
        interval_layout.addWidget(self.interval_spin)
        interval_layout.addStretch(1)
        common_layout.addLayout(interval_layout)
        layout.addWidget(common_card)

        self.advanced_settings_toggle_button = QToolButton(group)
        self.advanced_settings_toggle_button.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self.advanced_settings_toggle_button.setCheckable(True)
        self.advanced_settings_toggle_button.toggled.connect(self.on_advanced_settings_toggled)
        layout.addWidget(self.advanced_settings_toggle_button)

        self.advanced_settings_panel = QWidget(group)
        advanced_layout = QVBoxLayout(self.advanced_settings_panel)
        advanced_layout.setContentsMargins(0, 0, 0, 0)
        advanced_layout.setSpacing(10)

        recurrence_layout = QGridLayout()
        recurrence_layout.setHorizontalSpacing(10)
        recurrence_layout.setVerticalSpacing(8)
        recurrence_layout.addWidget(QLabel("重复频率"), 0, 0)
        self.schedule_mode_combo = QComboBox(self)
        self.schedule_mode_combo.addItem("一次性", SCHEDULE_MODE_ONCE)
        self.schedule_mode_combo.addItem("每天", SCHEDULE_MODE_DAILY)
        self.schedule_mode_combo.addItem("每周", SCHEDULE_MODE_WEEKLY)
        self.schedule_mode_combo.addItem("Cron 自定义", SCHEDULE_MODE_CRON)
        self.schedule_mode_combo.currentIndexChanged.connect(self.on_schedule_mode_changed)
        recurrence_layout.addWidget(self.schedule_mode_combo, 0, 1)
        recurrence_layout.addWidget(QLabel("规则值"), 1, 0)
        self.schedule_value_input = QLineEdit(self)
        self.schedule_value_input.setPlaceholderText("Cron 示例：0 9 * * 1-5")
        self.schedule_value_input.textChanged.connect(self.on_bulk_send_option_changed)
        recurrence_layout.addWidget(self.schedule_value_input, 1, 1)
        self.schedule_mode_hint_label = QLabel("一次性任务不会自动生成下一次执行。")
        self.style_helper_label(self.schedule_mode_hint_label, color="#555")
        recurrence_layout.addWidget(self.schedule_mode_hint_label, 2, 0, 1, 2)
        advanced_layout.addLayout(recurrence_layout)

        advanced_layout.addWidget(self.build_separator())

        delay_title = QLabel("高级节奏与汇报")
        self.style_section_title_label(delay_title)
        advanced_layout.addWidget(delay_title)

        rhythm_grid = QGridLayout()
        rhythm_grid.setHorizontalSpacing(10)
        rhythm_grid.setVerticalSpacing(8)
        rhythm_grid.addWidget(QLabel("随机延迟（秒）"), 0, 0)
        self.random_delay_min_spin = QSpinBox(self)
        self.random_delay_min_spin.setRange(0, 3600)
        self.random_delay_min_spin.valueChanged.connect(self.on_bulk_send_option_changed)
        rhythm_grid.addWidget(QLabel("最小"), 0, 1)
        rhythm_grid.addWidget(self.random_delay_min_spin, 0, 2)
        self.random_delay_max_spin = QSpinBox(self)
        self.random_delay_max_spin.setRange(0, 3600)
        self.random_delay_max_spin.valueChanged.connect(self.on_bulk_send_option_changed)
        rhythm_grid.addWidget(QLabel("最大"), 0, 3)
        rhythm_grid.addWidget(self.random_delay_max_spin, 0, 4)
        advanced_layout.addLayout(rhythm_grid)

        report_grid = QGridLayout()
        report_grid.setHorizontalSpacing(10)
        report_grid.setVerticalSpacing(8)
        report_grid.addWidget(QLabel("操作人"), 0, 0)
        self.operator_name_input = QLineEdit(self)
        self.operator_name_input.setPlaceholderText("用于任务汇报")
        self.operator_name_input.editingFinished.connect(self.on_bulk_send_option_changed)
        report_grid.addWidget(self.operator_name_input, 0, 1)
        report_grid.addWidget(QLabel("汇报微信号"), 1, 0)
        self.report_to_input = QLineEdit(self)
        self.report_to_input.setPlaceholderText(DEFAULT_REPORT_TARGET)
        self.report_to_input.editingFinished.connect(self.on_bulk_send_option_changed)
        report_grid.addWidget(self.report_to_input, 1, 1)
        self.auto_report_checkbox = QCheckBox("任务完成后自动汇报", self)
        self.auto_report_checkbox.toggled.connect(self.on_bulk_send_option_changed)
        report_grid.addWidget(self.auto_report_checkbox, 2, 0, 1, 2)
        self.stop_on_error_checkbox = QCheckBox("单条异常后停止后续发送", self)
        self.stop_on_error_checkbox.setToolTip("开启：某个联系人发送失败时立即停止整批任务；关闭：记录失败后继续发后面的联系人。")
        self.stop_on_error_checkbox.toggled.connect(self.on_bulk_send_option_changed)
        report_grid.addWidget(self.stop_on_error_checkbox, 3, 0, 1, 2)
        advanced_layout.addLayout(report_grid)
        advanced_layout.addStretch(1)
        layout.addWidget(self.advanced_settings_panel)
        layout.addStretch(1)
        self.update_advanced_settings_panel(False)
        return group

    def build_schedule_group(self) -> QGroupBox:
        group = QGroupBox("任务队列")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        action_panel = QWidget(group)
        action_panel_layout = QVBoxLayout(action_panel)
        action_panel_layout.setContentsMargins(0, 0, 0, 0)
        action_panel_layout.setSpacing(8)

        task_action_layout = QGridLayout()
        task_action_layout.setHorizontalSpacing(10)
        task_action_layout.setVerticalSpacing(8)
        self.task_action_layout = task_action_layout
        self.refresh_schedule_button = QPushButton("刷新任务列表")
        self.refresh_schedule_button.clicked.connect(self.refresh_scheduled_jobs)
        self.set_button_role(self.refresh_schedule_button, "secondary", min_width=88)
        self.preview_schedule_button = QPushButton("预览选中任务")
        self.preview_schedule_button.clicked.connect(self.preview_selected_scheduled_job)
        self.set_button_role(self.preview_schedule_button, "secondary", min_width=88)
        self.preview_schedule_button.setEnabled(False)
        self.enable_schedule_button = QPushButton("开启自动调度")
        self.enable_schedule_button.clicked.connect(self.enable_selected_scheduled_job)
        self.set_button_role(self.enable_schedule_button, "secondary", min_width=88)
        self.enable_schedule_button.setEnabled(False)
        self.disable_schedule_button = QPushButton("关闭自动调度")
        self.disable_schedule_button.clicked.connect(self.disable_selected_scheduled_job)
        self.set_button_role(self.disable_schedule_button, "secondary", min_width=88)
        self.disable_schedule_button.setEnabled(False)
        self.delete_schedule_button = QPushButton("删除队列记录")
        self.delete_schedule_button.clicked.connect(self.delete_selected_scheduled_job)
        self.set_button_role(self.delete_schedule_button, "danger", min_width=88)
        self.delete_schedule_button.setEnabled(False)
        self.cancel_schedule_button = QPushButton("取消选中任务")
        self.cancel_schedule_button.clicked.connect(self.cancel_selected_scheduled_job)
        self.set_button_role(self.cancel_schedule_button, "secondary", min_width=88)
        self.cancel_schedule_button.setEnabled(False)
        task_action_layout.addWidget(self.refresh_schedule_button, 0, 0)
        task_action_layout.addWidget(self.preview_schedule_button, 0, 1)
        task_action_layout.addWidget(self.enable_schedule_button, 0, 2)
        task_action_layout.addWidget(self.disable_schedule_button, 1, 0)
        task_action_layout.addWidget(self.delete_schedule_button, 1, 1)
        task_action_layout.addWidget(self.cancel_schedule_button, 1, 2)
        task_action_layout.setColumnStretch(0, 1)
        task_action_layout.setColumnStretch(1, 1)
        task_action_layout.setColumnStretch(2, 1)
        action_panel_layout.addLayout(task_action_layout)

        self.schedule_table = QTableWidget(0, 7, self)
        self.schedule_table.setHorizontalHeaderLabels(["队列ID", "计划时间", "执行状态", "自动调度", "人数", "来源", "内容摘要"])
        self.schedule_table.verticalHeader().setVisible(False)
        self.schedule_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.schedule_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.schedule_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.schedule_table.setTextElideMode(Qt.ElideMiddle)
        self.schedule_table.itemSelectionChanged.connect(self.update_action_button_state)
        self.configure_resizable_table_columns(
            self.schedule_table,
            initial_widths=[90, 180, 110, 110, 80, 150, 420],
            signature="schedule_table",
            min_section_size=60,
        )
        self.schedule_table.verticalHeader().setDefaultSectionSize(36)
        self.schedule_table.verticalHeader().setMinimumSectionSize(32)
        self.schedule_table.setMinimumHeight(220)

        schedule_splitter = self.build_splitter(
            Qt.Vertical,
            [
                self.configure_splitter_pane(action_panel, min_height=96, vertical_policy=QSizePolicy.Preferred),
                self.configure_splitter_pane(self.schedule_table, min_height=220),
            ],
            parent=group,
            stretch_factors=[1, 4],
            splitter_key="task_center.schedule",
            default_sizes=[118, 452],
        )
        layout.addWidget(schedule_splitter, stretch=1)

        return group

    def build_local_store_group(self) -> QGroupBox:
        group = QGroupBox("本地库数据")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        header_panel = QWidget(group)
        header_layout = QVBoxLayout(header_panel)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(8)

        action_layout = QHBoxLayout()
        self.refresh_local_store_button = QPushButton("刷新本地库")
        self.refresh_local_store_button.clicked.connect(self.refresh_local_store_page)
        self.use_local_store_button = QPushButton("筛选并导入发送计划")
        self.use_local_store_button.clicked.connect(self.filter_local_store_into_task)
        action_layout.addWidget(self.refresh_local_store_button)
        action_layout.addWidget(self.use_local_store_button)
        action_layout.addStretch(1)
        header_layout.addLayout(action_layout)

        self.local_store_tabs = QTabWidget(self)
        self.local_store_views: dict[str, dict[str, object]] = {}
        for dataset_type in (DATASET_FRIEND, DATASET_GROUP):
            tab, view_refs = self.build_local_store_dataset_panel(dataset_type)
            self.local_store_tabs.addTab(tab, DATASET_LABELS[dataset_type])
            self.local_store_views[dataset_type] = view_refs
        self.local_store_tabs.currentChanged.connect(self.on_local_store_tab_changed)
        local_store_splitter = self.build_splitter(
            Qt.Vertical,
            [
                self.configure_splitter_pane(header_panel, min_height=64, vertical_policy=QSizePolicy.Preferred),
                self.configure_splitter_pane(self.local_store_tabs, min_height=260),
            ],
            parent=group,
            stretch_factors=[1, 4],
            splitter_key="local_store.dataset_shell",
            default_sizes=[72, 508],
        )
        layout.addWidget(local_store_splitter, stretch=1)

        return group

    def build_local_store_dataset_panel(self, dataset_type: str) -> tuple[QWidget, dict[str, object]]:
        panel = QWidget(self)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        info_panel = QWidget(panel)
        info_layout = QVBoxLayout(info_panel)
        info_layout.setContentsMargins(0, 0, 0, 0)
        info_layout.setSpacing(8)

        summary_label = QLabel(f"{DATASET_LABELS[dataset_type]}库暂无数据。")
        self.style_helper_label(summary_label)
        info_layout.addWidget(summary_label)

        columns_view = QPlainTextEdit(self)
        columns_view.setReadOnly(True)
        columns_view.setFixedHeight(60)
        info_layout.addWidget(columns_view)

        table = QTableWidget(0, 0, self)
        table.verticalHeader().setVisible(False)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setTextElideMode(Qt.ElideMiddle)
        table.verticalHeader().setDefaultSectionSize(36)
        table.verticalHeader().setMinimumSectionSize(32)
        table.setMinimumHeight(220)

        dataset_splitter = self.build_splitter(
            Qt.Vertical,
            [
                self.configure_splitter_pane(info_panel, min_height=100, vertical_policy=QSizePolicy.Preferred),
                self.configure_splitter_pane(table, min_height=220),
            ],
            parent=panel,
            stretch_factors=[1, 4],
            splitter_key=f"local_store.{dataset_type}",
            default_sizes=[110, 360],
        )
        layout.addWidget(dataset_splitter, stretch=1)

        return panel, {
            "summary_label": summary_label,
            "columns_view": columns_view,
            "table": table,
        }

    def build_excel_group(self) -> QGroupBox:
        group = QGroupBox("Excel 数据")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        source_panel = QWidget(group)
        source_layout = QVBoxLayout(source_panel)
        source_layout.setContentsMargins(0, 0, 0, 0)
        source_layout.setSpacing(8)

        path_layout = QHBoxLayout()
        self.excel_path_input = FileDropLineEdit(
            suffixes=[".xlsx", ".xls", ".csv"],
            parent=self,
        )
        self.excel_path_input.setPlaceholderText("选择或拖入 Excel 文件（支持 .xlsx / .xls / .csv）")
        self.excel_path_input.textChanged.connect(self.on_excel_path_changed)
        path_layout.addWidget(self.excel_path_input)

        self.excel_choose_button = QPushButton("选择文件")
        self.excel_choose_button.clicked.connect(self.select_excel_file)
        path_layout.addWidget(self.excel_choose_button)

        load_button = QPushButton("读取数据")
        load_button.clicked.connect(self.load_excel_data)
        path_layout.addWidget(load_button)
        self.load_excel_button = load_button

        import_button = QPushButton("导入到本地库")
        import_button.clicked.connect(self.import_excel_to_local_store)
        path_layout.addWidget(import_button)
        self.import_local_button = import_button

        source_layout.addLayout(path_layout)

        self.data_info_label = QLabel("尚未读取 Excel 数据。")
        self.style_helper_label(self.data_info_label, color="#555")
        source_layout.addWidget(self.data_info_label)

        self.local_db_status_label = QLabel("本地库尚未导入数据。")
        self.style_helper_label(self.local_db_status_label, color="#555")
        source_layout.addWidget(self.local_db_status_label)

        target_panel = QWidget(group)
        target_layout = QVBoxLayout(target_panel)
        target_layout.setContentsMargins(0, 0, 0, 0)
        target_layout.setSpacing(8)

        self.send_target_column_input = QLineEdit(self)
        self.send_target_column_input.setPlaceholderText("微信号")
        self.send_target_column_input.textChanged.connect(self.on_send_target_column_changed)
        target_layout.addWidget(self.send_target_column_input)

        self.send_target_status_label = QLabel("默认按“微信号”搜索。")
        self.style_helper_label(self.send_target_status_label, color="#555")
        target_layout.addWidget(self.send_target_status_label)

        self.columns_empty_label = QLabel("暂无列名。")
        self.style_empty_state_label(self.columns_empty_label, role="section-empty")

        self.columns_view = QPlainTextEdit(self)
        self.columns_view.setReadOnly(True)
        self.columns_view.setPlaceholderText("")
        self.columns_view.setFixedHeight(64)
        self.update_columns_reference_presentation()

        columns_panel_content = QWidget(group)
        columns_panel_layout = QVBoxLayout(columns_panel_content)
        columns_panel_layout.setContentsMargins(0, 0, 0, 0)
        columns_panel_layout.setSpacing(6)
        columns_panel_layout.addWidget(self.columns_empty_label)
        columns_panel_layout.addWidget(self.columns_view)

        source_section = self.build_section_panel(
            parent=group,
            title="文件与导入",
            content=source_panel,
        )
        self.data_template_source_section = source_section
        target_section = self.build_section_panel(
            parent=group,
            title="发送识别列",
            content=target_panel,
        )
        self.data_template_target_section = target_section
        columns_section = self.build_section_panel(
            parent=group,
            title="检测到的列名",
            content=columns_panel_content,
        )
        self.data_template_columns_section = columns_section
        columns_section.setMinimumHeight(180)

        excel_splitter = self.build_splitter(
            Qt.Vertical,
            [
                self.configure_splitter_pane(source_section, min_height=160, vertical_policy=QSizePolicy.Preferred),
                self.configure_splitter_pane(target_section, min_height=140, vertical_policy=QSizePolicy.Preferred),
                self.configure_splitter_pane(columns_section, min_height=180),
            ],
            parent=group,
            stretch_factors=[2, 2, 3],
            splitter_key="data_template.excel",
            default_sizes=[220, 190, 250],
        )
        layout.addWidget(excel_splitter, stretch=1)

        return group

    def build_template_group(self) -> QGroupBox:
        group = QGroupBox("消息模板")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        self.template_input = QPlainTextEdit(self)
        self.template_input.setPlaceholderText("")
        self.template_input.textChanged.connect(self.on_template_changed)
        self.template_input.setMinimumHeight(220)

        self.placeholder_status_label = QLabel("未使用占位符。")
        self.style_helper_label(self.placeholder_status_label)

        template_panel_content = QWidget(group)
        template_panel_layout = QVBoxLayout(template_panel_content)
        template_panel_layout.setContentsMargins(0, 0, 0, 0)
        template_panel_layout.setSpacing(0)
        template_panel_layout.addWidget(self.template_input, stretch=1)

        placeholder_panel_content = QWidget(group)
        placeholder_panel_layout = QVBoxLayout(placeholder_panel_content)
        placeholder_panel_layout.setContentsMargins(0, 0, 0, 0)
        placeholder_panel_layout.setSpacing(0)
        placeholder_panel_layout.addWidget(self.placeholder_status_label)

        attachment_path_layout = QHBoxLayout()
        self.common_attachment_input = FileDropLineEdit(
            allow_multiple=True,
            parent=self,
        )
        self.common_attachment_input.setPlaceholderText("输入或拖入附件路径")
        attachment_path_layout.addWidget(self.common_attachment_input)

        choose_attachment_button = QPushButton("选择附件")
        choose_attachment_button.clicked.connect(self.select_common_attachments)
        attachment_path_layout.addWidget(choose_attachment_button)

        add_attachment_button = QPushButton("添加到通用附件")
        add_attachment_button.clicked.connect(self.import_common_attachments_from_input)
        attachment_path_layout.addWidget(add_attachment_button)

        attachment_action_layout = QHBoxLayout()
        self.remove_common_attachment_button = QPushButton("删除选中附件")
        self.remove_common_attachment_button.clicked.connect(self.remove_selected_common_attachments)
        self.clear_common_attachment_button = QPushButton("清空通用附件")
        self.clear_common_attachment_button.clicked.connect(self.clear_common_attachments)
        attachment_action_layout.addWidget(self.remove_common_attachment_button)
        attachment_action_layout.addWidget(self.clear_common_attachment_button)
        attachment_action_layout.addStretch(1)

        self.common_attachment_status_label = QLabel("未添加通用附件。")
        self.style_helper_label(self.common_attachment_status_label, color="#555")

        self.common_attachment_empty_label = QLabel("暂无通用附件。")
        self.style_empty_state_label(self.common_attachment_empty_label, role="section-empty")

        self.common_attachment_table = QTableWidget(0, 2, self)
        self.common_attachment_table.setHorizontalHeaderLabels(["类型", "路径"])
        self.common_attachment_table.verticalHeader().setVisible(False)
        self.common_attachment_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.common_attachment_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.common_attachment_table.setTextElideMode(Qt.ElideMiddle)
        self.configure_resizable_table_columns(
            self.common_attachment_table,
            initial_widths=[110, 520],
            signature="common_attachment_table",
            min_section_size=60,
        )
        self.common_attachment_table.verticalHeader().setDefaultSectionSize(36)
        self.common_attachment_table.verticalHeader().setMinimumSectionSize(32)
        self.common_attachment_table.setMinimumHeight(150)
        self.update_common_attachment_presentation()

        attachment_panel_content = QWidget(group)
        attachment_panel_layout = QVBoxLayout(attachment_panel_content)
        attachment_panel_layout.setContentsMargins(0, 0, 0, 0)
        attachment_panel_layout.setSpacing(8)
        attachment_panel_layout.addLayout(attachment_path_layout)
        attachment_panel_layout.addLayout(attachment_action_layout)
        attachment_panel_layout.addWidget(self.common_attachment_status_label)
        attachment_panel_layout.addWidget(self.common_attachment_empty_label)
        attachment_panel_layout.addWidget(self.common_attachment_table, stretch=1)

        template_section = self.build_section_panel(
            parent=group,
            title="模板内容",
            content=template_panel_content,
        )
        self.data_template_template_section = template_section
        placeholder_section = self.build_section_panel(
            parent=group,
            title="占位符反馈",
            content=placeholder_panel_content,
        )
        self.data_template_placeholder_section = placeholder_section
        placeholder_section.setMinimumHeight(120)
        attachment_section = self.build_section_panel(
            parent=group,
            title="通用附件",
            content=attachment_panel_content,
        )
        self.data_template_attachment_section = attachment_section

        template_splitter = self.build_splitter(
            Qt.Vertical,
            [
                self.configure_splitter_pane(template_section, min_height=240),
                self.configure_splitter_pane(placeholder_section, min_height=120, vertical_policy=QSizePolicy.Preferred),
                self.configure_splitter_pane(attachment_section, min_height=220),
            ],
            parent=group,
            stretch_factors=[3, 1, 2],
            splitter_key="data_template.template",
            default_sizes=[320, 150, 280],
        )
        layout.addWidget(template_splitter, stretch=1)

        return group

    def build_filter_group(self) -> QGroupBox:
        group = QGroupBox("本地库筛选条件")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        tip_label = QLabel(
            "筛选作用于“本地库数据”页当前选中的页签。多个字段请用英文逗号分隔；规则留空时，会把当前页签全部数据带入确认弹窗。"
        )
        self.style_helper_label(tip_label)
        layout.addWidget(tip_label)

        fields_layout = QHBoxLayout()
        fields_layout.addWidget(QLabel("筛选字段"))
        self.filter_fields_input = QLineEdit(self)
        self.filter_fields_input.setPlaceholderText("例如：显示名称,备注,昵称")
        self.filter_fields_input.textChanged.connect(self.on_filter_fields_changed)
        fields_layout.addWidget(self.filter_fields_input)
        layout.addLayout(fields_layout)

        pattern_layout = QHBoxLayout()
        pattern_layout.addWidget(QLabel("正则规则"))
        self.filter_pattern_input = QLineEdit(self)
        self.filter_pattern_input.setPlaceholderText("例如：陈|到期|高意向；留空=当前页签全量")
        self.filter_pattern_input.textChanged.connect(self.on_filter_pattern_changed)
        pattern_layout.addWidget(self.filter_pattern_input)
        layout.addLayout(pattern_layout)

        action_layout = QHBoxLayout()
        self.filter_ignore_case_checkbox = QCheckBox("忽略大小写", self)
        self.filter_ignore_case_checkbox.toggled.connect(self.on_filter_ignore_case_changed)
        self.apply_filter_button = QPushButton("筛选并导入发送计划")
        self.apply_filter_button.clicked.connect(self.filter_local_store_into_task)
        self.reset_filter_button = QPushButton("重置条件")
        self.reset_filter_button.clicked.connect(self.reset_local_filter_inputs)
        action_layout.addWidget(self.filter_ignore_case_checkbox)
        action_layout.addWidget(self.apply_filter_button)
        action_layout.addWidget(self.reset_filter_button)
        action_layout.addStretch(1)
        layout.addLayout(action_layout)

        self.filter_status_label = QLabel("请先在上方选择好友库或群聊库，再按条件筛选并导入发送计划。")
        self.style_helper_label(self.filter_status_label, color="#555")
        layout.addWidget(self.filter_status_label)

        examples_label = QLabel(
            "场景规则示例：\n"
            "1. 到期提醒：到期|续费|复购\n"
            "2. 跟进客户：待跟进|未回复|沉默\n"
            "3. 地区筛选：上海|北京|深圳\n"
            "4. 精确匹配：^高意向客户$\n"
            "5. 排除空白场景：^(?!\\s*$).+"
        )
        self.style_helper_label(examples_label, color="#555")
        layout.addWidget(examples_label)

        return group

    def build_action_bar(self) -> QVBoxLayout:
        layout = QVBoxLayout()
        layout.setSpacing(8)

        action_grid = QGridLayout()
        action_grid.setHorizontalSpacing(10)
        action_grid.setVerticalSpacing(8)

        self.preview_button = QPushButton("刷新发送计划")
        self.preview_button.clicked.connect(self.show_preview_results)
        self.set_button_role(self.preview_button, "secondary", min_width=84, min_height=40)

        self.start_button = QPushButton("立即开始发送")
        self.start_button.clicked.connect(self.start_sending)
        self.set_button_role(self.start_button, "primary", min_width=96, min_height=42)

        self.stop_button = QPushButton("停止发送")
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_sending)
        self.set_button_role(self.stop_button, "danger", min_width=72, min_height=42)

        self.continue_button = QPushButton("继续发送")
        self.continue_button.setEnabled(False)
        self.continue_button.clicked.connect(self.continue_sending)
        self.set_button_role(self.continue_button, "secondary", min_width=72, min_height=42)

        self.export_json_button = QPushButton("导出 JSON")
        self.export_json_button.clicked.connect(self.export_current_plan_to_json)
        self.set_button_role(self.export_json_button, "secondary", min_width=72, min_height=38)

        self.debug_mode_button = QPushButton("调试模式：关")
        self.debug_mode_button.setCheckable(True)
        self.debug_mode_button.toggled.connect(self.on_debug_mode_toggled)
        self.set_button_role(self.debug_mode_button, "neutral", min_width=84, min_height=38)

        action_grid.addWidget(self.start_button, 0, 0)
        action_grid.addWidget(self.stop_button, 0, 1)
        action_grid.addWidget(self.continue_button, 0, 2)
        action_grid.addWidget(self.preview_button, 1, 0)
        action_grid.addWidget(self.export_json_button, 1, 1)
        action_grid.addWidget(self.debug_mode_button, 1, 2)
        action_grid.setColumnStretch(0, 1)
        action_grid.setColumnStretch(1, 1)
        action_grid.setColumnStretch(2, 1)

        layout.addLayout(action_grid)
        return layout

    def build_preview_group(self) -> QGroupBox:
        group = QGroupBox("发送计划预览")
        layout = QVBoxLayout(group)

        self.preview_table = QTableWidget(0, 4, self)
        self.update_preview_headers()
        self.preview_table.verticalHeader().setVisible(False)
        self.preview_table.setEditTriggers(
            QAbstractItemView.DoubleClicked
            | QAbstractItemView.EditKeyPressed
            | QAbstractItemView.SelectedClicked
        )
        self.preview_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.preview_table.itemChanged.connect(self.on_preview_item_changed)
        self.preview_table.setTextElideMode(Qt.ElideMiddle)
        self.apply_table_header_font(self.preview_table)
        self.configure_resizable_table_columns(
            self.preview_table,
            initial_widths=[180, 220, 520, 150],
            signature="preview_table",
            min_section_size=80,
        )
        self.preview_table.verticalHeader().setDefaultSectionSize(36)
        self.preview_table.verticalHeader().setMinimumSectionSize(32)
        layout.addWidget(self.preview_table)

        return group

    def build_log_group(self) -> QGroupBox:
        group = QGroupBox("执行日志")
        layout = QVBoxLayout(group)

        self.log_view = QPlainTextEdit(self)
        self.log_view.setReadOnly(True)
        self.log_view.setPlaceholderText("发送过程中的日志会显示在这里。")
        layout.addWidget(self.log_view)

        return group

    def init_language_choose(self) -> QVBoxLayout:
        layout = QVBoxLayout()
        layout.addWidget(QLabel("请选择你的微信系统语言"))

        button_layout = QHBoxLayout()
        self.language_group = QButtonGroup(self)
        self.lang_zh_cn = QRadioButton("简体中文")
        self.lang_zh_tw = QRadioButton("繁体中文")
        self.lang_en = QRadioButton("English")

        self.language_group.addButton(self.lang_zh_cn)
        self.language_group.addButton(self.lang_zh_tw)
        self.language_group.addButton(self.lang_en)

        self.lang_zh_cn.clicked.connect(lambda: self.set_language("zh-CN"))
        self.lang_zh_tw.clicked.connect(lambda: self.set_language("zh-TW"))
        self.lang_en.clicked.connect(lambda: self.set_language("en-US"))

        button_layout.addWidget(self.lang_zh_cn)
        button_layout.addWidget(self.lang_zh_tw)
        button_layout.addWidget(self.lang_en)
        layout.addLayout(button_layout)
        return layout

    def restore_initial_state(self) -> None:
        self._is_restoring_state = True
        try:
            self.excel_path_input.setText(self.config["excel"]["path"])
            self.basic_excel_path_input.setText(self.config["excel"]["path"])
            self.send_target_column_input.setText(self.config["excel"]["send_target_column"])
            self.filter_fields_input.setText(self.config["filter"]["fields"])
            self.filter_pattern_input.setText(self.config["filter"]["pattern"])
            self.filter_ignore_case_checkbox.setChecked(self.config["filter"]["ignore_case"])
            self.template_input.setPlainText(self.config["template"]["text"])
            self.common_attachments = self.normalize_attachment_items(self.config["template"].get("common_attachments", []))
            self.refresh_common_attachment_table()
            self.interval_spin.setValue(self.config["settings"]["send_interval"])
            basic_mode = dict(self.config.get("basic_mode") or {})
            self.basic_message_input.setPlainText(str(basic_mode.get("message_text") or ""))
            self.basic_match_field = str(basic_mode.get("match_field") or DEFAULT_SEND_TARGET_COLUMN).strip() or DEFAULT_SEND_TARGET_COLUMN
            self.basic_match_keyword_input.setText(str(basic_mode.get("match_keyword") or ""))
            self.basic_batch_limit_spin.setValue(max(1, int(basic_mode.get("batch_limit") or 50)))
            self.basic_interval_spin.setValue(int(self.config["settings"]["send_interval"]))
            self.basic_attachments = self.normalize_attachment_items(basic_mode.get("attachments", []))
            self.refresh_basic_attachment_table()
            self.update_basic_match_field_options()
            bulk_send = dict(self.config["bulk_send"])
            self.random_delay_min_spin.setValue(int(bulk_send["random_delay_min"]))
            self.random_delay_max_spin.setValue(int(bulk_send["random_delay_max"]))
            self.operator_name_input.setText(str(bulk_send["operator_name"]))
            self.report_to_input.setText(str(bulk_send["report_to"]))
            self.auto_report_checkbox.setChecked(bool(bulk_send["auto_report_enabled"]))
            self.stop_on_error_checkbox.setChecked(bool(bulk_send.get("stop_on_error", True)))
            self.debug_mode_button.setChecked(bool(bulk_send.get("debug_mode_enabled")))
            schedule_mode = str(bulk_send.get("schedule_mode") or SCHEDULE_MODE_ONCE)
            schedule_value = str(bulk_send.get("schedule_value") or "")
            schedule_index = self.schedule_mode_combo.findData(schedule_mode)
            self.schedule_mode_combo.setCurrentIndex(max(schedule_index, 0))
            self.schedule_value_input.setText(schedule_value)
            self.update_debug_mode_button_text()
            if bulk_send.get("send_mode") == "scheduled":
                self.scheduled_mode_radio.setChecked(True)
            else:
                self.immediate_mode_radio.setChecked(True)
            self.on_send_mode_changed()
            self.on_schedule_mode_changed()

            language = self.config["settings"]["language"]
            if hasattr(self, "lang_zh_tw") and hasattr(self, "lang_en") and hasattr(self, "lang_zh_cn"):
                if language == "zh-TW":
                    self.lang_zh_tw.setChecked(True)
                elif language == "en-US":
                    self.lang_en.setChecked(True)
                else:
                    self.lang_zh_cn.setChecked(True)
            theme_mode = str(self.config["settings"].get("theme_mode") or THEME_MODE_AUTO)
            theme_index = self.theme_mode_combo.findData(theme_mode)
            self.theme_mode_combo.setCurrentIndex(max(theme_index, 0))
            self._theme_mode = theme_mode if theme_mode in {THEME_MODE_AUTO, THEME_MODE_LIGHT, THEME_MODE_DARK} else THEME_MODE_AUTO
            ui_config = dict(self.config.get("ui") or {})
            self.update_advanced_settings_panel(bool(ui_config.get("advanced_settings_expanded", False)))
        finally:
            self._is_restoring_state = False

        self.on_bulk_send_option_changed()
        self.on_send_mode_changed()

        self.update_local_db_status()
        self.refresh_local_store_page()
        self.refresh_scheduled_jobs()
        self.update_placeholder_status()
        self.update_send_target_column_status()
        self.update_action_button_state()
        self.update_basic_variable_options()
        self.refresh_basic_selected_table()
        self.update_basic_progress_status()
        self.apply_theme()
        ui_config = dict(self.config.get("ui") or {})
        self.navigate_to(
            str(ui_config.get("nav_page") or PAGE_KEY_WORKBENCH),
            str(ui_config.get("workbench_view") or WORKBENCH_VIEW_BASIC),
            persist=False,
        )
        if self.excel_path_input.text().strip():
            self.load_excel_data(show_success=False)
        else:
            self.render_preview()
        QTimer.singleShot(0, self.restore_registered_splitter_states)

    def set_language(self, language: str) -> None:
        self.config["settings"]["language"] = language
        self.save_config()

    def is_local_db_mode(self) -> bool:
        return self.active_source_mode == SOURCE_MODE_LOCAL_DB

    def can_edit_preview_rows(self) -> bool:
        return self.active_source_mode == SOURCE_MODE_FILE or self.current_task_id is not None

    def update_local_db_status(self) -> None:
        summaries = self.local_store.get_current_import_summaries()
        if not summaries:
            self.set_label_tone(self.local_db_status_label, "muted")
            self.local_db_status_label.setText("本地库暂无批次。")
            return

        parts = [
            f"{summary.dataset_label}：{summary.source_name}（{summary.row_count} 行）"
            for summary in summaries.values()
        ]
        text = "本地库批次：" + "；".join(parts)
        if self.is_local_db_mode():
            if self.current_task_id is not None:
                text += f"｜当前快照 {len(self.records)} 行"
            else:
                text += "｜当前使用中"
        self.set_label_tone(self.local_db_status_label, "success")
        self.local_db_status_label.setText(text)

    def refresh_local_store_page(self) -> None:
        summaries = self.local_store.get_current_import_summaries()
        if not summaries:
            for view_refs in self.local_store_views.values():
                summary_label = view_refs["summary_label"]
                columns_view = view_refs["columns_view"]
                table = view_refs["table"]
                assert isinstance(summary_label, QLabel)
                assert isinstance(columns_view, QPlainTextEdit)
                assert isinstance(table, QTableWidget)
                self.set_label_tone(summary_label, "muted")
                summary_label.setText("暂无数据。")
                columns_view.clear()
                table.setColumnCount(0)
                table.setRowCount(0)
            self.update_local_filter_scope()
            return

        has_any_records = False
        for dataset_type in (DATASET_FRIEND, DATASET_GROUP):
            summary = summaries.get(dataset_type)
            view_refs = self.local_store_views[dataset_type]
            summary_label = view_refs["summary_label"]
            columns_view = view_refs["columns_view"]
            table = view_refs["table"]
            assert isinstance(summary_label, QLabel)
            assert isinstance(columns_view, QPlainTextEdit)
            assert isinstance(table, QTableWidget)

            if summary is None:
                self.set_label_tone(summary_label, "muted")
                summary_label.setText(f"{DATASET_LABELS[dataset_type]}库暂无 current 批次。")
                columns_view.clear()
                table.setColumnCount(0)
                table.setRowCount(0)
                continue

            records, columns, _ = self.local_store.load_current_contacts(dataset_type)
            has_any_records = has_any_records or bool(records)
            self.set_label_tone(summary_label, "success")
            summary_label.setText(
                f"{summary.source_name} | 导入时间：{summary.imported_at} | 共 {summary.row_count} 行"
            )
            columns_view.setPlainText("、".join(columns))
            table.setColumnCount(len(columns))
            table.setHorizontalHeaderLabels(columns)
            table.setRowCount(len(records))

            for row_index, row in enumerate(records):
                for column_index, column_name in enumerate(columns):
                    table.setItem(
                        row_index,
                        column_index,
                        QTableWidgetItem(str(row.get(column_name, ""))),
                    )

            self.configure_resizable_table_columns(
                table,
                signature=f"local_store:{dataset_type}:{'|'.join(columns)}",
                min_section_size=72,
                auto_fit_on_signature_change=True,
                max_auto_width=340,
            )
            table.resizeRowsToContents()

        self.use_local_store_button.setEnabled(has_any_records)
        self.update_local_filter_scope()

    def open_local_store_page(self) -> None:
        self.refresh_local_store_page()
        self.navigate_to(PAGE_KEY_LOCAL_STORE)

    def open_send_prepare_page(self) -> None:
        self.navigate_to(PAGE_KEY_WORKBENCH, WORKBENCH_VIEW_SEND)

    def open_task_center_page(self) -> None:
        self.refresh_scheduled_jobs()
        self.navigate_to(PAGE_KEY_TASK_CENTER)

    def invalidate_basic_task(self, reason: str | None = None) -> None:
        if self.basic_task_id is None:
            return
        self.basic_task_id = None
        if reason:
            self.append_log(reason)
        self.update_basic_progress_status()
        self.refresh_basic_selected_table()

    def save_basic_mode_config(self) -> None:
        self.config["basic_mode"]["message_text"] = self.basic_message_input.toPlainText()
        self.config["basic_mode"]["attachments"] = [dict(item) for item in self.basic_attachments]
        self.config["basic_mode"]["match_field"] = self.get_basic_match_field()
        self.config["basic_mode"]["match_keyword"] = self.basic_match_keyword_input.text().strip()
        self.config["basic_mode"]["batch_limit"] = self.basic_batch_limit_spin.value()
        self.save_config_if_ready()

    def on_basic_excel_path_changed(self, path: str) -> None:
        normalized_path = path.strip()
        self.config["excel"]["path"] = normalized_path
        self.save_config_if_ready()
        if hasattr(self, "excel_path_input") and self.excel_path_input.text().strip() != normalized_path:
            self.excel_path_input.blockSignals(True)
            self.excel_path_input.setText(normalized_path)
            self.excel_path_input.blockSignals(False)
        if normalized_path != self.basic_last_loaded_path:
            self.basic_source_records = []
            self.basic_columns = []
            self.basic_selected_records = []
            self.basic_last_loaded_path = ""
            self.basic_last_match_total = 0
            self.basic_last_duplicate_removed = 0
            self.invalidate_basic_task("已更换基本功能页的数据文件，上一轮续发进度已重置。")
            self.update_basic_variable_options()
            self.update_basic_match_field_options()
            self.set_basic_receiver_overview(
                "未选择接收人",
                duplicate_removed=0,
                tone="muted",
                empty_message=self.get_basic_receiver_empty_prompt(),
            )
            self.refresh_basic_selected_table()
            self.update_basic_progress_status()
            self.basic_data_status_label.setText("文件已变更，请重新点击“导入数据”。")
            self.set_label_tone(self.basic_data_status_label, "warning")

    def select_basic_excel_file(self) -> None:
        path = QFileDialog.getOpenFileName(
            self,
            "选择 Excel 文件",
            "",
            "表格文件(*.xlsx *.xls *.csv)",
        )[0]
        if path:
            self.basic_excel_path_input.setText(path)

    def load_basic_excel_data(self) -> bool:
        path = self.basic_excel_path_input.text().strip()
        if path == "":
            QMessageBox.warning(self, "输入错误", "请先选择 Excel 文件。")
            return False
        try:
            records, columns = load_contact_records(path)
            validate_contact_records(records, columns, required_column=DEFAULT_SEND_TARGET_COLUMN)
        except Exception as exc:
            self.basic_source_records = []
            self.basic_columns = []
            self.basic_selected_records = []
            self.basic_last_loaded_path = ""
            self.basic_last_match_total = 0
            self.basic_last_duplicate_removed = 0
            self.invalidate_basic_task()
            self.basic_data_status_label.setText(f"读取失败：{exc}")
            self.set_label_tone(self.basic_data_status_label, "danger")
            self.update_basic_match_field_options()
            self.set_basic_receiver_overview(
                "未选择接收人",
                duplicate_removed=0,
                tone="muted",
                empty_message=self.get_basic_receiver_empty_prompt(),
            )
            self.basic_column_status_label.setText("请确认 Excel 中存在“微信号”列后再重试。")
            self.set_label_tone(self.basic_column_status_label, "warning")
            if hasattr(self, "basic_match_field_status_label"):
                self.basic_match_field_status_label.setText("当前无法匹配接收人，请先导入包含“微信号”列的 Excel。")
                self.set_label_tone(self.basic_match_field_status_label, "warning")
            self.update_basic_variable_options()
            self.refresh_basic_selected_table()
            self.update_basic_progress_status()
            QMessageBox.warning(self, "读取失败", f"读取 Excel 失败！\n错误信息：{exc}")
            return False

        self.basic_source_records = self.attach_record_ids(records)
        self.basic_columns = list(columns)
        self.basic_selected_records = []
        self.basic_last_loaded_path = path
        self.basic_last_match_total = 0
        self.basic_last_duplicate_removed = 0
        self.invalidate_basic_task()
        self.update_basic_variable_options()
        self.update_basic_match_field_options()
        self.set_basic_receiver_overview(
            "未选择接收人",
            duplicate_removed=0,
            tone="muted",
            empty_message=self.get_basic_receiver_empty_prompt(),
        )
        self.refresh_basic_selected_table()
        valid_count = len([row for row in self.basic_source_records if (row.get(DEFAULT_SEND_TARGET_COLUMN) or "").strip()])
        self.basic_data_status_label.setText(
            f"已导入 {len(self.basic_source_records)} 行数据，其中 {valid_count} 行包含可用“微信号”。"
        )
        self.set_label_tone(self.basic_data_status_label, "success")
        self.update_basic_progress_status()
        QMessageBox.information(self, "读取成功", f"已成功读取 {len(self.basic_source_records)} 行 Excel 数据。")
        return True

    def get_basic_display_columns(self) -> list[str]:
        return [str(column) for column in self.basic_columns if not str(column).startswith("__")]

    def get_basic_match_field(self) -> str:
        if hasattr(self, "basic_match_field_combo") and self.basic_match_field_combo.count() > 0:
            selected_value = str(self.basic_match_field_combo.currentData() or "").strip()
            if selected_value:
                return selected_value
        return str(self.basic_match_field or DEFAULT_SEND_TARGET_COLUMN).strip() or DEFAULT_SEND_TARGET_COLUMN

    def get_basic_receiver_empty_prompt(self) -> str:
        return "暂无接收人，请先输入关键词并点击“预览结果”。"

    def set_basic_receiver_overview(
        self,
        status_text: str,
        *,
        duplicate_removed: int | None = None,
        tone: str = "muted",
        empty_message: str | None = None,
        empty_tone: str | None = None,
    ) -> None:
        if hasattr(self, "basic_selected_summary_label"):
            removed_count = self.basic_last_duplicate_removed if duplicate_removed is None else max(int(duplicate_removed), 0)
            self.basic_selected_summary_label.setText(f"{status_text}｜去重 {removed_count} 人")
            self.set_label_tone(self.basic_selected_summary_label, tone)
        if hasattr(self, "basic_selected_empty_label"):
            message = empty_message or self.get_basic_receiver_empty_prompt()
            self.basic_selected_empty_label.setText(message)
            self.basic_selected_empty_label.setProperty("themeTone", empty_tone or tone)
            self.apply_semantic_widget_style(self.basic_selected_empty_label)

    def update_basic_match_field_status(self, fallback_message: str = "") -> None:
        match_field = self.get_basic_match_field()
        display_columns = self.get_basic_display_columns()
        if not display_columns:
            self.basic_column_status_label.setText("导入 Excel 后可从列名中选择匹配字段；发送仍要求存在“微信号”列。")
            self.set_label_tone(self.basic_column_status_label, "muted")
            if hasattr(self, "basic_match_field_status_label"):
                self.basic_match_field_status_label.setText("导入 Excel 后可选择匹配字段；发送仍以“微信号”为准。")
                self.set_label_tone(self.basic_match_field_status_label, "muted")
            return

        valid_count = len([row for row in self.basic_source_records if (row.get(DEFAULT_SEND_TARGET_COLUMN) or "").strip()])
        import_text = (
            f"当前可从 {len(display_columns)} 个字段中选择匹配列；发送仍使用“微信号”，"
            f"当前共有 {valid_count} 行可发送。"
        )
        receiver_text = f"当前按“{match_field}”字段匹配；发送仍以“微信号”为准。"
        if fallback_message:
            import_text = f"{import_text} {fallback_message}"
            receiver_text = f"{receiver_text} {fallback_message}"
            self.set_label_tone(self.basic_column_status_label, "warning")
            if hasattr(self, "basic_match_field_status_label"):
                self.set_label_tone(self.basic_match_field_status_label, "warning")
        else:
            self.set_label_tone(self.basic_column_status_label, "muted")
            if hasattr(self, "basic_match_field_status_label"):
                self.set_label_tone(self.basic_match_field_status_label, "muted")

        self.basic_column_status_label.setText(import_text)
        if hasattr(self, "basic_match_field_status_label"):
            self.basic_match_field_status_label.setText(receiver_text)

    def update_basic_match_field_options(self) -> None:
        if not hasattr(self, "basic_match_field_combo"):
            return

        preferred_field = str(
            (self.config.get("basic_mode") or {}).get("match_field")
            or self.basic_match_field
            or DEFAULT_SEND_TARGET_COLUMN
        ).strip() or DEFAULT_SEND_TARGET_COLUMN
        display_columns = self.get_basic_display_columns()
        fallback_message = ""
        resolved_field = preferred_field

        self.basic_match_field_combo.blockSignals(True)
        self.basic_match_field_combo.clear()
        for column in display_columns:
            self.basic_match_field_combo.addItem(column, column)

        if display_columns:
            if preferred_field not in display_columns:
                if DEFAULT_SEND_TARGET_COLUMN in display_columns:
                    resolved_field = DEFAULT_SEND_TARGET_COLUMN
                else:
                    resolved_field = display_columns[0]
                fallback_message = f"当前文件中不存在“{preferred_field}”，已自动回退到“{resolved_field}”。"
            selected_index = self.basic_match_field_combo.findData(resolved_field)
            self.basic_match_field_combo.setCurrentIndex(max(selected_index, 0))
            self.basic_match_field_combo.setEnabled(True)
        else:
            self.basic_match_field_combo.setEnabled(False)

        self.basic_match_field_combo.blockSignals(False)
        self.basic_match_field = resolved_field
        self.config["basic_mode"]["match_field"] = resolved_field
        self.save_config_if_ready()
        self.update_basic_match_field_status(fallback_message)

    def update_basic_variable_options(self) -> None:
        if not hasattr(self, "basic_variable_combo"):
            return
        self.basic_variable_combo.blockSignals(True)
        self.basic_variable_combo.clear()
        display_columns = self.get_basic_display_columns()
        for column in display_columns:
            self.basic_variable_combo.addItem(column, column)
        self.basic_variable_combo.blockSignals(False)
        enabled = bool(display_columns)
        self.basic_variable_combo.setEnabled(enabled)
        self.basic_insert_variable_button.setEnabled(enabled)
        if enabled:
            self.basic_variable_status_label.setText(f"当前可插入 {len(display_columns)} 个变量，例如：{display_columns[0]}。")
            self.set_label_tone(self.basic_variable_status_label, "success")
        else:
            self.basic_variable_status_label.setText("导入 Excel 后会在这里显示可插入的变量。")
            self.set_label_tone(self.basic_variable_status_label, "muted")

    def insert_basic_variable(self) -> None:
        field_name = str(self.basic_variable_combo.currentData() or "").strip()
        if not field_name:
            return
        self.basic_message_input.insertPlainText(f"{{{{{field_name}}}}}")
        self.basic_message_input.setFocus()

    def on_basic_match_field_changed(self, _index: int) -> None:
        self.basic_match_field = str(self.basic_match_field_combo.currentData() or "").strip() or DEFAULT_SEND_TARGET_COLUMN
        self.save_basic_mode_config()
        self.update_basic_match_field_status()
        if not self.basic_selected_records and self.basic_task_id is None:
            self.set_basic_receiver_overview(
                "未选择接收人",
                duplicate_removed=self.basic_last_duplicate_removed,
                tone="muted",
                empty_message=self.get_basic_receiver_empty_prompt(),
            )
        if self.basic_selected_records:
            self.invalidate_basic_task("已修改匹配字段，若需重新匹配请再次点击“预览匹配结果”。")

    def on_basic_match_keyword_changed(self, value: str) -> None:
        self.basic_match_keyword = value.strip()
        self.save_basic_mode_config()
        if not self.basic_selected_records and self.basic_task_id is None:
            self.set_basic_receiver_overview(
                "未选择接收人",
                duplicate_removed=self.basic_last_duplicate_removed,
                tone="muted",
                empty_message=self.get_basic_receiver_empty_prompt(),
            )
        if self.basic_selected_records:
            self.invalidate_basic_task("已修改接收人关键词，若需重新匹配请再次点击“预览匹配结果”。")

    def on_basic_message_changed(self) -> None:
        self.save_basic_mode_config()
        if self.basic_task_id is not None:
            self.invalidate_basic_task("已修改基本功能页消息内容，续发进度已重置。")

    def on_basic_interval_changed(self, value: int) -> None:
        self.config["settings"]["send_interval"] = value
        if hasattr(self, "interval_spin") and self.interval_spin.value() != value:
            self.interval_spin.blockSignals(True)
            self.interval_spin.setValue(value)
            self.interval_spin.blockSignals(False)
        self.save_config_if_ready()

    def on_basic_batch_limit_changed(self, _value: int) -> None:
        self.save_basic_mode_config()
        self.update_basic_progress_status()

    def build_basic_match_candidates(self) -> tuple[list[dict[str, str]], int, int]:
        if not self.basic_source_records and not self.load_basic_excel_data():
            return [], 0, 0
        match_field = self.get_basic_match_field()
        if not match_field:
            raise ValueError("请先导入 Excel 并选择匹配字段。")
        if match_field not in self.get_basic_display_columns():
            raise ValueError(f"当前 Excel 中不存在匹配字段“{match_field}”，请重新选择。")
        keywords = [
            segment.strip()
            for segment in re.split(r"[，,]", self.basic_match_keyword_input.text().strip())
            if segment.strip()
        ]
        if not keywords:
            raise ValueError("请输入至少一个匹配关键词。")
        matched: list[dict[str, str]] = []
        seen_targets: set[str] = set()
        duplicate_removed = 0
        for row in self.basic_source_records:
            search_value = (row.get(match_field) or "").strip()
            if not search_value:
                continue
            target_value = (row.get(DEFAULT_SEND_TARGET_COLUMN) or "").strip()
            if not target_value:
                continue
            lower_search_value = search_value.lower()
            if not any(keyword.lower() in lower_search_value for keyword in keywords):
                continue
            normalized_row = dict(row)
            normalized_row[TARGET_VALUE_KEY] = target_value
            normalized_row["_search_key"] = search_value
            if target_value in seen_targets:
                duplicate_removed += 1
                continue
            seen_targets.add(target_value)
            matched.append(normalized_row)
        return matched, len(matched) + duplicate_removed, duplicate_removed

    def preview_basic_match_results(self) -> None:
        try:
            matched_rows, raw_total, duplicate_removed = self.build_basic_match_candidates()
        except Exception as exc:
            QMessageBox.warning(self, "匹配失败", str(exc))
            return
        self.basic_last_match_total = raw_total
        self.basic_last_duplicate_removed = duplicate_removed
        if not matched_rows:
            self.basic_selected_records = []
            self.set_basic_receiver_overview(
                "未找到接收人",
                duplicate_removed=duplicate_removed,
                tone="warning",
                empty_message="没有找到匹配的接收人，请调整关键词或匹配字段后重试。",
                empty_tone="warning",
            )
            self.refresh_basic_selected_table()
            self.update_basic_progress_status()
            QMessageBox.information(self, "无匹配结果", "当前关键词没有匹配到任何接收人。")
            return

        dialog = ContactConfirmDialog(matched_rows, parent=self)
        dialog.setWindowTitle("基本功能页匹配结果确认")
        dialog.ok_btn.setText("确认接收人")
        if dialog.exec_() != ContactConfirmDialog.Accepted:
            return
        confirmed_rows = dialog.get_confirmed_contacts()
        if not confirmed_rows:
            QMessageBox.information(self, "无选中", "没有勾选任何联系人。")
            return
        self.basic_selected_records = [dict(row) for row in confirmed_rows]
        self.invalidate_basic_task("已更新基本功能页接收人名单，续发进度已重置。")
        self.refresh_basic_selected_table()
        self.update_basic_progress_status()

    def refresh_basic_selected_table(self) -> None:
        if not hasattr(self, "basic_selected_table"):
            return
        if self.basic_task_id is not None:
            records = self.local_store.load_task_records(self.basic_task_id)
        else:
            records = list(self.basic_selected_records)
        if hasattr(self, "basic_selected_table_stack"):
            self.basic_selected_table_stack.setCurrentWidget(
                self.basic_selected_table if records else self.basic_selected_empty_label
            )
        self.basic_selected_table.setRowCount(len(records))
        for row_index, row in enumerate(records):
            target_value = str(row.get(TARGET_VALUE_KEY) or row.get(DEFAULT_SEND_TARGET_COLUMN) or "").strip()
            display_name = self.get_display_name(row) or target_value
            status_value = str(row.get(ROW_SEND_STATUS_KEY) or row.get("send_status") or "").strip().lower()
            status_text = BASIC_SEND_STATUS_TEXT.get(status_value, "待发送")

            target_item = QTableWidgetItem(target_value)
            target_item.setToolTip(target_value)
            display_item = QTableWidgetItem(display_name)
            display_item.setToolTip(display_name)

            status_bg_color, status_text_color = BASIC_SEND_STATUS_COLORS.get(status_value, BASIC_SEND_STATUS_COLORS[""])
            status_item = QTableWidgetItem(status_text)
            status_item.setBackground(QColor(status_bg_color))
            status_item.setForeground(QColor(status_text_color))
            status_item.setTextAlignment(Qt.AlignCenter)
            status_item.setToolTip(f"状态：{status_text}")

            self.basic_selected_table.setItem(row_index, 0, target_item)
            self.basic_selected_table.setItem(row_index, 1, display_item)
            self.basic_selected_table.setItem(row_index, 2, status_item)
        self.basic_selected_table.resizeRowsToContents()

        selected_count = len(records)
        if selected_count:
            self.set_basic_receiver_overview(
                f"已确认 {selected_count} 人",
                duplicate_removed=self.basic_last_duplicate_removed,
                tone="success",
                empty_message=self.get_basic_receiver_empty_prompt(),
            )

    def update_basic_progress_status(self) -> None:
        if not hasattr(self, "basic_progress_label"):
            return
        total_selected = len(self.basic_selected_records)
        batch_limit = self.basic_batch_limit_spin.value() if hasattr(self, "basic_batch_limit_spin") else 0
        if total_selected == 0:
            self.basic_progress_label.setText("当前没有可发送任务。")
            self.basic_runtime_status_label.setText("请先导入 Excel 并确认接收人。")
            self.set_label_tone(self.basic_runtime_status_label, "muted")
            return

        remaining = total_selected
        completed = 0
        if self.basic_task_id is not None:
            task_records = self.local_store.load_task_records(self.basic_task_id)
            remaining = len(self.get_basic_pending_records(self.basic_task_id))
            completed = max(len(task_records) - remaining, 0)
        self.basic_progress_label.setText(
            f"当前已确认 {total_selected} 人；本次计划发送 {min(batch_limit, remaining) if remaining else 0} 人；剩余 {remaining} 人。"
        )
        if remaining == 0:
            self.basic_runtime_status_label.setText(f"当前名单已全部处理完成，共完成 {completed} 人。")
            self.set_label_tone(self.basic_runtime_status_label, "success")
        elif completed > 0:
            self.basic_runtime_status_label.setText(
                f"上一轮已完成 {completed} 人，剩余 {remaining} 人；再次点击发送会从剩余联系人继续。"
            )
            self.set_label_tone(self.basic_runtime_status_label, "warning")
        else:
            self.basic_runtime_status_label.setText("准备就绪，点击“发送”即可开始。")
            self.set_label_tone(self.basic_runtime_status_label, "muted")
        if hasattr(self, "basic_start_button"):
            self.basic_start_button.setText("继续发送" if completed > 0 and remaining > 0 else "发送")

    def refresh_basic_attachment_table(self) -> None:
        if not hasattr(self, "basic_attachment_table"):
            return
        self.basic_attachment_table.setRowCount(len(self.basic_attachments))
        for row_index, item in enumerate(self.basic_attachments):
            file_type = str(item.get("file_type") or "")
            file_path = str(item.get("file_path") or "")

            type_bg_color, type_text_color = ATTACHMENT_TYPE_COLORS.get(file_type, ATTACHMENT_TYPE_COLORS.get("file", ("#6b7280", "#ffffff")))
            type_item = QTableWidgetItem(file_type)
            type_item.setBackground(QColor(type_bg_color))
            type_item.setForeground(QColor(type_text_color))
            type_item.setTextAlignment(Qt.AlignCenter)
            type_item.setToolTip(f"类型：{file_type}\n路径：{file_path}")

            path_item = QTableWidgetItem(file_path)
            path_item.setToolTip(file_path)

            self.basic_attachment_table.setItem(row_index, 0, type_item)
            self.basic_attachment_table.setItem(row_index, 1, path_item)
        self.basic_attachment_table.resizeRowsToContents()
        self.basic_remove_attachment_button.setEnabled(bool(self.basic_attachments))
        self.basic_clear_attachment_button.setEnabled(bool(self.basic_attachments))

    def select_basic_attachments(self) -> None:
        start_dir = (
            self.config.get("json_tasks", {}).get("last_attachment_dir")
            or self.config.get("json_tasks", {}).get("last_import_dir")
            or ""
        )
        paths, _ = QFileDialog.getOpenFileNames(self, "选择附件", start_dir, "所有文件(*.*)")
        if not paths:
            return
        self.config["json_tasks"]["last_attachment_dir"] = str(Path(paths[0]).parent)
        self.save_config_if_ready()
        self.basic_attachment_input.setText(";".join(paths))

    def import_basic_attachments_from_input(self) -> None:
        raw_value = self.basic_attachment_input.text().strip()
        if raw_value == "":
            QMessageBox.information(self, "未选择附件", "请先选择或拖入至少一个附件。")
            return
        try:
            new_items = self.normalize_attachment_items(raw_value)
        except Exception as exc:
            QMessageBox.warning(self, "附件无效", str(exc))
            return
        existing_paths = {str(item.get("file_path") or "") for item in self.basic_attachments}
        for item in new_items:
            file_path = str(item.get("file_path") or "")
            if file_path and file_path not in existing_paths:
                self.basic_attachments.append(item)
                existing_paths.add(file_path)
        self.basic_attachment_input.clear()
        self.refresh_basic_attachment_table()
        self.save_basic_mode_config()
        if self.basic_task_id is not None:
            self.invalidate_basic_task("已修改基本功能页附件，续发进度已重置。")

    def remove_selected_basic_attachments(self) -> None:
        if not self.basic_attachments:
            return
        rows = sorted({index.row() for index in self.basic_attachment_table.selectionModel().selectedRows()}, reverse=True)
        if not rows:
            QMessageBox.information(self, "未选择附件", "请先选择要删除的附件。")
            return
        for row in rows:
            if 0 <= row < len(self.basic_attachments):
                self.basic_attachments.pop(row)
        self.refresh_basic_attachment_table()
        self.save_basic_mode_config()
        if self.basic_task_id is not None:
            self.invalidate_basic_task("已修改基本功能页附件，续发进度已重置。")

    def clear_basic_attachments(self) -> None:
        if not self.basic_attachments:
            return
        self.basic_attachments = []
        self.refresh_basic_attachment_table()
        self.save_basic_mode_config()
        if self.basic_task_id is not None:
            self.invalidate_basic_task("已修改基本功能页附件，续发进度已重置。")

    def create_basic_task_snapshot(self) -> int:
        rows: list[dict[str, Any]] = []
        for row in self.basic_selected_records:
            normalized_row = dict(row)
            normalized_row[TARGET_VALUE_KEY] = str(
                normalized_row.get(TARGET_VALUE_KEY) or normalized_row.get(DEFAULT_SEND_TARGET_COLUMN) or ""
            ).strip()
            rows.append(normalized_row)
        return self.local_store.create_task_snapshot(
            rows=rows,
            filter_fields=self.get_basic_match_field(),
            filter_pattern=self.basic_match_keyword_input.text().strip(),
            target_column=DEFAULT_SEND_TARGET_COLUMN,
            template_text=self.basic_message_input.toPlainText(),
            source_batch_id=None,
            source_mode=SOURCE_MODE_FILE,
            dataset_type="",
            common_attachments=[dict(item) for item in self.basic_attachments],
        )

    def ensure_basic_task_snapshot(self) -> int:
        if self.basic_task_id is not None:
            return self.basic_task_id
        self.basic_task_id = self.create_basic_task_snapshot()
        return self.basic_task_id

    def get_basic_pending_records(self, task_id: int) -> list[dict[str, Any]]:
        records = self.local_store.load_task_records(task_id)
        if not records:
            return []
        if all(str(row.get("send_status") or "").strip() == "" for row in records):
            return records
        return self.build_resume_records(task_id)

    def start_basic_send(self) -> None:
        if self.send_thread is not None and self.send_thread.isRunning():
            QMessageBox.information(self, "发送中", "当前已有发送任务正在执行。")
            return
        if not self.basic_selected_records:
            QMessageBox.information(self, "无法发送", "请先导入 Excel 并确认接收人。")
            return
        message_text = self.basic_message_input.toPlainText().strip()
        if not message_text and not self.basic_attachments:
            QMessageBox.warning(self, "无法发送", "请输入消息内容，或至少选择一个附件。")
            return
        missing_fields = find_missing_fields(extract_placeholders(self.basic_message_input.toPlainText()), self.basic_columns)
        if missing_fields:
            QMessageBox.warning(self, "无法发送", f"消息中存在缺失列：{', '.join(missing_fields)}")
            return

        task_id = self.ensure_basic_task_snapshot()
        pending_records = self.get_basic_pending_records(task_id)
        if not pending_records:
            QMessageBox.information(self, "无需继续", "当前接收人名单已经全部处理完成。")
            self.update_basic_progress_status()
            return

        batch_limit = max(1, self.basic_batch_limit_spin.value())
        batch_records = pending_records[:batch_limit]
        reply = QMessageBox.question(
            self,
            "确认发送",
            (
                f"本次将发送 {len(batch_records)} 人。\n"
                f"当前名单剩余 {len(pending_records)} 人待处理。\n"
                "确认后会在达到本次人数上限后自动暂停。"
            ),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.current_send_origin = "basic"
        self.current_send_batch_limit = batch_limit
        self.current_send_remaining_before_start = len(pending_records)
        task_details = self.local_store.get_task_details(task_id) or {}
        self.launch_send_thread(
            records=batch_records,
            template_text=self.basic_message_input.toPlainText(),
            target_column=TARGET_VALUE_KEY,
            interval_seconds=self.basic_interval_spin.value(),
            random_delay_min=0,
            random_delay_max=0,
            operator_name=self.operator_name_input.text().strip(),
            report_to=self.report_to_input.text().strip() or DEFAULT_REPORT_TARGET,
            auto_report=False,
            scheduled_job=None,
            task_id_override=task_id,
            common_attachments_override=self.basic_attachments,
            send_origin="basic",
        )
        self.basic_runtime_status_label.setText(
            f"正在发送：本轮 {len(batch_records)} 人，当前总剩余 {len(pending_records)} 人。"
        )
        self.set_label_tone(self.basic_runtime_status_label, "warning")

    def on_local_store_tab_changed(self, _index: int) -> None:
        self.update_local_filter_scope()

    def get_active_local_dataset_type(self) -> str:
        if not hasattr(self, "local_store_tabs"):
            return DATASET_FRIEND

        current_index = self.local_store_tabs.currentIndex()
        if current_index == 1:
            return DATASET_GROUP
        return DATASET_FRIEND

    def update_local_filter_scope(self) -> None:
        if not hasattr(self, "local_filter_scope_label"):
            return

        dataset_type = self.get_active_local_dataset_type()
        summary = self.local_store.get_current_import_summary(dataset_type)
        if summary is None:
            self.set_label_tone(self.local_filter_scope_label, "muted")
            self.local_filter_scope_label.setText(
                f"当前筛选对象：{DATASET_LABELS[dataset_type]}库当前批次（暂无数据）。"
            )
            self.use_local_store_button.setEnabled(False)
            return

        self.set_label_tone(self.local_filter_scope_label, "success")
        self.local_filter_scope_label.setText(
            f"当前筛选对象：{summary.dataset_label}库当前批次，来源 {summary.source_name}，共 {summary.row_count} 行。"
        )
        self.use_local_store_button.setEnabled(summary.row_count > 0)

    def activate_local_store_for_send(self) -> None:
        self.filter_local_store_into_task()

    def update_action_button_state(self) -> None:
        if not hasattr(self, "start_button"):
            return

        selected_job = self.get_selected_scheduled_job() if hasattr(self, "schedule_table") else None
        selected_json_pending = bool(
            selected_job is not None
            and selected_job.task_kind == "json"
            and selected_job.status == SCHEDULE_STATUS_PENDING
        )

        if selected_json_pending:
            self.start_button.setText("开始所选任务" if self._compact_ui_mode else "立即开始所选任务")
        elif hasattr(self, "scheduled_mode_radio") and self.scheduled_mode_radio.isChecked():
            self.start_button.setText("创建定时任务" if self._compact_ui_mode else "创建普通定时任务")
        else:
            self.start_button.setText("开始发送" if self._compact_ui_mode else "立即开始发送")

        if hasattr(self, "cancel_schedule_button"):
            can_cancel = bool(selected_job is not None and selected_job.status == SCHEDULE_STATUS_PENDING)
            self.cancel_schedule_button.setEnabled(can_cancel)
        if hasattr(self, "preview_schedule_button"):
            self.preview_schedule_button.setEnabled(selected_job is not None)
        can_toggle_auto = bool(selected_job is not None and selected_job.status == SCHEDULE_STATUS_PENDING)
        if hasattr(self, "enable_schedule_button"):
            self.enable_schedule_button.setEnabled(bool(can_toggle_auto and not bool(selected_job.enabled if selected_job else 0)))
        if hasattr(self, "disable_schedule_button"):
            self.disable_schedule_button.setEnabled(bool(can_toggle_auto and bool(selected_job.enabled if selected_job else 0)))
        if hasattr(self, "delete_schedule_button"):
            can_delete = bool(selected_job is not None and selected_job.status != SCHEDULE_STATUS_RUNNING)
            self.delete_schedule_button.setEnabled(can_delete)
        if hasattr(self, "continue_button"):
            self.continue_button.setEnabled(self.get_resume_context() is not None)

    def get_schedule_mode_text(self, schedule_mode: str, schedule_value: str = "") -> str:
        normalized_mode = str(schedule_mode or SCHEDULE_MODE_ONCE).strip().lower()
        if normalized_mode == SCHEDULE_MODE_DAILY:
            return "每天"
        if normalized_mode == SCHEDULE_MODE_WEEKLY:
            return "每周"
        if normalized_mode == SCHEDULE_MODE_CRON:
            return f"Cron（{schedule_value or '未设置'}）"
        return "一次性"

    def get_schedule_config_for_queue(self) -> tuple[str, str, str | None]:
        schedule_mode = self.get_selected_schedule_mode()
        schedule_value = self.schedule_value_input.text().strip() if hasattr(self, "schedule_value_input") else ""
        if schedule_mode == SCHEDULE_MODE_CRON:
            if schedule_value == "":
                return schedule_mode, schedule_value, "请填写 Cron 表达式。"
            try:
                next_run = self.compute_next_run_from_cron(datetime.now(), schedule_value)
            except ValueError as exc:
                return schedule_mode, schedule_value, str(exc)
            if next_run is None:
                return schedule_mode, schedule_value, "Cron 表达式在未来一年内没有可执行时间。"
        return schedule_mode, schedule_value, None

    def parse_cron_field(self, field_text: str, min_value: int, max_value: int, *, allow_seven_as_zero: bool = False) -> set[int]:
        normalized = str(field_text or "").strip()
        if normalized == "":
            raise ValueError("Cron 表达式字段不能为空。")

        values: set[int] = set()
        for part in normalized.split(","):
            segment = part.strip()
            if segment == "":
                raise ValueError("Cron 表达式字段存在空片段。")
            if "/" in segment:
                base_text, step_text = segment.split("/", 1)
                try:
                    step = int(step_text)
                except ValueError as exc:
                    raise ValueError(f"Cron 步长无效：{segment}") from exc
                if step <= 0:
                    raise ValueError(f"Cron 步长必须大于 0：{segment}")
            else:
                base_text = segment
                step = 1

            if base_text in {"", "*"}:
                start_value, end_value = min_value, max_value
            elif "-" in base_text:
                start_text, end_text = base_text.split("-", 1)
                start_value = int(start_text)
                end_value = int(end_text)
            else:
                start_value = int(base_text)
                end_value = start_value

            if allow_seven_as_zero:
                if start_value == 7:
                    start_value = 0
                if end_value == 7:
                    end_value = 0

            if start_value > end_value and not allow_seven_as_zero:
                raise ValueError(f"Cron 范围无效：{segment}")
            if start_value < min_value or end_value > max_value:
                raise ValueError(f"Cron 范围超出限制：{segment}")

            current = start_value
            while current <= end_value:
                values.add(0 if allow_seven_as_zero and current == 7 else current)
                current += step

        return values

    def cron_matches_datetime(self, dt: datetime, expression: str) -> bool:
        parts = [part.strip() for part in str(expression or "").split() if part.strip()]
        if len(parts) != 5:
            raise ValueError("Cron 表达式必须是 5 段：分 时 日 月 周。")
        minute_values = self.parse_cron_field(parts[0], 0, 59)
        hour_values = self.parse_cron_field(parts[1], 0, 23)
        day_values = self.parse_cron_field(parts[2], 1, 31)
        month_values = self.parse_cron_field(parts[3], 1, 12)
        weekday_values = self.parse_cron_field(parts[4], 0, 7, allow_seven_as_zero=True)
        cron_weekday = (dt.weekday() + 1) % 7
        return (
            dt.minute in minute_values
            and dt.hour in hour_values
            and dt.day in day_values
            and dt.month in month_values
            and cron_weekday in weekday_values
        )

    def compute_next_run_from_cron(self, base_time: datetime, expression: str) -> datetime | None:
        candidate = base_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(366 * 24 * 60):
            if self.cron_matches_datetime(candidate, expression):
                return candidate
            candidate += timedelta(minutes=1)
        return None

    def compute_next_run_time(self, job: ScheduledSendJob, finished_at_text: str) -> str | None:
        schedule_mode = str(job.schedule_mode or SCHEDULE_MODE_ONCE).strip().lower()
        if schedule_mode == SCHEDULE_MODE_ONCE:
            return None
        base_dt = datetime.strptime(
            finished_at_text or datetime.now().strftime(JSON_TIME_FORMAT),
            JSON_TIME_FORMAT,
        )
        current_schedule = datetime.strptime(job.scheduled_at, JSON_TIME_FORMAT)
        if schedule_mode == SCHEDULE_MODE_DAILY:
            next_dt = current_schedule + timedelta(days=1)
            while next_dt <= base_dt:
                next_dt += timedelta(days=1)
            return next_dt.strftime(JSON_TIME_FORMAT)
        if schedule_mode == SCHEDULE_MODE_WEEKLY:
            next_dt = current_schedule + timedelta(days=7)
            while next_dt <= base_dt:
                next_dt += timedelta(days=7)
            return next_dt.strftime(JSON_TIME_FORMAT)
        if schedule_mode == SCHEDULE_MODE_CRON:
            next_dt = self.compute_next_run_from_cron(base_dt, str(job.schedule_value or ""))
            if next_dt is None:
                return None
            return next_dt.strftime(JSON_TIME_FORMAT)
        return None

    def get_resume_context(self) -> tuple[int, ScheduledSendJob | None, list[dict[str, Any]], str] | None:
        selected_job = self.get_selected_scheduled_job() if hasattr(self, "schedule_table") else None
        if selected_job is not None and selected_job.status in {SCHEDULE_STATUS_FAILED, SCHEDULE_STATUS_CANCELLED}:
            records = self.build_resume_records(selected_job.task_id)
            if records:
                return selected_job.task_id, selected_job, records, f"定时任务 {selected_job.job_id}"
        if self.current_task_id is not None:
            records = self.build_resume_records(self.current_task_id)
            if records:
                return self.current_task_id, None, records, f"任务快照 {self.current_task_id}"
        return None

    def build_resume_records(self, task_id: int) -> list[dict[str, Any]]:
        records = self.local_store.load_task_records(task_id)
        if not records:
            return []
        last_terminal_index = -1
        for index, row in enumerate(records):
            send_status = str(row.get("send_status") or "").strip().lower()
            if send_status in TERMINAL_SEND_STATUSES:
                last_terminal_index = index
        if last_terminal_index < 0 or last_terminal_index >= len(records) - 1:
            return []
        remaining: list[dict[str, Any]] = []
        for row in records[last_terminal_index + 1 :]:
            send_status = str(row.get("send_status") or "").strip().lower()
            if send_status not in TERMINAL_SEND_STATUSES:
                remaining.append(dict(row))
        return remaining

    def ensure_local_filter_defaults(self) -> None:
        current_fields = [field.strip() for field in self.filter_fields_input.text().split(",") if field.strip()]
        if current_fields and any(field in self.columns for field in current_fields):
            return

        available_fields = [field for field in DEFAULT_LOCAL_FILTER_FIELDS if field in self.columns]
        if not available_fields:
            return

        normalized_fields = ",".join(available_fields)
        self.filter_fields_input.setText(normalized_fields)

    def clear_task_snapshot(self, reason: str | None = None) -> None:
        if self.current_task_id is None:
            return

        self.current_task_id = None
        self.records = list(self.filtered_records)
        if reason:
            self.append_log(reason)
        self.update_local_db_status()
        self.update_action_button_state()

    def load_records_into_view(
        self,
        *,
        records: list[dict[str, str]],
        columns: list[str],
        source_mode: str,
        loaded_path: str = "",
        batch_id: int | None = None,
        batch_ids: dict[str, int] | None = None,
    ) -> None:
        self.active_source_mode = source_mode
        self.current_batch_id = batch_id
        self.current_batch_ids = dict(batch_ids or {})
        self.current_task_id = None
        self.current_runtime_task_id = None
        self.current_runtime_records = []
        self.current_runtime_source_json_path = ""
        self.current_runtime_log_path = ""
        self.source_records = self.attach_record_ids(records)
        self.filtered_records = list(self.source_records)
        self.records = list(self.filtered_records)
        self.columns = list(columns)
        self.records_loaded = True
        self.loaded_excel_path = loaded_path
        self.update_columns_reference_presentation()
        self.update_preview_headers()
        self.update_send_target_column_status()
        self.update_data_info_label()
        self.update_placeholder_status()
        self.update_local_db_status()
        self.update_action_button_state()
        self.render_preview()

    def on_excel_path_changed(self, path: str) -> None:
        normalized_path = path.strip()
        self.config["excel"]["path"] = normalized_path
        if hasattr(self, "basic_excel_path_input") and self.basic_excel_path_input.text().strip() != normalized_path:
            self.basic_excel_path_input.blockSignals(True)
            self.basic_excel_path_input.setText(normalized_path)
            self.basic_excel_path_input.blockSignals(False)
        if normalized_path != self.loaded_excel_path and self.active_source_mode == SOURCE_MODE_FILE:
            self.records_loaded = False
        self.save_config_if_ready()

    def on_template_changed(self) -> None:
        self.template_change_timer.start(500)

    def apply_template_changes(self) -> None:
        self.config["template"]["text"] = self.template_input.toPlainText()
        self.save_config_if_ready()
        if self.current_task_id is not None:
            self.clear_task_snapshot("已修改消息模板，任务快照已失效，请重新从本地库筛选并导入发送计划。")
        self.update_placeholder_status()
        self.update_send_target_column_status()
        self.update_data_info_label()
        self.render_preview()

    def on_send_target_column_changed(self, value: str) -> None:
        self.config["excel"]["send_target_column"] = value.strip() or DEFAULT_SEND_TARGET_COLUMN
        self.save_config_if_ready()
        if self.current_task_id is not None:
            self.clear_task_snapshot("已修改发送识别列，任务快照已失效，请重新从本地库筛选并导入发送计划。")
        self.update_preview_headers()
        self.update_send_target_column_status()
        self.update_data_info_label()
        self.render_preview()

    def on_filter_fields_changed(self, value: str) -> None:
        self.config["filter"]["fields"] = value
        self.save_config_if_ready()

    def on_filter_pattern_changed(self, value: str) -> None:
        self.config["filter"]["pattern"] = value
        self.save_config_if_ready()

    def on_filter_ignore_case_changed(self, checked: bool) -> None:
        self.config["filter"]["ignore_case"] = checked
        self.save_config_if_ready()

    def on_interval_changed(self, value: int) -> None:
        self.config["settings"]["send_interval"] = value
        if hasattr(self, "basic_interval_spin") and self.basic_interval_spin.value() != value:
            self.basic_interval_spin.blockSignals(True)
            self.basic_interval_spin.setValue(value)
            self.basic_interval_spin.blockSignals(False)
        self.save_config_if_ready()

    def on_bulk_send_option_changed(self, *_args) -> None:
        self.config["bulk_send"]["random_delay_min"] = self.random_delay_min_spin.value()
        self.config["bulk_send"]["random_delay_max"] = self.random_delay_max_spin.value()
        self.config["bulk_send"]["operator_name"] = self.operator_name_input.text().strip()
        self.config["bulk_send"]["report_to"] = self.report_to_input.text().strip() or DEFAULT_REPORT_TARGET
        self.config["bulk_send"]["auto_report_enabled"] = self.auto_report_checkbox.isChecked()
        self.config["bulk_send"]["stop_on_error"] = self.is_stop_on_error_enabled()
        self.config["bulk_send"]["debug_mode_enabled"] = self.is_debug_mode_enabled()
        self.config["bulk_send"]["schedule_mode"] = self.get_selected_schedule_mode()
        self.config["bulk_send"]["schedule_value"] = self.schedule_value_input.text().strip() if hasattr(self, "schedule_value_input") else ""
        self.save_config_if_ready()

    def get_selected_schedule_mode(self) -> str:
        if not hasattr(self, "schedule_mode_combo"):
            return SCHEDULE_MODE_ONCE
        return str(self.schedule_mode_combo.currentData() or SCHEDULE_MODE_ONCE)

    def on_schedule_mode_changed(self, *_args) -> None:
        mode = self.get_selected_schedule_mode()
        if mode == SCHEDULE_MODE_ONCE:
            hint = "一次性任务不会自动生成下一次执行。"
        elif mode == SCHEDULE_MODE_DAILY:
            hint = "每天：本次成功后按相同时间顺延 1 天。"
        elif mode == SCHEDULE_MODE_WEEKLY:
            hint = "每周：本次成功后按相同时间顺延 7 天。"
        else:
            hint = "Cron：使用 5 段表达式（分 时 日 月 周），例如 `0 9 * * 1-5`。"
        if hasattr(self, "schedule_mode_hint_label"):
            self.schedule_mode_hint_label.setText(hint)
        if hasattr(self, "schedule_value_input"):
            self.schedule_value_input.setEnabled(mode == SCHEDULE_MODE_CRON)
            if mode != SCHEDULE_MODE_CRON and self.schedule_value_input.text().strip():
                self.schedule_value_input.clear()
        self.on_bulk_send_option_changed()
        self.update_action_button_state()

    def normalize_attachment_items(self, raw_items: Any) -> list[dict[str, str]]:
        if json_task_helper is not None:
            try:
                return [
                    dict(item)
                    for item in json_task_helper.normalize_attachment_list(raw_items, validate_exists=True)
                ]
            except Exception:
                pass

        if raw_items in (None, ""):
            return []
        if isinstance(raw_items, dict):
            raw_items = [raw_items]
        elif isinstance(raw_items, str):
            raw_items = [segment.strip() for segment in raw_items.split(";") if segment.strip()]
        elif not isinstance(raw_items, list):
            raw_items = list(raw_items)

        normalized: list[dict[str, str]] = []
        for item in raw_items:
            if isinstance(item, dict):
                file_path = str(item.get("file_path") or item.get("path") or "").strip()
                file_type = str(item.get("file_type") or "").strip().lower()
            else:
                file_path = str(item or "").strip()
                file_type = ""

            if file_path == "":
                continue
            resolved_path = str(Path(file_path).expanduser().resolve(strict=False))
            if not Path(resolved_path).exists():
                raise ValueError(f"附件不存在：{resolved_path}")
            file_type = file_type or self.infer_attachment_type(resolved_path)
            normalized.append(
                {
                    "file_path": resolved_path,
                    "file_type": file_type,
                }
            )
        return normalized

    def infer_attachment_type(self, file_path: str) -> str:
        suffix = Path(file_path).suffix.lower()
        if suffix == ".pdf":
            return "pdf"
        if suffix in {".jpg", ".jpeg", ".png", ".bmp", ".webp"}:
            return "image"
        return "file"

    def update_columns_reference_presentation(self) -> None:
        if not hasattr(self, "columns_view"):
            return
        columns_text = "、".join(self.columns)
        has_columns = bool(columns_text.strip())
        self.columns_view.blockSignals(True)
        self.columns_view.setPlainText(columns_text)
        self.columns_view.blockSignals(False)
        if hasattr(self, "columns_empty_label"):
            self.columns_empty_label.setVisible(not has_columns)
        self.columns_view.setVisible(has_columns)
        self.columns_view.setFixedHeight(72 if len(self.columns) > 8 else 64)

    def update_common_attachment_presentation(self) -> None:
        if not hasattr(self, "common_attachment_table"):
            return
        attachment_count = len(self.common_attachments)
        if hasattr(self, "common_attachment_status_label"):
            if attachment_count == 0:
                self.common_attachment_status_label.setText("未添加通用附件。")
                self.set_label_tone(self.common_attachment_status_label, "muted")
            else:
                self.common_attachment_status_label.setText(f"已添加 {attachment_count} 个通用附件。")
                self.set_label_tone(self.common_attachment_status_label, "success")
        if hasattr(self, "common_attachment_empty_label"):
            self.common_attachment_empty_label.setVisible(attachment_count == 0)
        self.common_attachment_table.setVisible(attachment_count > 0)
        self.common_attachment_table.setMinimumHeight(120 if attachment_count > 0 else 0)

    def refresh_common_attachment_table(self) -> None:
        if not hasattr(self, "common_attachment_table"):
            return
        self.common_attachment_table.setRowCount(len(self.common_attachments))
        for row_index, item in enumerate(self.common_attachments):
            file_type = str(item.get("file_type") or "")
            file_path = str(item.get("file_path") or "")

            type_bg_color, type_text_color = ATTACHMENT_TYPE_COLORS.get(file_type, ATTACHMENT_TYPE_COLORS.get("file", ("#6b7280", "#ffffff")))
            type_item = QTableWidgetItem(file_type)
            type_item.setBackground(QColor(type_bg_color))
            type_item.setForeground(QColor(type_text_color))
            type_item.setTextAlignment(Qt.AlignCenter)
            type_item.setToolTip(f"类型：{file_type}\n路径：{file_path}")

            path_item = QTableWidgetItem(file_path)
            path_item.setToolTip(file_path)

            self.common_attachment_table.setItem(row_index, 0, type_item)
            self.common_attachment_table.setItem(row_index, 1, path_item)
        self.common_attachment_table.resizeRowsToContents()
        self.remove_common_attachment_button.setEnabled(bool(self.common_attachments))
        self.clear_common_attachment_button.setEnabled(bool(self.common_attachments))
        self.update_common_attachment_presentation()

    def save_common_attachments_to_config(self) -> None:
        self.config["template"]["common_attachments"] = [dict(item) for item in self.common_attachments]
        self.save_config()

    def select_common_attachments(self) -> None:
        start_dir = (
            self.config.get("json_tasks", {}).get("last_attachment_dir")
            or self.config.get("json_tasks", {}).get("last_import_dir")
            or ""
        )
        paths, _ = QFileDialog.getOpenFileNames(
            self,
            "选择通用附件",
            start_dir,
            "所有文件(*.*)",
        )
        if not paths:
            return
        self.config["json_tasks"]["last_attachment_dir"] = str(Path(paths[0]).parent)
        self.save_config()
        self.common_attachment_input.setText(";".join(paths))

    def import_common_attachments_from_input(self) -> None:
        raw_value = self.common_attachment_input.text().strip()
        if raw_value == "":
            QMessageBox.information(self, "未选择附件", "请先选择或拖入至少一个附件。")
            return
        try:
            new_items = self.normalize_attachment_items(raw_value)
        except Exception as exc:
            QMessageBox.warning(self, "附件无效", str(exc))
            return

        existing_paths = {str(item.get("file_path") or "") for item in self.common_attachments}
        for item in new_items:
            file_path = str(item.get("file_path") or "")
            if file_path and file_path not in existing_paths:
                self.common_attachments.append(item)
                existing_paths.add(file_path)
        self.common_attachment_input.clear()
        self.refresh_common_attachment_table()
        self.save_common_attachments_to_config()
        self.append_log(f"已更新通用附件，共 {len(self.common_attachments)} 个。")

    def remove_selected_common_attachments(self) -> None:
        if not self.common_attachments:
            return
        rows = sorted({index.row() for index in self.common_attachment_table.selectionModel().selectedRows()}, reverse=True)
        if not rows:
            QMessageBox.information(self, "未选择附件", "请先在通用附件表中选择要删除的附件。")
            return
        for row in rows:
            if 0 <= row < len(self.common_attachments):
                self.common_attachments.pop(row)
        self.refresh_common_attachment_table()
        self.save_common_attachments_to_config()

    def clear_common_attachments(self) -> None:
        if not self.common_attachments:
            return
        self.common_attachments = []
        self.refresh_common_attachment_table()
        self.save_common_attachments_to_config()

    def extract_row_custom_attachments(self, row: dict[str, Any]) -> list[dict[str, str]]:
        return self.normalize_attachment_items(row.get(ROW_ATTACHMENTS_KEY) or row.get("custom_attachments") or [])

    def build_attachment_summary_text(self, attachments: list[dict[str, str]], *, max_items: int = 3) -> str:
        if not attachments:
            return "未设置附件"

        names = [Path(str(item.get("file_path") or "")).name for item in attachments if str(item.get("file_path") or "").strip()]
        visible_names = names[:max_items]
        if len(names) > max_items:
            visible_names.append(f"... 还有 {len(names) - max_items} 个")
        return "；".join(visible_names)

    def resolve_row_attachments_for_preview(self, row: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
        custom_attachments = self.extract_row_custom_attachments(row)
        if custom_attachments:
            return custom_attachments, "custom"
        return [dict(item) for item in self.common_attachments], "common"

    def get_row_attachment_button_text(self, row: dict[str, Any]) -> str:
        custom_attachments = self.extract_row_custom_attachments(row)
        if custom_attachments:
            return f"附件({len(custom_attachments)})"
        return "附件"

    def get_row_attachment_tooltip(self, row: dict[str, Any]) -> str:
        attachments, attachment_mode = self.resolve_row_attachments_for_preview(row)
        if attachment_mode == "custom":
            return "当前使用自定义附件：\n" + self.build_attachment_summary_text(attachments, max_items=5)
        if attachments:
            return "当前使用通用附件：\n" + self.build_attachment_summary_text(attachments, max_items=5)
        return "当前未设置附件。"

    def set_row_custom_attachments(self, row_index: int, attachments: list[dict[str, str]] | None) -> None:
        if row_index < 0 or row_index >= len(self.records):
            return
        record = self.records[row_index]
        attachment_list = [dict(item) for item in (attachments or [])]
        if attachment_list:
            record[ROW_ATTACHMENT_MODE_KEY] = "custom"
            record[ROW_ATTACHMENTS_KEY] = attachment_list
        else:
            record[ROW_ATTACHMENT_MODE_KEY] = "common"
            record.pop(ROW_ATTACHMENTS_KEY, None)
            record.pop("custom_attachments", None)

        if self.current_task_id is not None:
            task_item_id = record.get(TASK_ITEM_ID_KEY)
            if task_item_id:
                self.local_store.update_task_item(int(str(task_item_id)), record)
        target_name = self.get_display_name(record) or self.get_send_target_value(record) or f"第 {row_index + 1} 行"
        if attachment_list:
            self.append_log(f"已更新 {target_name} 的自定义附件，共 {len(attachment_list)} 个。")
        else:
            self.append_log(f"已将 {target_name} 恢复为使用通用附件。")
        self.render_preview()

    def edit_row_attachments(self, row_index: int) -> None:
        if row_index < 0 or row_index >= len(self.records):
            return
        record = self.records[row_index]
        existing_custom = self.extract_row_custom_attachments(record)
        start_dir = (
            self.config.get("json_tasks", {}).get("last_attachment_dir")
            or self.config.get("json_tasks", {}).get("last_import_dir")
            or ""
        )
        dialog = AttachmentManageDialog(existing_custom, start_dir=start_dir, parent=self)
        if dialog.exec_() != AttachmentManageDialog.Accepted:
            return
        if dialog.use_common_attachments:
            self.set_row_custom_attachments(row_index, [])
            return

        attachments = dialog.get_attachments()
        if attachments:
            self.config["json_tasks"]["last_attachment_dir"] = str(Path(attachments[0]["file_path"]).parent)
            self.save_config()
        self.set_row_custom_attachments(row_index, attachments)

    def choose_json_start_time_text(self) -> str:
        if self.scheduled_mode_radio.isChecked():
            return self.scheduled_time_edit.dateTime().toString("yyyy-MM-dd HH:mm:00")
        return datetime.now().strftime(JSON_TIME_FORMAT)

    def infer_target_type(self, row: dict[str, Any]) -> str:
        explicit_type = str(row.get(ROW_TARGET_TYPE_KEY) or row.get("target_type") or "").strip().lower()
        if explicit_type in {"person", "group"}:
            return explicit_type
        row_type = str(row.get("类型") or "").strip()
        if row_type == "群聊":
            return "group"
        if row_type == "好友":
            return "person"
        username = str(row.get("用户名") or "").strip()
        if username.endswith("@chatroom"):
            return "group"
        if username:
            return "person"
        return ""

    def build_target_payload(self, row: dict[str, Any], *, index: int) -> dict[str, Any]:
        target_value = self.get_send_target_value(row)
        if target_value == "":
            raise ValueError(f"第 {index + 1} 行缺少可发送的目标值。")

        target_type = self.infer_target_type(row)
        if target_type == "":
            raise ValueError(f"第 {index + 1} 行无法识别 target_type，请补齐“类型”列或“用户名”信息。")

        custom_message = str(row.get(CUSTOM_MESSAGE_OVERRIDE_KEY, "") or "")
        message_mode = "custom" if custom_message.strip() else "template"
        custom_attachments = self.extract_row_custom_attachments(row)
        attachment_mode = "custom" if custom_attachments else "common"

        payload = {
            "target_value": target_value,
            "target_type": target_type,
            "message_mode": message_mode,
            "message": custom_message if message_mode == "custom" else "",
            "attachment_mode": attachment_mode,
            "attachments": custom_attachments,
            "display_name": self.get_display_name(row),
            "send_status": str(row.get(ROW_SEND_STATUS_KEY) or "").strip().lower() or "pending",
            "attachment_status": str(row.get(ROW_ATTACHMENT_STATUS_KEY) or "").strip().lower() or "none",
            "error_msg": str(row.get(ROW_ERROR_MSG_KEY) or ""),
            "send_time": str(row.get(ROW_SEND_TIME_KEY) or ""),
            "source_target_index": int(row.get("source_json_index") or row.get("source_target_index") or (index + 1)),
        }
        return payload

    def build_json_task_payload(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        targets = [self.build_target_payload(row, index=index) for index, row in enumerate(records)]
        schedule_mode = self.get_selected_schedule_mode()
        schedule_value = self.schedule_value_input.text().strip() if hasattr(self, "schedule_value_input") else ""
        return {
            "start_time": self.choose_json_start_time_text(),
            "end_time": "",
            "schedule_mode": schedule_mode,
            "schedule_value": schedule_value,
            "total_count": len(targets),
            "template_content": self.template_input.toPlainText(),
            "common_attachments": [dict(item) for item in self.common_attachments],
            "targets": targets,
        }

    def export_current_plan_to_json(self) -> None:
        records, error_message = self.validate_before_send()
        if error_message is not None:
            QMessageBox.warning(self, "无法导出 JSON", error_message)
            return
        assert records is not None
        _schedule_mode, _schedule_value, schedule_error = self.get_schedule_config_for_queue()
        if schedule_error is not None:
            QMessageBox.warning(self, "无法导出 JSON", schedule_error)
            return
        try:
            payload = self.build_json_task_payload(records)
        except Exception as exc:
            QMessageBox.warning(self, "无法导出 JSON", str(exc))
            return

        path = self.build_export_json_path()

        try:
            if json_task_helper is not None:
                json_task_helper.dump_json_task_file(path, payload, create_backup=False)
            else:
                with open(path, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle, ensure_ascii=False, indent=2)
        except Exception as exc:
            QMessageBox.warning(self, "导出失败", f"导出 JSON 失败：{exc}")
            return

        self.config["json_tasks"]["last_export_dir"] = str(Path(path).parent)
        self.save_config()
        self.append_log(f"已导出 JSON：{path}")
        QMessageBox.information(self, "导出成功", f"JSON 已导出到：\n{path}")

    def build_export_json_path(self, now: datetime | None = None) -> str:
        current_time = now or datetime.now()
        base_dir = Path(self.config_path).resolve(strict=False).parent / "task"
        export_dir = base_dir / current_time.strftime("%Y%m") / current_time.strftime("%d")
        file_name = f"{current_time.strftime('%H-%M')}.json"
        return str((export_dir / file_name).resolve(strict=False))

    def import_json_tasks(self) -> None:
        start_dir = self.config.get("json_tasks", {}).get("last_import_dir") or ""
        paths, _ = QFileDialog.getOpenFileNames(
            self,
            "导入 JSON 任务",
            start_dir,
            "JSON 文件(*.json)",
        )
        if not paths:
            return

        self.config["json_tasks"]["last_import_dir"] = str(Path(paths[0]).parent)
        self.save_config()

        preview_items: list[tuple[str, dict[str, Any]]] = []
        skipped_files: list[str] = []
        for path in paths:
            try:
                if json_task_helper is not None:
                    payload = json_task_helper.load_json_task_file(path)
                else:
                    with open(path, "r", encoding="utf-8") as handle:
                        payload = json.load(handle)
                preview_items.append((path, payload))
            except Exception as exc:
                skipped_files.append(f"{Path(path).name}（{exc}）")

        if not preview_items and skipped_files:
            QMessageBox.warning(self, "JSON 导入失败", "\n".join(skipped_files))
            return

        scheduler_was_active = self.scheduler_timer.isActive()
        if scheduler_was_active:
            self.scheduler_timer.stop()

        confirm_lines = [
            f"{Path(path).name} -> 开始时间 {str(payload.get('start_time') or '')}"
            for path, payload in preview_items
        ]
        confirm_parts = [
            "确认导入以下 JSON 任务吗？",
            "\n".join(confirm_lines),
            "说明：导入后会进入任务列表；到 start_time 会自动执行，你也可以提前在任务列表中选中后手动点击“开始发送”。",
        ]
        if skipped_files:
            confirm_parts.append("以下文件预检查失败，不会导入：\n" + "\n".join(skipped_files))

        reply = QMessageBox.question(
            self,
            "确认导入 JSON 任务",
            "\n\n".join(confirm_parts),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            self.append_log("已取消本次 JSON 导入。")
            if scheduler_was_active:
                self.scheduler_timer.start()
            return

        imported_jobs: list[tuple[str, int, int, str]] = []

        try:
            for path, payload in preview_items:
                try:
                    task_id, job_id = self.local_store.create_json_task_from_payload(
                        source_json_path=path,
                        payload=payload,
                        interval_seconds=self.interval_spin.value(),
                        random_delay_min=self.random_delay_min_spin.value(),
                        random_delay_max=self.random_delay_max_spin.value(),
                        operator_name=self.operator_name_input.text().strip(),
                        report_to=self.report_to_input.text().strip() or DEFAULT_REPORT_TARGET,
                        source_mode=SOURCE_MODE_JSON,
                        dataset_type=DATASET_ALL,
                        template_preview=str(payload.get("template_content") or "")[:50],
                        json_writeback_enabled=True,
                    )
                    imported_jobs.append((Path(path).name, task_id, job_id, str(payload.get("start_time") or "")))
                except Exception as exc:
                    skipped_files.append(f"{Path(path).name}（{exc}）")

            self.refresh_scheduled_jobs()
            if imported_jobs:
                self.select_scheduled_job(imported_jobs[0][2])

            if imported_jobs:
                summary_lines = [
                    f"{file_name} -> 任务快照 {task_id} / 定时任务 {job_id} / 开始时间 {start_time}"
                    for file_name, task_id, job_id, start_time in imported_jobs
                ]
                self.append_log("已导入 JSON 任务：" + "；".join(summary_lines))
            if skipped_files:
                self.append_log("以下 JSON 导入失败：" + "；".join(skipped_files))

            message_parts = []
            if imported_jobs:
                message_parts.append(
                    "成功导入（支持自动执行，也可手动提前开始）：\n"
                    + "\n".join(
                        f"{file_name} -> 定时任务 {job_id}（{start_time}）"
                        for file_name, _task_id, job_id, start_time in imported_jobs
                    )
                )
                message_parts.append("下一步：可等待到 start_time 自动执行；若想提前执行，也可以在任务列表中选中后手动点击“开始发送”。")
            if skipped_files:
                message_parts.append("导入失败：\n" + "\n".join(skipped_files))

            if not message_parts:
                message_parts.append("没有导入任何 JSON 任务。")

            self.open_task_center_page()
            QMessageBox.information(self, "JSON 导入结果", "\n\n".join(message_parts))
        finally:
            if scheduler_was_active:
                self.scheduler_timer.start()

    def on_send_mode_changed(self, *_args) -> None:
        is_scheduled = self.scheduled_mode_radio.isChecked()
        self.scheduled_time_edit.setEnabled(is_scheduled)
        self.config["bulk_send"]["send_mode"] = "scheduled" if is_scheduled else "immediate"
        self.save_config_if_ready()
        recurrence_text = self.get_schedule_mode_text(
            self.get_selected_schedule_mode(),
            self.schedule_value_input.text().strip() if hasattr(self, "schedule_value_input") else "",
        )
        if is_scheduled:
            self.set_label_tone(self.schedule_status_label, "warning")
            self.schedule_status_label.setText(
                f"当前为定时发送模式，计划时间：{self.scheduled_time_edit.dateTime().toString('yyyy-MM-dd HH:mm')}，频率：{recurrence_text}。"
            )
        else:
            self.set_label_tone(self.schedule_status_label, "muted")
            self.schedule_status_label.setText(f"当前为立即发送模式（导出/建队列时默认频率：{recurrence_text}）。")
        self.update_action_button_state()

    def refresh_scheduled_jobs(self) -> None:
        previous_job = self.get_selected_scheduled_job() if self.schedule_table.rowCount() > 0 else None
        jobs = self.local_store.list_scheduled_jobs()
        self.json_job_source_paths = {}
        self.schedule_table.setRowCount(len(jobs))
        for row_index, job in enumerate(jobs):
            self.schedule_table.setItem(row_index, 0, QTableWidgetItem(str(job.job_id)))
            self.schedule_table.setItem(row_index, 1, QTableWidgetItem(job.scheduled_at))
            status_item = QTableWidgetItem(self.get_schedule_status_text_for_job(job))
            if job.wait_reason:
                status_item.setToolTip(job.wait_reason)
            self.schedule_table.setItem(row_index, 2, status_item)
            auto_item = QTableWidgetItem(self.get_schedule_enabled_text(job))
            auto_item.setToolTip("开启：到点后允许自动执行；关闭：保留队列记录，但不会自动调度。")
            self.schedule_table.setItem(row_index, 3, auto_item)
            self.schedule_table.setItem(row_index, 4, QTableWidgetItem(str(job.total_count)))
            if job.task_kind == "json":
                source_text = f"JSON:{job.source_json_name or Path(job.source_json_path or '').name}"
            elif job.source_mode == SOURCE_MODE_FILE:
                source_text = "Excel"
            else:
                source_text = job.dataset_label
            source_item = QTableWidgetItem(source_text)
            if job.source_json_path:
                source_item.setToolTip(job.source_json_path)
                self.json_job_source_paths[job.job_id] = job.source_json_path
            self.schedule_table.setItem(row_index, 5, source_item)

            preview_text = job.template_preview or ""
            recurrence_text = self.get_schedule_mode_text(job.schedule_mode, job.schedule_value)
            if recurrence_text != "一次性":
                preview_text = (preview_text + " | " if preview_text else "") + f"频率：{recurrence_text}"
            if not job.enabled:
                preview_text = (preview_text + " | " if preview_text else "") + "自动调度：关闭"
            if job.wait_reason:
                preview_text = (preview_text + " | " if preview_text else "") + job.wait_reason
            self.schedule_table.setItem(row_index, 6, QTableWidgetItem(preview_text))

        self.schedule_table.resizeRowsToContents()
        if previous_job is not None:
            self.select_scheduled_job(previous_job.job_id)
        self.update_action_button_state()

    def get_schedule_status_text(self, status: str) -> str:
        mapping = {
            SCHEDULE_STATUS_PENDING: "待执行",
            SCHEDULE_STATUS_RUNNING: "执行中",
            SCHEDULE_STATUS_COMPLETED: "已完成",
            SCHEDULE_STATUS_CANCELLED: "已取消",
            SCHEDULE_STATUS_FAILED: "失败",
        }
        return mapping.get(status, status)

    def get_schedule_status_text_for_job(self, job: ScheduledSendJob) -> str:
        if str(job.conflict_status or "").strip() == "waiting":
            return "等待中"
        return self.get_schedule_status_text(job.status)

    def get_schedule_enabled_text(self, job: ScheduledSendJob) -> str:
        return "开启" if bool(job.enabled) else "关闭"

    def get_selected_scheduled_job(self) -> ScheduledSendJob | None:
        selection_model = self.schedule_table.selectionModel()
        if selection_model is None:
            return None
        selected_indexes = selection_model.selectedRows()
        if not selected_indexes:
            return None
        job_item = self.schedule_table.item(selected_indexes[0].row(), 0)
        if job_item is None:
            return None
        try:
            selected_job_id = int(job_item.text())
        except (TypeError, ValueError):
            return None
        for job in self.local_store.list_scheduled_jobs(limit=500):
            if job.job_id == selected_job_id:
                return job
        return None

    def select_scheduled_job(self, job_id: int) -> None:
        for row_index in range(self.schedule_table.rowCount()):
            job_item = self.schedule_table.item(row_index, 0)
            if job_item is None:
                continue
            if str(job_item.text()).strip() == str(job_id):
                self.schedule_table.selectRow(row_index)
                self.schedule_table.setCurrentCell(row_index, 0)
                return

    def cancel_selected_scheduled_job(self) -> None:
        selected_indexes = self.schedule_table.selectionModel().selectedRows()
        if not selected_indexes:
            QMessageBox.information(self, "未选择任务", "请先选择要取消的定时任务。")
            return

        cancelled_ids: list[str] = []
        skipped_ids: list[str] = []
        for model_index in selected_indexes:
            row = model_index.row()
            job_item = self.schedule_table.item(row, 0)
            if job_item is None:
                continue
            job_id = int(job_item.text())
            if self.local_store.cancel_scheduled_job(job_id):
                cancelled_ids.append(str(job_id))
            else:
                skipped_ids.append(str(job_id))

        self.refresh_scheduled_jobs()
        if cancelled_ids:
            self.append_log(f"已取消定时任务：{', '.join(cancelled_ids)}")
        if skipped_ids:
            self.append_log(f"以下任务未取消（可能已开始或已结束）：{', '.join(skipped_ids)}")

    def enable_selected_scheduled_job(self) -> None:
        selected_job = self.get_selected_scheduled_job()
        if selected_job is None:
            QMessageBox.information(self, "未选择任务", "请先选择要开启自动调度的队列任务。")
            return
        if selected_job.status != SCHEDULE_STATUS_PENDING:
            QMessageBox.information(self, "无法开启", "只有待执行的队列任务才支持开启自动调度。")
            return
        if bool(selected_job.enabled):
            QMessageBox.information(self, "无需开启", "当前选中的队列任务已经处于自动调度开启状态。")
            return
        if self.local_store.set_scheduled_job_enabled(selected_job.job_id, True):
            self.refresh_scheduled_jobs()
            self.select_scheduled_job(selected_job.job_id)
            self.append_log(f"已开启队列任务 {selected_job.job_id} 的自动调度。")
            return
        QMessageBox.warning(self, "开启失败", "开启自动调度失败，可能任务已开始执行。")

    def disable_selected_scheduled_job(self) -> None:
        selected_job = self.get_selected_scheduled_job()
        if selected_job is None:
            QMessageBox.information(self, "未选择任务", "请先选择要关闭自动调度的队列任务。")
            return
        if selected_job.status != SCHEDULE_STATUS_PENDING:
            QMessageBox.information(self, "无法关闭", "只有待执行的队列任务才支持关闭自动调度。")
            return
        if not bool(selected_job.enabled):
            QMessageBox.information(self, "无需关闭", "当前选中的队列任务已经处于自动调度关闭状态。")
            return
        if self.local_store.set_scheduled_job_enabled(selected_job.job_id, False):
            self.refresh_scheduled_jobs()
            self.select_scheduled_job(selected_job.job_id)
            self.append_log(f"已关闭队列任务 {selected_job.job_id} 的自动调度。")
            return
        QMessageBox.warning(self, "关闭失败", "关闭自动调度失败，可能任务已开始执行。")

    def delete_selected_scheduled_job(self) -> None:
        selection_model = self.schedule_table.selectionModel()
        selected_rows = selection_model.selectedRows() if selection_model is not None else []
        if not selected_rows:
            QMessageBox.information(self, "未选择任务", "请先选择要删除的队列任务。")
            return

        selected_job_ids: list[int] = []
        for model_index in selected_rows:
            job_item = self.schedule_table.item(model_index.row(), 0)
            if job_item is None:
                continue
            try:
                selected_job_ids.append(int(str(job_item.text()).strip()))
            except (TypeError, ValueError):
                continue
        selected_job_ids = list(dict.fromkeys(selected_job_ids))
        if not selected_job_ids:
            QMessageBox.information(self, "未选择任务", "当前没有可删除的有效队列任务。")
            return

        job_map = {job.job_id: job for job in self.local_store.list_scheduled_jobs(limit=500)}
        running_job_ids = [job_id for job_id in selected_job_ids if job_map.get(job_id) is not None and job_map[job_id].status == SCHEDULE_STATUS_RUNNING]
        deletable_job_ids = [job_id for job_id in selected_job_ids if job_id not in running_job_ids]
        if not deletable_job_ids:
            QMessageBox.information(self, "无法删除", "选中的任务都在执行中，不能删除，请等待结束或先停止。")
            return

        reply = QMessageBox.question(
            self,
            "确认删除队列记录",
            (
                f"准备删除 {len(deletable_job_ids)} 个队列任务"
                + (f"（运行中 {len(running_job_ids)} 个会跳过）" if running_job_ids else "")
                + "。\n"
                "仅删除调度记录，不会删除任务快照，也不会删除原始 JSON 文件。\n\n"
                "确认继续吗？"
            ),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        deleted_ids: list[str] = []
        failed_ids: list[str] = []
        for job_id in deletable_job_ids:
            if self.local_store.delete_scheduled_job(job_id):
                deleted_ids.append(str(job_id))
            else:
                failed_ids.append(str(job_id))

        self.refresh_scheduled_jobs()

        if deleted_ids:
            self.append_log(f"已批量删除队列任务：{', '.join(deleted_ids)}（仅调度记录）。")
        if running_job_ids:
            self.append_log(f"以下任务正在执行，已跳过删除：{', '.join(str(job_id) for job_id in running_job_ids)}")
        if failed_ids:
            self.append_log(f"以下任务删除失败：{', '.join(failed_ids)}")

        if failed_ids and not deleted_ids:
            QMessageBox.warning(self, "删除失败", "删除队列记录失败，可能任务已开始执行。")

    def preview_selected_scheduled_job(self) -> None:
        selected_job = self.get_selected_scheduled_job()
        if selected_job is None:
            QMessageBox.information(self, "未选择任务", "请先选择要预览的队列任务。")
            return
        records = self.local_store.load_task_records(selected_job.task_id)
        task_details = self.local_store.get_task_details(selected_job.task_id)
        if not records:
            QMessageBox.information(self, "无可预览内容", "当前任务快照为空。")
            return
        template_text = str(task_details.get("template_text") or "") if task_details is not None else ""
        preview_common_attachments = (
            self.normalize_attachment_items(task_details.get("common_attachments_json") or "[]")
            if task_details is not None
            else []
        )
        dialog = QDialog(self)
        dialog.setWindowTitle(f"预览任务 {selected_job.job_id} 的发送计划")
        dialog.resize(980, 560)
        dialog.setMinimumSize(760, 420)
        layout = QVBoxLayout(dialog)
        summary_label = QLabel(
            f"计划时间：{selected_job.scheduled_at} | 频率：{self.get_schedule_mode_text(selected_job.schedule_mode, selected_job.schedule_value)} | 人数：{selected_job.total_count}"
        )
        self.style_helper_label(summary_label, color="#555")
        layout.addWidget(summary_label)
        table = QTableWidget(len(records), 4, dialog)
        table.setHorizontalHeaderLabels([JSON_HEADER_TITLE, "显示名称", "消息摘要", "附件摘要"])
        table.verticalHeader().setVisible(False)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        for row_index, row in enumerate(records):
            table.setItem(row_index, 0, QTableWidgetItem(self.get_send_target_value(row)))
            table.setItem(row_index, 1, QTableWidgetItem(self.get_display_name(row)))
            if CUSTOM_MESSAGE_OVERRIDE_KEY in row:
                preview_message = str(row.get(CUSTOM_MESSAGE_OVERRIDE_KEY, ""))
            else:
                preview_message = render_template(template_text, row) if template_text else str(row.get("message") or "")
            table.setItem(row_index, 2, QTableWidgetItem(preview_message))
            custom_attachments = self.extract_row_custom_attachments(row)
            attachments = custom_attachments if custom_attachments else [dict(item) for item in preview_common_attachments]
            table.setItem(row_index, 3, QTableWidgetItem(self.build_attachment_summary_text(attachments, max_items=4)))
        self.configure_resizable_table_columns(
            table,
            initial_widths=[180, 180, 420, 260],
            signature="scheduled_job_preview_dialog",
            min_section_size=80,
        )
        table.resizeRowsToContents()
        layout.addWidget(table, stretch=1)
        button_box = QDialogButtonBox(QDialogButtonBox.Close, dialog)
        button_box.rejected.connect(dialog.reject)
        button_box.accepted.connect(dialog.accept)
        layout.addWidget(button_box)
        dialog.exec_()

    def continue_sending(self) -> None:
        if self.send_thread is not None and self.send_thread.isRunning():
            QMessageBox.information(self, "发送中", "当前已有发送任务正在执行。")
            return
        resume_context = self.get_resume_context()
        if resume_context is None:
            QMessageBox.information(self, "无需继续", "当前没有可继续发送的未发送项。")
            return
        task_id, selected_job, records, source_label = resume_context
        if not records:
            QMessageBox.information(self, "无需继续", "当前任务没有剩余未发送项。")
            return
        task_details = self.local_store.get_task_details(task_id)
        if task_details is None:
            QMessageBox.warning(self, "无法继续", "任务快照不存在。")
            return
        reply = QMessageBox.question(
            self,
            "确认继续发送",
            f"准备继续 {source_label}，剩余未发送 {len(records)} 项。\n失败项不会自动重试，确认继续吗？",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return
        self.current_task_id = task_id
        self.records = self.local_store.load_task_records(task_id)
        self.render_preview()
        if selected_job is not None:
            self.local_store.mark_scheduled_job_running(selected_job.job_id)
            self.local_store.clear_job_waiting_conflict(selected_job.job_id)
            self.refresh_scheduled_jobs()
        self.launch_send_thread(
            records=records,
            template_text=str(task_details.get("template_text") or ""),
            target_column=TARGET_VALUE_KEY,
            interval_seconds=selected_job.interval_seconds if selected_job is not None else self.interval_spin.value(),
            random_delay_min=selected_job.random_delay_min if selected_job is not None else self.random_delay_min_spin.value(),
            random_delay_max=selected_job.random_delay_max if selected_job is not None else self.random_delay_max_spin.value(),
            operator_name=selected_job.operator_name if selected_job is not None else self.operator_name_input.text().strip(),
            report_to=(selected_job.report_to if selected_job is not None else self.report_to_input.text().strip()) or DEFAULT_REPORT_TARGET,
            auto_report=selected_job is not None or self.auto_report_checkbox.isChecked(),
            scheduled_job=selected_job,
        )

    def poll_scheduled_jobs(self) -> None:
        due_jobs = self.local_store.get_due_scheduled_jobs(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), limit=20)
        if not due_jobs:
            return

        if self.send_thread is not None and self.send_thread.isRunning():
            for due_job in due_jobs:
                wait_reason = f"任务 {due_job.job_id} 到点，但当前仍有任务执行中，已进入等待队列。"
                should_notify = not bool(due_job.conflict_notified)
                self.local_store.mark_job_waiting_conflict(due_job.job_id, wait_reason, notify=should_notify)
                if should_notify:
                    self.append_log(wait_reason)
            self.refresh_scheduled_jobs()
            return

        self.execute_scheduled_job(due_jobs[0])

    def select_excel_file(self) -> None:
        path = QFileDialog.getOpenFileName(
            self,
            "选择 Excel 文件",
            "",
            "表格文件(*.xlsx *.xls *.csv)",
        )[0]
        if path:
            self.excel_path_input.setText(path)

    def load_excel_data(self, show_success: bool = True) -> bool:
        path = self.excel_path_input.text().strip()
        if path == "":
            if show_success:
                QMessageBox.warning(self, "输入错误", "请先选择 Excel 文件。")
            return False

        try:
            records, columns = load_contact_records(path)
            validate_contact_records(records, columns, required_column=None)
        except Exception as exc:
            self.records = []
            self.source_records = []
            self.filtered_records = []
            self.columns = []
            self.records_loaded = False
            self.loaded_excel_path = ""
            self.current_batch_id = None
            self.current_batch_ids = {}
            self.current_task_id = None
            self.active_source_mode = SOURCE_MODE_FILE
            self.update_columns_reference_presentation()
            self.data_info_label.setText("读取失败。")
            self.set_label_tone(self.filter_status_label, "warning")
            self.filter_status_label.setText("筛选配置待重新应用。")
            self.update_send_target_column_status()
            self.update_preview_headers()
            self.update_placeholder_status()
            self.update_local_db_status()
            self.update_action_button_state()
            self.render_preview()
            QMessageBox.warning(self, "读取失败", f"读取 Excel 失败！\n错误信息：{exc}")
            return False

        self.load_records_into_view(
            records=records,
            columns=columns,
            source_mode=SOURCE_MODE_FILE,
            loaded_path=path,
            batch_id=None,
        )
        self.set_label_tone(self.filter_status_label, "muted")
        self.filter_status_label.setText("文件读取完成；如需筛选，请先导入到本地库，再到“本地库数据”页创建发送计划。")

        if show_success:
            QMessageBox.information(self, "读取成功", f"已成功读取 {len(self.records)} 行 Excel 数据。")
        return True

    def import_excel_to_local_store(self) -> None:
        path = self.excel_path_input.text().strip()
        if path == "":
            QMessageBox.warning(self, "输入错误", "请先选择 Excel/CSV 文件。")
            return

        try:
            records, columns = load_contact_records(path)
            validate_contact_records(records, columns, required_column=None)
            summaries = self.local_store.import_contacts(path, records, columns)
        except Exception as exc:
            QMessageBox.warning(self, "导入失败", f"导入到本地库失败！\n错误信息：{exc}")
            return

        if not summaries:
            QMessageBox.warning(self, "导入失败", "没有识别到可导入的好友或群聊数据。")
            return

        self.update_local_db_status()
        self.refresh_local_store_page()
        summary_lines = [
            f"{summary.dataset_label}：{summary.source_name}，共 {summary.row_count} 行，批次ID={summary.batch_id}"
            for summary in summaries
        ]
        self.append_log("已导入本地库：" + "；".join(summary_lines) + "。")
        QMessageBox.information(
            self,
            "导入成功",
            "已导入到本地库。\n" + "\n".join(summary_lines) + "\n\n可前往“本地库数据”页查看、筛选并导入发送计划。",
        )
        self.main_tabs.setCurrentWidget(self.local_store_page)

    def load_local_contacts(self, show_success: bool = True) -> bool:
        summaries = self.local_store.get_current_import_summaries()
        if not summaries:
            if show_success:
                QMessageBox.information(self, "本地库为空", "本地库还没有导入记录，请先导入 Excel/CSV。")
            return False

        try:
            records, columns, batch_ids = self.local_store.load_all_current_contacts()
        except Exception as exc:
            QMessageBox.warning(self, "读取失败", f"读取本地库失败！\n错误信息：{exc}")
            return False

        if not records:
            if show_success:
                QMessageBox.information(self, "本地库为空", "当前本地库批次没有可用联系人。")
            return False

        self.load_records_into_view(
            records=records,
            columns=columns,
            source_mode=SOURCE_MODE_LOCAL_DB,
            loaded_path="",
            batch_id=None,
            batch_ids=batch_ids,
        )
        self.ensure_local_filter_defaults()
        self.set_label_tone(self.filter_status_label, "muted")
        self.filter_status_label.setText("已读取本地库数据，可在本页设置条件并导入发送计划。")
        self.refresh_local_store_page()

        if show_success:
            summary_text = "\n".join(
                f"{summary.dataset_label}：{summary.source_name}（{summary.row_count} 行）"
                for summary in summaries.values()
            )
            QMessageBox.information(
                self,
                "读取成功",
                f"已加载本地库当前批次：\n{summary_text}",
            )
        return True

    def reset_local_filter_inputs(self) -> None:
        dataset_type = self.get_active_local_dataset_type()
        summary = self.local_store.get_current_import_summary(dataset_type)
        if summary is not None:
            default_fields = [field for field in DEFAULT_LOCAL_FILTER_FIELDS if field in summary.columns]
            if default_fields:
                self.filter_fields_input.setText(",".join(default_fields))
        self.filter_pattern_input.setText("")
        self.set_label_tone(self.filter_status_label, "muted")
        self.filter_status_label.setText(
            f"已重置{DATASET_LABELS[dataset_type]}库筛选条件，请重新筛选并导入发送计划。"
        )

    def prepare_local_filter_candidates(
        self,
    ) -> tuple[str, int, list[str], list[dict[str, str]], list[dict[str, str]], str, str] | None:
        dataset_type = self.get_active_local_dataset_type()
        summary = self.local_store.get_current_import_summary(dataset_type)
        if summary is None:
            QMessageBox.information(
                self,
                "本地库为空",
                f"{DATASET_LABELS[dataset_type]}库当前没有可用批次，请先导入对应 Excel/CSV。",
            )
            return None

        try:
            records, columns, batch_id = self.local_store.load_current_contacts(dataset_type)
        except Exception as exc:
            QMessageBox.warning(self, "读取失败", f"读取本地库失败！\n错误信息：{exc}")
            return None

        if batch_id is None or not records:
            QMessageBox.information(
                self,
                "无可用数据",
                f"{DATASET_LABELS[dataset_type]}库当前批次没有可筛选的数据。",
            )
            return None

        source_records = self.attach_record_ids(records)
        available_fields = [field for field in DEFAULT_LOCAL_FILTER_FIELDS if field in columns]
        fields = [field.strip() for field in self.filter_fields_input.text().split(",") if field.strip()]
        if not fields or not any(field in columns for field in fields):
            fields = available_fields or list(columns[:1])
            if fields:
                self.filter_fields_input.setText(",".join(fields))

        missing_fields = [field for field in fields if field not in columns]
        if missing_fields:
            QMessageBox.warning(
                self,
                "筛选配置",
                f"筛选字段不存在：{', '.join(missing_fields)}\n请检查列名是否与本地库当前批次一致。",
            )
            return None

        pattern_text = self.filter_pattern_input.text().strip()
        filtered_records = list(source_records)
        if pattern_text:
            flags = re.IGNORECASE if self.filter_ignore_case_checkbox.isChecked() else 0
            try:
                regex = re.compile(pattern_text, flags)
            except re.error as exc:
                QMessageBox.warning(self, "筛选配置", f"正则规则无效：{exc}")
                return None

            filtered_records = [
                record for record in source_records
                if self.record_matches_regex(record, fields, regex)
            ]

        status_text = (
            f"{DATASET_LABELS[dataset_type]}库筛选完成：字段={', '.join(fields)}，命中 {len(filtered_records)} / {len(source_records)} 行。"
            if pattern_text
            else f"未填写筛选规则，已读取{DATASET_LABELS[dataset_type]}库当前批次全部 {len(filtered_records)} 行。"
        )
        return dataset_type, batch_id, columns, source_records, filtered_records, ",".join(fields), pattern_text

    def filter_local_store_into_task(self) -> None:
        prepared = self.prepare_local_filter_candidates()
        if prepared is None:
            return

        dataset_type, batch_id, columns, source_records, filtered_records, filter_fields_text, pattern_text = prepared
        if not filtered_records:
            self.set_label_tone(self.filter_status_label, "warning")
            self.filter_status_label.setText(
                f"{DATASET_LABELS[dataset_type]}库筛选结果为空，请调整筛选字段或正则规则。"
            )
            QMessageBox.information(self, "无命中结果", "当前筛选条件没有命中任何联系人。")
            return

        confirm_rows: list[dict[str, str]] = []
        for row in filtered_records:
            confirm_row = dict(row)
            confirm_row["_search_key"] = self.resolve_local_db_target_value(confirm_row)
            confirm_rows.append(confirm_row)

        dialog = ContactConfirmDialog(confirm_rows, parent=self)
        dialog.setWindowTitle(f"{DATASET_LABELS[dataset_type]}筛选结果确认")
        dialog.ok_btn.setText("导入发送计划")
        if dialog.exec_() != ContactConfirmDialog.Accepted:
            self.set_label_tone(self.filter_status_label, "muted")
            self.filter_status_label.setText(
                f"{DATASET_LABELS[dataset_type]}库筛选已生成候选名单，但尚未导入发送计划。"
            )
            return

        confirmed_rows = dialog.get_confirmed_contacts()
        if not confirmed_rows:
            QMessageBox.information(self, "无选中", "没有勾选任何联系人，未生成发送计划。")
            return

        task_rows: list[dict[str, str]] = []
        for row in confirmed_rows:
            task_row = dict(row)
            task_row.pop("_search_key", None)
            task_row[TARGET_VALUE_KEY] = self.resolve_local_db_target_value(task_row)
            task_rows.append(task_row)

        task_id = self.local_store.create_task_snapshot(
            rows=task_rows,
            filter_fields=filter_fields_text,
            filter_pattern=pattern_text,
            target_column=self.get_send_target_column(),
            template_text=self.template_input.toPlainText(),
            source_batch_id=batch_id,
            source_mode=SOURCE_MODE_LOCAL_DB,
            dataset_type=dataset_type,
        )

        self.active_source_mode = SOURCE_MODE_LOCAL_DB
        self.current_batch_id = batch_id
        self.current_batch_ids = {dataset_type: batch_id}
        self.current_task_id = task_id
        self.source_records = list(source_records)
        self.filtered_records = list(filtered_records)
        self.records = self.local_store.load_task_records(task_id)
        self.columns = list(columns)
        self.records_loaded = True
        self.loaded_excel_path = ""
        self.update_columns_reference_presentation()
        self.set_label_tone(self.filter_status_label, "success")
        self.filter_status_label.setText(
            f"{DATASET_LABELS[dataset_type]}库候选名单已导入发送计划：筛选 {len(filtered_records)} 行，最终确认 {len(self.records)} 行，任务快照 ID={task_id}。"
        )
        self.update_preview_headers()
        self.update_data_info_label()
        self.update_send_target_column_status()
        self.update_placeholder_status()
        self.update_local_db_status()
        self.update_action_button_state()
        self.render_preview()
        self.append_log(
            f"已从{DATASET_LABELS[dataset_type]}库导入发送计划：任务快照 {task_id}，候选 {len(filtered_records)} 行，确认 {len(self.records)} 行。"
        )
        QMessageBox.information(
            self,
            "发送计划已创建",
            f"已从{DATASET_LABELS[dataset_type]}库导入发送计划。\n任务快照 ID：{task_id}\n候选人数：{len(filtered_records)}\n确认人数：{len(self.records)}",
        )
        self.open_send_prepare_page()

    def attach_record_ids(self, records: list[dict[str, str]]) -> list[dict[str, str]]:
        normalized_records: list[dict[str, str]] = []
        for index, row in enumerate(records, start=1):
            normalized_row = dict(row)
            normalized_row[RECORD_ID_KEY] = f"row-{index}"
            normalized_records.append(normalized_row)
        return normalized_records

    def update_data_info_label(self) -> None:
        if not self.records_loaded and not self.records:
            self.data_info_label.setText("尚未读取 Excel 数据。")
            self.update_execution_overview_label()
            return

        source_total = len(self.source_records) if self.source_records else len(self.records)
        filtered_total = len(self.filtered_records) if self.filtered_records else len(self.records)
        current_total = len(self.records)
        valid_count = len([row for row in self.records if self.get_send_target_value(row)])

        if self.current_task_id is not None:
            text = f"本地数据 {source_total} 行｜筛选 {filtered_total} 行｜快照 {current_total} 行｜可发送 {valid_count} 行"
        elif current_total != source_total:
            text = f"已读取 {source_total} 行｜当前 {current_total} 行｜可发送 {valid_count} 行"
        elif self.is_local_db_mode():
            text = f"本地库 {current_total} 行｜可发送 {valid_count} 行"
        else:
            text = f"已读取 {current_total} 行｜“{self.get_send_target_column()}”有效 {valid_count} 行"

        self.data_info_label.setText(text)
        self.update_execution_overview_label()

    def update_execution_overview_label(self) -> None:
        if not hasattr(self, "execution_overview_label"):
            return

        if not self.records_loaded and not self.records:
            self.execution_overview_label.setText("发送计划尚未准备好，请先读取数据或从本地库导入发送计划。")
            return

        current_total = len(self.records)
        valid_count = len([row for row in self.records if self.get_send_target_value(row)])
        if self.active_source_mode == SOURCE_MODE_JSON:
            source_label = "JSON 任务"
        elif self.current_task_id is not None:
            source_label = "本地库任务快照"
        elif self.is_local_db_mode():
            source_label = "本地库数据"
        else:
            source_label = "Excel/文件"

        text = f"{source_label}：当前计划 {current_total} 行，可发送 {valid_count} 行。"
        if self.current_task_id is not None:
            text += f" 任务快照 ID={self.current_task_id}。"
        self.execution_overview_label.setText(text)

    def get_send_target_column(self) -> str:
        return self.send_target_column_input.text().strip() or DEFAULT_SEND_TARGET_COLUMN

    def resolve_local_db_target_value(self, row: dict[str, str]) -> str:
        selected_column = self.get_send_target_column()
        selected_value = (row.get(selected_column) or "").strip()
        if selected_value:
            return selected_value

        contact_type = (row.get("类型") or "").strip()
        priorities = GROUP_SEARCH_PRIORITY if contact_type == "群聊" else FRIEND_SEARCH_PRIORITY
        for field in priorities:
            value = (row.get(field) or "").strip()
            if value:
                return value
        return ""

    def get_send_target_value(self, row: dict[str, str]) -> str:
        if TARGET_VALUE_KEY in row:
            return str(row.get(TARGET_VALUE_KEY) or "").strip()
        if self.is_local_db_mode():
            return self.resolve_local_db_target_value(row)
        return (row.get(self.get_send_target_column()) or "").strip()

    def update_preview_headers(self) -> None:
        if not hasattr(self, "preview_table"):
            return
        if self.active_source_mode == SOURCE_MODE_JSON:
            first_header = JSON_HEADER_TITLE
        elif self.is_local_db_mode():
            first_header = LOCAL_DB_HEADER_TITLE
        else:
            first_header = self.get_send_target_column()
        self.preview_table.setHorizontalHeaderLabels([first_header, "显示名称", "发送消息", "操作"])

    def update_send_target_column_status(self) -> None:
        if not hasattr(self, "send_target_status_label"):
            return

        target_column = self.get_send_target_column()
        if not self.columns:
            self.set_label_tone(self.send_target_status_label, "muted")
            self.send_target_status_label.setText(f"默认按“{target_column}”搜索。")
            return

        valid_count = len([row for row in self.records if self.get_send_target_value(row)])
        if self.is_local_db_mode():
            if target_column in self.columns:
                self.set_label_tone(self.send_target_status_label, "success")
                self.send_target_status_label.setText(f"识别列：{target_column}｜已匹配 {valid_count} 行")
                return

            self.set_label_tone(self.send_target_status_label, "warning")
            self.send_target_status_label.setText(f"识别列：{target_column}｜已回退匹配 {valid_count} 行")
            return

        if target_column not in self.columns:
            self.set_label_tone(self.send_target_status_label, "danger")
            self.send_target_status_label.setText(f"识别列：{target_column}｜Excel 中缺少此列")
            return

        self.set_label_tone(self.send_target_status_label, "success")
        self.send_target_status_label.setText(f"识别列：{target_column}｜可发送 {valid_count} 行")

    def apply_regex_filter(self) -> None:
        if not self.records_loaded:
            if self.is_local_db_mode():
                if not self.load_local_contacts(show_success=False):
                    return
            elif not self.load_excel_data(show_success=False):
                return

        pattern_text = self.filter_pattern_input.text().strip()
        if pattern_text == "":
            QMessageBox.warning(self, "筛选配置", "请输入正则规则后再应用筛选。")
            return

        fields = [field.strip() for field in self.filter_fields_input.text().split(",") if field.strip()]
        if not fields:
            fields = list(DEFAULT_LOCAL_FILTER_FIELDS) if self.is_local_db_mode() else ["场景"]
            self.filter_fields_input.setText(",".join(fields))

        missing_fields = [field for field in fields if field not in self.columns]
        if missing_fields:
            QMessageBox.warning(
                self,
                "筛选配置",
                f"筛选字段不存在：{', '.join(missing_fields)}\n请检查列名是否与 Excel 中一致。",
            )
            return

        flags = re.IGNORECASE if self.filter_ignore_case_checkbox.isChecked() else 0
        try:
            regex = re.compile(pattern_text, flags)
        except re.error as exc:
            QMessageBox.warning(self, "筛选配置", f"正则规则无效：{exc}")
            return

        self.clear_task_snapshot("已重新应用筛选，原任务快照已失效，请重新从本地库筛选并导入发送计划。")
        self.filtered_records = [
            record for record in self.source_records
            if self.record_matches_regex(record, fields, regex)
        ]
        self.records = list(self.filtered_records)
        self.update_data_info_label()
        self.update_send_target_column_status()
        self.update_action_button_state()
        self.set_label_tone(self.filter_status_label, "success")
        self.filter_status_label.setText(
            f"已应用正则筛选：字段={', '.join(fields)}，命中 {len(self.records)} / {len(self.source_records)} 行。"
        )
        self.append_log(
            f"已应用正则筛选：字段={', '.join(fields)}，规则={pattern_text}，命中 {len(self.records)} 行。"
        )
        self.render_preview()

    def reset_regex_filter(self) -> None:
        if not self.records_loaded:
            return

        self.clear_task_snapshot("已重置筛选，原任务快照已失效，请重新从本地库筛选并导入发送计划。")
        self.filtered_records = list(self.source_records)
        self.records = list(self.filtered_records)
        self.update_data_info_label()
        self.update_send_target_column_status()
        self.update_action_button_state()
        self.set_label_tone(self.filter_status_label, "muted")
        self.filter_status_label.setText("已重置筛选，当前显示全部发送数据。")
        self.append_log("已重置正则筛选，恢复全部发送数据。")
        self.render_preview()

    def confirm_current_selection(self) -> None:
        if not self.local_store.get_current_import_summaries():
            QMessageBox.information(self, "本地库为空", "请先导入 Excel/CSV 到本地库，再创建发送计划。")
            return
        self.filter_local_store_into_task()

    def record_matches_regex(self, record: dict[str, str], fields: list[str], regex: re.Pattern[str]) -> bool:
        for field in fields:
            value = record.get(field, "")
            value_text = "" if value is None else str(value)
            if regex.search(value_text):
                return True
        return False

    def update_placeholder_status(self) -> None:
        template = self.template_input.toPlainText()
        placeholders = extract_placeholders(template)
        if not placeholders:
            self.set_label_tone(self.placeholder_status_label, "muted")
            self.placeholder_status_label.setText("未使用占位符。")
            return

        missing_fields = find_missing_fields(placeholders, self.columns)
        text = f"占位符：{', '.join(placeholders)}"
        if missing_fields:
            self.set_label_tone(self.placeholder_status_label, "danger")
            self.placeholder_status_label.setText(text + f"｜缺列：{', '.join(missing_fields)}")
        else:
            self.set_label_tone(self.placeholder_status_label, "success")
            self.placeholder_status_label.setText(text + "｜全部可用")

    def render_preview(self) -> None:
        self._updating_preview_table = True
        self.preview_table.blockSignals(True)
        self.preview_table.setRowCount(0)
        if self.can_edit_preview_rows():
            self.preview_table.setEditTriggers(
                QAbstractItemView.DoubleClicked
                | QAbstractItemView.EditKeyPressed
                | QAbstractItemView.SelectedClicked
            )
        else:
            self.preview_table.setEditTriggers(QAbstractItemView.NoEditTriggers)

        if not self.records:
            self.preview_table.blockSignals(False)
            self._updating_preview_table = False
            return

        plan_records = self.records
        self.preview_table.setRowCount(len(plan_records))
        allow_edit = self.can_edit_preview_rows()

        for row_index, row in enumerate(plan_records):
            target_value = self.get_send_target_value(row)
            display_name = self.get_display_name(row)
            preview_message = self.get_preview_message(row)

            wechat_item = QTableWidgetItem(target_value)
            wechat_item.setToolTip(target_value)
            display_item = QTableWidgetItem(display_name)
            display_item.setToolTip(display_name)

            message_item = QTableWidgetItem(preview_message)
            message_item.setToolTip(preview_message if preview_message else "无消息内容")

            if CUSTOM_MESSAGE_OVERRIDE_KEY in row:
                message_item.setBackground(QColor("#fef3c7"))
            elif self.template_input.toPlainText():
                message_item.setBackground(QColor("#d1fae5"))

            if not allow_edit:
                readonly_flags = wechat_item.flags() & ~Qt.ItemIsEditable
                wechat_item.setFlags(readonly_flags)
                display_item.setFlags(display_item.flags() & ~Qt.ItemIsEditable)
                message_item.setFlags(message_item.flags() & ~Qt.ItemIsEditable)

            self.preview_table.setItem(row_index, 0, wechat_item)
            self.preview_table.setItem(row_index, 1, display_item)
            self.preview_table.setItem(row_index, 2, message_item)

            operation_widget = QWidget(self.preview_table)
            operation_layout = QHBoxLayout(operation_widget)
            operation_layout.setContentsMargins(0, 0, 0, 0)
            operation_layout.setSpacing(6)

            attachment_button = QPushButton(self.get_row_attachment_button_text(row), operation_widget)
            attachment_button.clicked.connect(lambda _, index=row_index: self.edit_row_attachments(index))
            attachment_button.setEnabled(allow_edit)
            attachment_button.setToolTip(self.get_row_attachment_tooltip(row))
            operation_layout.addWidget(attachment_button)

            delete_button = QPushButton("删除", operation_widget)
            delete_button.clicked.connect(lambda _, index=row_index: self.delete_preview_row(index))
            delete_button.setEnabled(allow_edit)
            operation_layout.addWidget(delete_button)
            operation_layout.addStretch(1)
            self.preview_table.setCellWidget(row_index, 3, operation_widget)

        self.preview_table.resizeRowsToContents()
        self.preview_table.blockSignals(False)
        self._updating_preview_table = False

    def show_preview_results(self) -> None:
        self.render_preview()
        self.open_send_prepare_page()

    def get_display_name(self, row: dict[str, str]) -> str:
        override_value = (row.get(DISPLAY_NAME_OVERRIDE_KEY) or "").strip()
        if override_value:
            return override_value
        for field in ("显示名称", "备注", "昵称", "姓名"):
            value = (row.get(field) or "").strip()
            if value:
                return value
        return ""

    def get_preview_message(self, row: dict[str, str]) -> str:
        if CUSTOM_MESSAGE_OVERRIDE_KEY in row:
            return str(row.get(CUSTOM_MESSAGE_OVERRIDE_KEY, ""))

        template = self.template_input.toPlainText()
        return render_template(template, row) if template else ""

    def on_preview_item_changed(self, item: QTableWidgetItem) -> None:
        if self._updating_preview_table or not self.can_edit_preview_rows():
            return

        row_index = item.row()
        if row_index >= len(self.records):
            return

        record = self.records[row_index]
        text = item.text()

        if item.column() == 0:
            if self.current_task_id is not None:
                record[TARGET_VALUE_KEY] = text.strip()
            else:
                record[self.get_send_target_column()] = text.strip()
        elif item.column() == 1:
            record[DISPLAY_NAME_OVERRIDE_KEY] = text.strip()
        elif item.column() == 2:
            record[CUSTOM_MESSAGE_OVERRIDE_KEY] = text
        else:
            return

        if self.current_task_id is not None:
            task_item_id = record.get(TASK_ITEM_ID_KEY)
            if task_item_id:
                self.local_store.update_task_item(int(str(task_item_id)), record)
        self.update_data_info_label()
        self.update_send_target_column_status()

        self.preview_table.resizeRowsToContents()

    def delete_preview_row(self, row_index: int) -> None:
        if self.send_thread is not None and self.send_thread.isRunning():
            return

        if not self.can_edit_preview_rows():
            return

        if row_index >= len(self.records):
            return

        deleted_record = self.records.pop(row_index)
        if self.current_task_id is not None:
            task_item_id = deleted_record.get(TASK_ITEM_ID_KEY)
            if task_item_id:
                self.local_store.delete_task_item(int(str(task_item_id)))
        else:
            record_id = deleted_record.get(RECORD_ID_KEY)
            if record_id:
                self.source_records = [
                    record for record in self.source_records
                    if record.get(RECORD_ID_KEY) != record_id
                ]
                self.filtered_records = [
                    record for record in self.filtered_records
                    if record.get(RECORD_ID_KEY) != record_id
                ]
                self.records = [
                    record for record in self.records
                    if record.get(RECORD_ID_KEY) != record_id
                ]
        deleted_name = self.get_display_name(deleted_record) or self.get_send_target_value(deleted_record)
        self.update_data_info_label()
        self.update_send_target_column_status()
        self.update_local_db_status()
        self.update_action_button_state()
        self.append_log(f"已从发送名单中删除：{deleted_name}")
        self.render_preview()

    def get_current_dataset_type(self, records: list[dict[str, str]] | None = None) -> str:
        rows = records if records is not None else self.records
        if self.active_source_mode != SOURCE_MODE_LOCAL_DB:
            return ""

        dataset_types = set()
        for row in rows:
            row_dataset = str(row.get("__dataset_type") or "").strip()
            if row_dataset:
                dataset_types.add(row_dataset)
                continue
            row_type = str(row.get("类型") or "").strip()
            dataset_types.add(DATASET_GROUP if row_type == "群聊" else DATASET_FRIEND)

        if not dataset_types:
            return ""
        if len(dataset_types) == 1:
            return next(iter(dataset_types))
        return DATASET_ALL

    def get_source_batch_id_for_snapshot(self, dataset_type: str) -> int | None:
        if self.current_batch_id is not None:
            return self.current_batch_id
        if dataset_type in (DATASET_FRIEND, DATASET_GROUP):
            return self.current_batch_ids.get(dataset_type)
        return None

    def build_snapshot_rows(
        self,
        records: list[dict[str, str]],
        *,
        target_column: str | None = None,
    ) -> list[dict[str, str]]:
        snapshot_rows: list[dict[str, str]] = []
        for row in records:
            snapshot_row = dict(row)
            if target_column and target_column in snapshot_row:
                snapshot_row[TARGET_VALUE_KEY] = str(snapshot_row.get(target_column) or "").strip()
            elif TARGET_VALUE_KEY in snapshot_row:
                snapshot_row[TARGET_VALUE_KEY] = str(snapshot_row.get(TARGET_VALUE_KEY) or "").strip()
            else:
                snapshot_row[TARGET_VALUE_KEY] = self.get_send_target_value(snapshot_row)
            snapshot_rows.append(snapshot_row)
        return snapshot_rows

    def create_task_snapshot_from_records(
        self,
        records: list[dict[str, str]],
        *,
        filter_fields: str | None = None,
        filter_pattern: str | None = None,
        target_column: str | None = None,
        template_text: str | None = None,
        source_batch_id: int | None | object = ...,
        source_mode: str | None = None,
        dataset_type: str | None = None,
        common_attachments: list[dict[str, str]] | None = None,
    ) -> int:
        resolved_dataset_type = self.get_current_dataset_type(records) if dataset_type is None else dataset_type
        resolved_target_column = target_column or self.get_send_target_column()
        snapshot_rows = self.build_snapshot_rows(records, target_column=resolved_target_column)
        if source_batch_id is ...:
            resolved_source_batch_id = self.get_source_batch_id_for_snapshot(resolved_dataset_type)
        else:
            resolved_source_batch_id = source_batch_id
        return self.local_store.create_task_snapshot(
            rows=snapshot_rows,
            filter_fields=self.filter_fields_input.text().strip() if filter_fields is None else filter_fields,
            filter_pattern=self.filter_pattern_input.text().strip() if filter_pattern is None else filter_pattern,
            target_column=resolved_target_column,
            template_text=self.template_input.toPlainText() if template_text is None else template_text,
            source_batch_id=resolved_source_batch_id,
            source_mode=self.active_source_mode if source_mode is None else source_mode,
            dataset_type=resolved_dataset_type,
            common_attachments=common_attachments,
        )

    def build_template_preview(self, records: list[dict[str, str]]) -> str:
        for row in records:
            preview = self.get_preview_message(row).strip()
            if preview:
                return preview[:50]
        return self.template_input.toPlainText().strip()[:50]

    def validate_before_send(self) -> tuple[list[dict[str, str]] | None, str | None]:
        if self.is_local_db_mode():
            if not self.records_loaded and not self.load_local_contacts(show_success=False):
                return None, "本地库数据读取失败。"
            if self.current_task_id is None:
                return None, "请先从本地库筛选并导入发送计划，再开始发送。"
            records = self.local_store.load_task_records(self.current_task_id)
            if not records:
                return None, "当前任务快照为空，请重新从本地库筛选并导入发送计划。"
        else:
            if not self.records_loaded and not self.load_excel_data(show_success=False):
                return None, "Excel 数据读取失败。"
            records = list(self.records)

        template = self.template_input.toPlainText()
        if template.strip() == "":
            has_custom_messages = any(
                str(row.get(CUSTOM_MESSAGE_OVERRIDE_KEY, "")).strip()
                for row in records
                if CUSTOM_MESSAGE_OVERRIDE_KEY in row
            )
            has_any_attachments = bool(self.common_attachments) or any(
                bool(row.get(ROW_ATTACHMENTS_KEY))
                for row in records
            )
            if not has_custom_messages and not has_any_attachments:
                return None, "请输入要发送的消息模板，或先在预览表中手动编辑每行消息 / 附件。"
        else:
            placeholders = extract_placeholders(template)
            missing_fields = find_missing_fields(placeholders, self.columns)
            if missing_fields:
                return None, f"模板中的占位符缺少对应列：{', '.join(missing_fields)}"

        target_column = self.get_send_target_column()
        if not self.is_local_db_mode() and self.columns and target_column not in self.columns:
            return None, f"当前发送识别列“{target_column}”不在 Excel 列名中。"

        valid_records = [row for row in records if self.get_send_target_value(row)]
        if not valid_records:
            if self.is_local_db_mode():
                return None, "当前任务快照中没有可发送的微信搜索关键词。"
            return None, f"Excel 中没有可发送的“{target_column}”数据。"

        return valid_records, None

    def start_sending(self) -> None:
        if self.send_thread is not None and self.send_thread.isRunning():
            QMessageBox.information(self, "发送中", "当前已有发送任务正在执行。")
            return

        selected_job = self.get_selected_scheduled_job()
        if selected_job is not None and selected_job.task_kind == "json":
            self.start_selected_json_job(selected_job)
            return

        if self.scheduled_mode_radio.isChecked():
            self.queue_scheduled_send()
            return

        if self.template_change_timer.isActive():
            self.template_change_timer.stop()
            self.apply_template_changes()

        records, error_message = self.validate_before_send()
        if error_message is not None:
            QMessageBox.warning(self, "无法发送", error_message)
            return

        assert records is not None
        reply = QMessageBox.question(
            self,
            "确认发送",
            f"准备向 {len(records)} 个联系人发送个性化消息，是否继续？",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.launch_send_thread(
            records=records,
            template_text=self.template_input.toPlainText(),
            target_column=TARGET_VALUE_KEY if self.is_local_db_mode() else self.get_send_target_column(),
            interval_seconds=self.interval_spin.value(),
            random_delay_min=self.random_delay_min_spin.value(),
            random_delay_max=self.random_delay_max_spin.value(),
            operator_name=self.operator_name_input.text().strip(),
            report_to=self.report_to_input.text().strip() or DEFAULT_REPORT_TARGET,
            auto_report=self.auto_report_checkbox.isChecked(),
            scheduled_job=None,
        )

    def start_selected_json_job(self, job: ScheduledSendJob) -> None:
        if job.status != SCHEDULE_STATUS_PENDING:
            QMessageBox.information(self, "无法开始", f"当前选中的 JSON 任务状态为“{self.get_schedule_status_text_for_job(job)}”，不能手动开始。")
            return

        reply = QMessageBox.question(
            self,
            "确认开始 JSON 任务",
            (
                f"准备立即手动开始 JSON 任务 {job.job_id}。\n"
                f"计划时间：{job.scheduled_at}\n"
                f"发送对象：{job.total_count}\n\n"
                "确认后将立即执行，不再等待 start_time。"
            ),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.execute_scheduled_job(job)

    def queue_scheduled_send(self) -> None:
        if self.template_change_timer.isActive():
            self.template_change_timer.stop()
            self.apply_template_changes()

        records, error_message = self.validate_before_send()
        if error_message is not None:
            QMessageBox.warning(self, "无法创建定时任务", error_message)
            return

        assert records is not None
        scheduled_at = self.scheduled_time_edit.dateTime()
        if scheduled_at <= QDateTime.currentDateTime():
            QMessageBox.warning(self, "计划时间无效", "定时发送时间必须晚于当前时间。")
            return
        schedule_mode, schedule_value, schedule_error = self.get_schedule_config_for_queue()
        if schedule_error is not None:
            QMessageBox.warning(self, "重复频率无效", schedule_error)
            return

        if self.is_local_db_mode() and self.current_task_id is not None:
            task_id = self.current_task_id
        else:
            task_id = self.create_task_snapshot_from_records(records)

        task_details = self.local_store.get_task_details(task_id)
        if task_details is None:
            QMessageBox.warning(self, "任务创建失败", "未能读取刚生成的任务快照。")
            return

        job_id = self.local_store.create_scheduled_job(
            task_id=task_id,
            scheduled_at=scheduled_at.toString("yyyy-MM-dd HH:mm:00"),
            interval_seconds=self.interval_spin.value(),
            random_delay_min=self.random_delay_min_spin.value(),
            random_delay_max=self.random_delay_max_spin.value(),
            operator_name=self.operator_name_input.text().strip(),
            report_to=self.report_to_input.text().strip() or DEFAULT_REPORT_TARGET,
            source_mode=task_details["source_mode"] or self.active_source_mode,
            dataset_type=task_details["dataset_type"] or self.get_current_dataset_type(records),
            template_preview=self.build_template_preview(records),
            total_count=len(records),
            schedule_mode=schedule_mode,
            schedule_value=schedule_value,
        )
        self.refresh_scheduled_jobs()
        self.append_log(
            f"已创建定时任务 {job_id}，计划于 {scheduled_at.toString('yyyy-MM-dd HH:mm')} 执行，频率 {self.get_schedule_mode_text(schedule_mode, schedule_value)}，发送对象 {len(records)} 个。"
        )
        QMessageBox.information(
            self,
            "定时任务已创建",
            f"任务ID：{job_id}\n计划时间：{scheduled_at.toString('yyyy-MM-dd HH:mm')}\n重复频率：{self.get_schedule_mode_text(schedule_mode, schedule_value)}\n发送人数：{len(records)}",
        )

    def execute_scheduled_job(self, job: ScheduledSendJob) -> None:
        task_details = self.local_store.get_task_details(job.task_id)
        if task_details is None:
            self.local_store.complete_scheduled_job(
                job.job_id,
                status=SCHEDULE_STATUS_FAILED,
                result={},
                last_error="关联的任务快照不存在。",
            )
            self.refresh_scheduled_jobs()
            self.append_log(f"定时任务 {job.job_id} 执行失败：关联的任务快照不存在。")
            return

        records = self.local_store.load_task_records(job.task_id)
        if not records:
            self.local_store.complete_scheduled_job(
                job.job_id,
                status=SCHEDULE_STATUS_FAILED,
                result={},
                last_error="任务快照为空。",
            )
            self.refresh_scheduled_jobs()
            self.append_log(f"定时任务 {job.job_id} 执行失败：任务快照为空。")
            return

        self.local_store.mark_scheduled_job_running(job.job_id)
        self.local_store.clear_job_waiting_conflict(job.job_id)
        self.refresh_scheduled_jobs()
        self.launch_send_thread(
            records=records,
            template_text=task_details["template_text"],
            target_column=TARGET_VALUE_KEY,
            interval_seconds=job.interval_seconds,
            random_delay_min=job.random_delay_min,
            random_delay_max=job.random_delay_max,
            operator_name=job.operator_name,
            report_to=job.report_to or DEFAULT_REPORT_TARGET,
            auto_report=True,
            scheduled_job=job,
        )

    def launch_send_thread(
        self,
        *,
        records: list[dict[str, str]],
        template_text: str,
        target_column: str,
        interval_seconds: int,
        random_delay_min: int,
        random_delay_max: int,
        operator_name: str,
        report_to: str,
        auto_report: bool,
        scheduled_job: ScheduledSendJob | None,
        task_id_override: int | None = None,
        common_attachments_override: list[dict[str, str]] | None = None,
        send_origin: str = "classic",
    ) -> None:
        task_id = task_id_override if task_id_override is not None else (scheduled_job.task_id if scheduled_job is not None else self.current_task_id)
        if task_id is None:
            task_id = self.create_task_snapshot_from_records(
                records,
                target_column=target_column,
                template_text=template_text,
                common_attachments=common_attachments_override,
            )
            self.current_task_id = task_id

        task_details = self.local_store.get_task_details(task_id)
        common_attachments = [dict(item) for item in (common_attachments_override or self.common_attachments)]
        if task_details is not None and task_details.get("common_attachments_json"):
            common_attachments = (
                [dict(item) for item in common_attachments_override]
                if common_attachments_override is not None
                else self.normalize_attachment_items(task_details["common_attachments_json"])
            )

        source_json_path = ""
        if scheduled_job is not None:
            source_json_path = str(scheduled_job.source_json_path or "")
        if not source_json_path and task_details is not None:
            source_json_path = str(task_details.get("source_json_path") or "")

        log_path = ""
        if scheduled_job is not None:
            log_path = str(scheduled_job.log_path or "")
        if not log_path and task_details is not None:
            log_path = str(task_details.get("json_log_path") or "")
        if not log_path:
            log_path = self.resolve_runtime_log_path(task_id, source_json_path)

        self.active_scheduled_job = scheduled_job
        self.current_send_origin = send_origin
        self.current_runtime_task_id = task_id
        self.current_runtime_records = [dict(row) for row in records]
        self.current_runtime_source_json_path = source_json_path
        self.current_runtime_log_path = log_path
        self.last_runtime_summary = {}

        thread_kwargs = {
            "records": records,
            "template": template_text,
            "interval_seconds": interval_seconds,
            "target_column": target_column,
            "locale": self.config["settings"]["language"],
            "random_delay_min": random_delay_min,
            "random_delay_max": random_delay_max,
            "operator_name": operator_name,
            "report_to": report_to,
            "auto_report": auto_report,
            "debug_mode": self.is_debug_mode_enabled(),
            "stop_on_error": self.is_stop_on_error_enabled(),
            "common_attachments": common_attachments,
            "target_result_callback": self.handle_target_result,
            "target_log_callback": self.handle_target_log,
            "summary_callback": self.handle_summary_result,
        }
        signature = inspect.signature(PersonalizedSendThread.__init__)
        filtered_kwargs = {
            key: value
            for key, value in thread_kwargs.items()
            if key in signature.parameters
        }
        self.send_thread = PersonalizedSendThread(**filtered_kwargs)
        self.send_thread.progress.connect(self.on_send_progress)
        self.send_thread.log.connect(self.append_log)
        self.send_thread.error.connect(self.on_send_error)
        self.send_thread.completed.connect(self.on_send_completed)
        self.send_thread.finished.connect(self.on_thread_finished)
        self.send_thread.start()

        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.preview_button.setEnabled(False)
        self.load_excel_button.setEnabled(False)
        self.import_local_button.setEnabled(False)
        self.preview_table.setEnabled(False)
        self.debug_mode_button.setEnabled(False)
        if hasattr(self, "basic_start_button"):
            self.basic_start_button.setEnabled(False)
        if hasattr(self, "basic_stop_button"):
            self.basic_stop_button.setEnabled(True)
        if hasattr(self, "basic_load_button"):
            self.basic_load_button.setEnabled(False)
        self.send_status_label.setText("发送中...")
        self.open_send_prepare_page()
        if scheduled_job is not None:
            start_message = f"开始执行定时任务 {scheduled_job.job_id}，任务快照 {scheduled_job.task_id}。"
        elif self.is_local_db_mode():
            start_message = f"开始执行本地库任务快照发送，任务ID={self.current_task_id}。"
        elif self.active_source_mode == SOURCE_MODE_JSON:
            start_message = f"开始执行 JSON 任务发送，任务ID={task_id}。"
        elif send_origin == "basic":
            start_message = f"开始执行基本功能页发送，任务ID={task_id}。"
        else:
            start_message = "开始执行 Excel 个性化群发。"
        if self.is_debug_mode_enabled():
            start_message = start_message.rstrip("。") + "（调试模式：会自动定位联系人并预填消息草稿，但不会按回车发送）。"
        if not self.is_stop_on_error_enabled():
            start_message = start_message.rstrip("。") + "（失败策略：记录失败后继续发送）。"
        self.append_log(start_message)
        self.append_runtime_log_line(start_message)

    def stop_sending(self) -> None:
        if self.send_thread is not None and self.send_thread.isRunning():
            self.send_thread.request_stop()
            self.stop_button.setEnabled(False)
            if hasattr(self, "basic_stop_button"):
                self.basic_stop_button.setEnabled(False)
            self.send_status_label.setText("正在停止...")
            if self.current_send_origin == "basic":
                self.basic_runtime_status_label.setText("正在停止当前批次，请稍候...")
                self.set_label_tone(self.basic_runtime_status_label, "warning")
            self.append_log("已收到停止请求，将在当前联系人到达安全停止点后尽快终止。")

    def on_send_progress(self, current: int, total: int, wechat_id: str) -> None:
        self.send_status_label.setText(f"发送进度：{current}/{total}")
        self.append_log(f"进度更新：{current}/{total} -> {wechat_id}")

    def on_send_error(self, error_message: str) -> None:
        self.append_log(f"发送线程异常：{error_message}")
        if self.active_scheduled_job is None:
            QMessageBox.warning(self, "发送异常", error_message)

    def on_send_completed(self, summary: dict) -> None:
        self.last_runtime_summary = dict(summary)
        message = (
            f"总数：{summary['total']}\n"
            f"已发送：{summary['sent']}\n"
            f"失败：{summary['failed']}\n"
            f"跳过：{summary['skipped']}"
        )
        if summary.get("attachments_sent") is not None:
            message += (
                f"\n附件成功：{summary.get('attachments_sent', 0)}"
                f"\n附件失败：{summary.get('attachments_failed', 0)}"
            )
        if summary.get("random_delay_count"):
            message += f"\n随机延迟事务：{summary['random_delay_count']} 次"
        if summary.get("report_sent"):
            message += "\n自动汇报：已发送"
        elif summary.get("report_error"):
            message += f"\n自动汇报失败：{summary['report_error']}"
        if summary.get("debug_mode"):
            message += "\n调试模式：已自动定位联系人并预填消息草稿，但未按回车发送"
        if summary.get("error"):
            message += f"\n线程异常：{summary['error']}"
        if self.current_runtime_log_path:
            message += f"\n日志文件：{self.current_runtime_log_path}"
        if summary.get("stopped_by_error"):
            message += "\n状态：异常停止"
        elif summary.get("stopped"):
            message += "\n状态：已手动停止"
        else:
            message += "\n状态：已完成"

        if self.current_send_origin == "basic" and self.basic_task_id is not None:
            remaining = len(self.get_basic_pending_records(self.basic_task_id))
            if not summary.get("error") and not summary.get("stopped_by_error") and not summary.get("stopped") and remaining > 0:
                message += f"\n状态：已达到本次发送人数上限，剩余 {remaining} 人待继续"
                self.basic_runtime_status_label.setText(f"本轮已完成，剩余 {remaining} 人。再次点击发送会从剩余联系人继续。")
                self.set_label_tone(self.basic_runtime_status_label, "warning")
            elif remaining == 0 and not summary.get("error"):
                self.basic_runtime_status_label.setText("当前接收人名单已全部处理完成。")
                self.set_label_tone(self.basic_runtime_status_label, "success")
            elif summary.get("stopped"):
                self.basic_runtime_status_label.setText("已手动停止当前批次，可再次点击发送继续剩余联系人。")
                self.set_label_tone(self.basic_runtime_status_label, "warning")
            else:
                self.basic_runtime_status_label.setText("本轮发送出现异常，请处理后再决定是否继续。")
                self.set_label_tone(self.basic_runtime_status_label, "danger")
            self.refresh_basic_selected_table()
            self.update_basic_progress_status()

        self.send_status_label.setText("发送结束。")
        self.append_log("发送任务结束。")
        if self.active_scheduled_job is not None:
            if summary.get("error") or summary.get("stopped_by_error"):
                job_status = SCHEDULE_STATUS_FAILED
            elif summary.get("stopped"):
                job_status = SCHEDULE_STATUS_CANCELLED
            else:
                next_run_at = self.compute_next_run_time(
                    self.active_scheduled_job,
                    str(summary.get("finished_at") or ""),
                )
                if next_run_at:
                    self.local_store.reschedule_scheduled_job(
                        self.active_scheduled_job.job_id,
                        next_scheduled_at=next_run_at,
                        result=summary,
                        last_error=summary.get("report_error", ""),
                    )
                    self.refresh_scheduled_jobs()
                    self.append_log(
                        f"定时任务 {self.active_scheduled_job.job_id} 本次完成，已按 {self.get_schedule_mode_text(self.active_scheduled_job.schedule_mode, self.active_scheduled_job.schedule_value)} 续排到 {next_run_at}。"
                    )
                    return
                job_status = SCHEDULE_STATUS_COMPLETED
            self.local_store.complete_scheduled_job(
                self.active_scheduled_job.job_id,
                status=job_status,
                result=summary,
                last_error=summary.get("error", "") or summary.get("report_error", ""),
            )
            self.refresh_scheduled_jobs()
            self.append_log(f"定时任务 {self.active_scheduled_job.job_id} 执行结束：{self.get_schedule_status_text(job_status)}。")
            return

        QMessageBox.information(self, "发送结果", message)

    def on_thread_finished(self) -> None:
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.preview_button.setEnabled(True)
        self.load_excel_button.setEnabled(True)
        self.import_local_button.setEnabled(True)
        self.preview_table.setEnabled(True)
        self.debug_mode_button.setEnabled(True)
        if hasattr(self, "basic_start_button"):
            self.basic_start_button.setEnabled(True)
        if hasattr(self, "basic_stop_button"):
            self.basic_stop_button.setEnabled(False)
        if hasattr(self, "basic_load_button"):
            self.basic_load_button.setEnabled(True)
        self.send_thread = None
        self.active_scheduled_job = None
        self.current_runtime_task_id = None
        self.current_runtime_records = []
        self.current_runtime_source_json_path = ""
        self.current_runtime_log_path = ""
        self.current_send_batch_limit = None
        self.current_send_remaining_before_start = 0
        previous_origin = self.current_send_origin
        self.current_send_origin = "classic"
        self.refresh_scheduled_jobs()
        self.update_action_button_state()
        if previous_origin == "basic":
            self.refresh_basic_selected_table()
            self.update_basic_progress_status()

    def append_log(self, message: str) -> None:
        self.log_view.appendPlainText(message)
        self.log_view.verticalScrollBar().setValue(self.log_view.verticalScrollBar().maximum())

    def append_runtime_log_line(self, entry: str | dict[str, Any]) -> None:
        if not self.current_runtime_log_path:
            return
        if json_task_helper is not None:
            try:
                json_task_helper.append_task_log(self.current_runtime_log_path, entry)
                return
            except Exception as exc:
                self.append_log(f"写入任务日志失败：{exc}")
                return
        path = Path(self.current_runtime_log_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        text = entry if isinstance(entry, str) else json.dumps(entry, ensure_ascii=False)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(str(text).rstrip() + "\n")

    def resolve_runtime_log_path(self, task_id: int, source_json_path: str = "") -> str:
        if json_task_helper is not None:
            try:
                return str(json_task_helper.build_log_path(source_json_path or None, task_id=task_id))
            except Exception:
                pass
        logs_dir = Path("logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        if source_json_path:
            source_path = Path(source_json_path)
            return str(source_path.with_name(f"{source_path.stem}.easychat.log"))
        return str((logs_dir / f"easychat-task-{task_id}.log").resolve(strict=False))

    def handle_target_result(self, row_result: dict[str, Any]) -> None:
        if self.current_runtime_task_id is None:
            return
        index = int(row_result.get("index") or 0) - 1
        if index < 0 or index >= len(self.current_runtime_records):
            return

        record = self.current_runtime_records[index]
        record[ROW_SEND_STATUS_KEY] = str(row_result.get("send_status") or "")
        record[ROW_ATTACHMENT_STATUS_KEY] = str(row_result.get("attachment_status") or "")
        record[ROW_ERROR_MSG_KEY] = str(row_result.get("error_msg") or "")
        record[ROW_SEND_TIME_KEY] = str(row_result.get("send_time") or "")
        record["attachment_details"] = list(row_result.get("attachments") or [])
        self.current_runtime_records[index] = record

        task_item_id = record.get(TASK_ITEM_ID_KEY)
        if task_item_id:
            try:
                self.local_store.update_task_item_result(
                    int(str(task_item_id)),
                    send_status=record[ROW_SEND_STATUS_KEY],
                    send_time=record[ROW_SEND_TIME_KEY],
                    error_msg=record[ROW_ERROR_MSG_KEY],
                    attachment_status=record[ROW_ATTACHMENT_STATUS_KEY],
                    attachments=record.get(ROW_ATTACHMENTS_KEY) if record.get(ROW_ATTACHMENT_MODE_KEY) == "custom" else None,
                    attachment_details=record.get("attachment_details"),
                    raw_updates={
                        ROW_SEND_STATUS_KEY: record[ROW_SEND_STATUS_KEY],
                        ROW_ATTACHMENT_STATUS_KEY: record[ROW_ATTACHMENT_STATUS_KEY],
                        ROW_ERROR_MSG_KEY: record[ROW_ERROR_MSG_KEY],
                        ROW_SEND_TIME_KEY: record[ROW_SEND_TIME_KEY],
                        "attachment_details": record.get("attachment_details", []),
                    },
                )
            except Exception as exc:
                self.append_log(f"写入任务项结果失败：{exc}")

        scheduled_job_id = self.active_scheduled_job.job_id if self.active_scheduled_job is not None else None
        try:
            self.local_store.append_send_event(
                task_id=self.current_runtime_task_id,
                task_item_id=int(str(task_item_id)) if task_item_id else None,
                scheduled_job_id=scheduled_job_id,
                target_value=str(row_result.get("target_value") or ""),
                target_type=str(row_result.get(ROW_TARGET_TYPE_KEY) or ""),
                message_mode=str(row_result.get(ROW_MESSAGE_MODE_KEY) or ""),
                send_status=str(row_result.get("send_status") or ""),
                send_time=str(row_result.get("send_time") or ""),
                error_msg=str(row_result.get("error_msg") or ""),
                attachment_status=str(row_result.get("attachment_status") or ""),
                source_json_path=self.current_runtime_source_json_path,
                log_path=self.current_runtime_log_path,
                event_data=row_result,
            )
            for attachment_item in row_result.get("attachments") or []:
                self.local_store.append_send_event(
                    task_id=self.current_runtime_task_id,
                    task_item_id=int(str(task_item_id)) if task_item_id else None,
                    scheduled_job_id=scheduled_job_id,
                    target_value=str(row_result.get("target_value") or ""),
                    target_type=str(row_result.get(ROW_TARGET_TYPE_KEY) or ""),
                    message_mode=str(row_result.get(ROW_MESSAGE_MODE_KEY) or ""),
                    send_status=str(row_result.get("send_status") or ""),
                    send_time=str(row_result.get("send_time") or ""),
                    error_msg=str(attachment_item.get("error_msg") or ""),
                    file_path=str(attachment_item.get("file_path") or ""),
                    file_type=str(attachment_item.get("file_type") or ""),
                    attachment_status=str(attachment_item.get("attachment_status") or ""),
                    source_json_path=self.current_runtime_source_json_path,
                    log_path=self.current_runtime_log_path,
                    event_data=attachment_item,
                )
        except Exception as exc:
            self.append_log(f"写入发送事件失败：{exc}")

        if self.current_runtime_source_json_path and json_task_helper is not None:
            source_index = int(record.get("source_json_index") or record.get("source_target_index") or index)
            try:
                json_task_helper.update_json_target_status(
                    self.current_runtime_source_json_path,
                    source_json_index=source_index,
                    send_status=str(row_result.get("send_status") or ""),
                    error_msg=str(row_result.get("error_msg") or ""),
                    attachment_status=str(row_result.get("attachment_status") or ""),
                    send_time=str(row_result.get("send_time") or ""),
                    attachment_results=list(row_result.get("attachments") or []),
                )
            except Exception as exc:
                self.append_log(f"回写 JSON 目标状态失败：{exc}")

    def handle_target_log(self, _message: str, row_result: dict[str, Any]) -> None:
        if not self.current_runtime_log_path:
            return
        if not row_result or (not row_result.get("target_value") and not row_result.get("send_status")):
            stage_message = _message.strip()
            if stage_message:
                self.append_runtime_log_line(f"{datetime.now().strftime(JSON_TIME_FORMAT)} | stage={stage_message}")
            return
        if str(row_result.get("log_type") or "") == "stage":
            stage = str(row_result.get("stage") or "").strip() or "未知阶段"
            target = str(row_result.get("target_value") or "-").strip() or "-"
            detail = str(row_result.get("detail") or "").strip()
            index = row_result.get("index")
            total = row_result.get("total")
            prefix = f"{datetime.now().strftime(JSON_TIME_FORMAT)} | stage={stage} | target={target}"
            if index and total:
                prefix += f" | progress={index}/{total}"
            if detail:
                prefix += f" | detail={detail}"
            self.append_runtime_log_line(prefix)
            return
        if json_task_helper is not None:
            try:
                json_task_helper.append_task_log(
                    self.current_runtime_log_path,
                    {
                        "timestamp": row_result.get("send_time") or datetime.now().strftime(JSON_TIME_FORMAT),
                        "target": row_result.get("target_value"),
                        "text_status": row_result.get("text_status"),
                        "attachment_status": row_result.get("attachment_status"),
                        "reason": row_result.get("error_msg"),
                        "attachments": row_result.get("attachments") or [],
                    },
                )
                return
            except Exception as exc:
                self.append_log(f"写入任务日志失败：{exc}")

    def handle_summary_result(self, summary: dict[str, Any]) -> None:
        if self.current_runtime_log_path and json_task_helper is not None:
            try:
                json_task_helper.append_task_log(
                    self.current_runtime_log_path,
                    f"SUMMARY | total={summary.get('total')} | sent={summary.get('sent')} | failed={summary.get('failed')} | skipped={summary.get('skipped')}",
                )
            except Exception as exc:
                self.append_log(f"写入汇总日志失败：{exc}")

        payload = None
        if self.current_runtime_source_json_path and json_task_helper is not None:
            try:
                should_write_end_time = (
                    not summary.get("stopped")
                    and not summary.get("stopped_by_error")
                    and (
                        self.active_scheduled_job is None
                        or str(self.active_scheduled_job.schedule_mode or SCHEDULE_MODE_ONCE) == SCHEDULE_MODE_ONCE
                    )
                )
                if should_write_end_time:
                    json_task_helper.update_json_task_end_time(
                        self.current_runtime_source_json_path,
                        end_time=str(summary.get("finished_at") or datetime.now().strftime(JSON_TIME_FORMAT)),
                    )
                payload = json_task_helper.load_json_task_file(self.current_runtime_source_json_path, validate_exists=False)
            except Exception as exc:
                self.append_log(f"回写 JSON 结束时间失败：{exc}")

        if self.current_runtime_task_id is not None and payload is not None:
            try:
                self.local_store.sync_json_task_payload(
                    self.current_runtime_task_id,
                    payload,
                    json_end_time=str(payload.get("end_time") or ""),
                    log_path=self.current_runtime_log_path,
                    common_attachments=payload.get("common_attachments") or [],
                )
            except Exception as exc:
                self.append_log(f"同步 JSON 任务快照失败：{exc}")

    def showEvent(self, event) -> None:
        super().showEvent(event)
        self.update_compact_ui_mode()
        if self._startup_layout_refreshed:
            return
        self._startup_layout_refreshed = True
        QTimer.singleShot(0, self.refresh_startup_layout)
        QTimer.singleShot(80, self.refresh_startup_layout)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self.update_compact_ui_mode()

    def refresh_startup_layout(self) -> None:
        root_layout = self.layout()
        if root_layout is not None:
            root_layout.activate()
        self.updateGeometry()
        self.main_tabs.updateGeometry()
        for widget in (
            self.workbench_page,
            self.basic_page,
            self.data_template_page,
            self.local_store_page,
            self.send_prepare_page,
            self.task_center_page,
        ):
            child_layout = widget.layout()
            if child_layout is not None:
                child_layout.activate()
            widget.updateGeometry()
        for table_name in (
            "preview_table",
            "schedule_table",
            "basic_selected_table",
            "basic_attachment_table",
            "common_attachment_table",
        ):
            table = getattr(self, table_name, None)
            if isinstance(table, QTableWidget):
                self.apply_table_header_font(table)

        width = max(self.width(), self.minimumWidth())
        height = max(self.height(), self.minimumHeight())
        self.resize(width + 1, height + 1)
        self.resize(width, height)

    def show_wechat_open_notice(self) -> None:
        msg_box = QMessageBox(self)
        msg_box.setIcon(QMessageBox.Information)
        msg_box.setWindowTitle("重要提示")
        msg_box.setText("微信打开方式已变更")
        msg_box.setInformativeText(
            "由于微信版本更新，我们现在使用微信内置的快捷键来打开/隐藏微信窗口，请确保你的微信打开快捷键为Ctrl+Alt+w。具体查看方式为“设置”->“快捷键”->“显示/隐藏窗口”\n\n"
            "⚠️ 注意事项：\n"
            "• 如果微信已经打开且在前台，再次按快捷键会导致微信窗口被隐藏\n"
            "• 为避免此问题，建议在使用批量发送功能前，先手动关闭或最小化微信窗口\n"
            "• 这样可以确保程序能够正常打开微信并发送消息\n"
        )
        msg_box.setStandardButtons(QMessageBox.Ok)
        msg_box.exec_()

    def closeEvent(self, event) -> None:
        if self.send_thread is not None and self.send_thread.isRunning():
            QMessageBox.warning(self, "请先停止发送", "当前仍在发送中，请先停止任务后再关闭窗口。")
            event.ignore()
            return
        self.splitter_state_save_timer.stop()
        self.save_registered_splitter_states()
        self.scheduler_timer.stop()
        super().closeEvent(event)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ExcelSenderGUI()
    window.show()
    sys.exit(app.exec_())
