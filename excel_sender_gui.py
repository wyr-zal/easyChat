import json
import os
import sys
import re
from datetime import datetime
from pathlib import Path

from PyQt5.QtCore import QDateTime, Qt, QTimer
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QButtonGroup,
    QCheckBox,
    QDateTimeEdit,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QRadioButton,
    QSpinBox,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
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
    SCHEDULE_STATUS_PENDING,
    SCHEDULE_STATUS_RUNNING,
    ScheduledSendJob,
    SOURCE_MODE_FILE as STORE_SOURCE_MODE_FILE,
    SOURCE_MODE_LOCAL_DB as STORE_SOURCE_MODE_LOCAL_DB,
)
from module import ContactConfirmDialog, FileDropLineEdit


DISPLAY_NAME_OVERRIDE_KEY = "__display_name_override"
RECORD_ID_KEY = "__record_id"
TASK_ITEM_ID_KEY = "__task_item_id"
TARGET_VALUE_KEY = "__target_value"
SOURCE_MODE_FILE = STORE_SOURCE_MODE_FILE
SOURCE_MODE_LOCAL_DB = STORE_SOURCE_MODE_LOCAL_DB
DEFAULT_LOCAL_FILTER_FIELDS = ("显示名称", "备注", "昵称", "标签", "详细描述")
LOCAL_DB_HEADER_TITLE = "微信搜索关键词"


class ExcelSenderGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.config_path = "excel_sender_config.json"
        self.config = self.load_config()
        self.local_store = LocalContactStore(self.config["local_store"]["db_path"])
        self.records: list[dict[str, str]] = []
        self.source_records: list[dict[str, str]] = []
        self.filtered_records: list[dict[str, str]] = []
        self.columns: list[str] = []
        self.send_thread: PersonalizedSendThread | None = None
        self.preview_limit = 10
        self.records_loaded = False
        self.loaded_excel_path = ""
        self.active_source_mode = SOURCE_MODE_FILE
        self.current_batch_id: int | None = None
        self.current_task_id: int | None = None
        self.current_batch_ids: dict[str, int] = {}
        self.active_scheduled_job: ScheduledSendJob | None = None
        self._startup_layout_refreshed = False
        self._updating_preview_table = False
        self.template_change_timer = QTimer(self)
        self.template_change_timer.setSingleShot(True)
        self.template_change_timer.timeout.connect(self.apply_template_changes)
        self.scheduler_timer = QTimer(self)
        self.scheduler_timer.setInterval(5000)
        self.scheduler_timer.timeout.connect(self.poll_scheduled_jobs)

        self.init_ui()
        self.restore_initial_state()
        self.scheduler_timer.start()

    def load_config(self) -> dict:
        if os.path.exists(self.config_path):
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

        if changed:
            with open(self.config_path, "w", encoding="utf-8") as file:
                json.dump(config, file, indent=4, ensure_ascii=False)

        return config

    def save_config(self) -> None:
        with open(self.config_path, "w", encoding="utf-8") as file:
            json.dump(self.config, file, indent=4, ensure_ascii=False)

    def init_ui(self) -> None:
        self.setWindowTitle("EasyChat Excel 个性化群发")
        self.resize(1120, 860)
        self.setMinimumSize(960, 720)
        self.apply_font_scaling()

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(18, 18, 18, 18)
        root_layout.setSpacing(12)

        self.main_tabs = QTabWidget(self)
        self.main_tabs.setDocumentMode(True)
        self.main_tabs.tabBar().setExpanding(True)
        self.data_template_page = self.build_data_template_page()
        self.local_store_page = self.build_local_store_page()
        self.execution_page = self.build_execution_page()
        self.main_tabs.addTab(self.data_template_page, "数据与模板")
        self.main_tabs.addTab(self.local_store_page, "本地库数据")
        self.main_tabs.addTab(self.execution_page, "发送执行")

        root_layout.addWidget(self.main_tabs)

    def apply_font_scaling(self) -> None:
        base_font = QFont(self.font())
        base_font.setPointSize(11)
        self.setFont(base_font)

        self.setStyleSheet(
            """
            QTabBar::tab {
                min-height: 40px;
                padding: 6px 18px;
            }
            QPushButton {
                min-height: 38px;
                padding: 4px 12px;
            }
            QSpinBox,
            QLineEdit {
                min-height: 34px;
            }
            QGroupBox {
                font-weight: 600;
            }
            """
        )

    def build_settings_group(self) -> QGroupBox:
        group = QGroupBox("基础设置")
        layout = QVBoxLayout(group)
        layout.setSpacing(12)

        self.wechat_notice_btn = QPushButton("查看微信启动说明", group)
        self.wechat_notice_btn.clicked.connect(self.show_wechat_open_notice)
        layout.addWidget(self.wechat_notice_btn)
        layout.addLayout(self.init_language_choose())

        tip_label = QLabel("建议先确认微信快捷键为 Ctrl+Alt+W，再进行 Excel 读取和批量发送。")
        tip_label.setWordWrap(True)
        tip_label.setStyleSheet("color:#555;")
        layout.addWidget(tip_label)
        return group

    def build_data_template_page(self) -> QWidget:
        page = QWidget(self)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)
        layout.addWidget(self.build_excel_group())
        layout.addWidget(self.build_template_group(), stretch=1)
        return page

    def build_local_store_page(self) -> QWidget:
        page = QWidget(self)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)
        layout.addWidget(self.build_local_store_group(), stretch=3)
        layout.addWidget(self.build_filter_group(), stretch=2)
        return page

    def build_execution_page(self) -> QWidget:
        page = QWidget(self)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)
        layout.addWidget(self.build_settings_group())
        layout.addWidget(self.build_control_group())
        layout.addWidget(self.build_schedule_group())
        layout.addWidget(self.build_preview_group(), stretch=3)
        layout.addWidget(self.build_log_group(), stretch=2)
        return page

    def build_control_group(self) -> QGroupBox:
        group = QGroupBox("发送控制")
        layout = QVBoxLayout(group)
        layout.setSpacing(12)

        helper_label = QLabel("建议先准备消息模板；若使用本地库模式，请先在“本地库数据”页筛选并导入发送计划。")
        helper_label.setWordWrap(True)
        helper_label.setStyleSheet("color:#555;")
        layout.addWidget(helper_label)
        layout.addLayout(self.build_action_bar())

        detail_label = QLabel("“刷新预览”会渲染前 10 条消息；本地库模式下，只有已导入发送计划的任务快照才允许编辑单条目标和消息。定时发送会冻结当前快照。")
        detail_label.setWordWrap(True)
        detail_label.setStyleSheet("color:#555;")
        layout.addWidget(detail_label)
        return group

    def build_schedule_group(self) -> QGroupBox:
        group = QGroupBox("发送模式与定时任务")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        mode_layout = QHBoxLayout()
        mode_layout.addWidget(QLabel("发送模式"))
        self.send_mode_group = QButtonGroup(self)
        self.immediate_mode_radio = QRadioButton("立即发送")
        self.scheduled_mode_radio = QRadioButton("定时发送")
        self.send_mode_group.addButton(self.immediate_mode_radio)
        self.send_mode_group.addButton(self.scheduled_mode_radio)
        self.immediate_mode_radio.toggled.connect(self.on_send_mode_changed)
        mode_layout.addWidget(self.immediate_mode_radio)
        mode_layout.addWidget(self.scheduled_mode_radio)
        mode_layout.addSpacing(16)
        mode_layout.addWidget(QLabel("计划时间"))
        self.scheduled_time_edit = QDateTimeEdit(self)
        self.scheduled_time_edit.setCalendarPopup(True)
        self.scheduled_time_edit.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.scheduled_time_edit.setDateTime(QDateTime.currentDateTime().addSecs(60))
        self.scheduled_time_edit.dateTimeChanged.connect(self.on_send_mode_changed)
        mode_layout.addWidget(self.scheduled_time_edit)
        mode_layout.addStretch(1)
        layout.addLayout(mode_layout)

        delay_layout = QHBoxLayout()
        delay_layout.addWidget(QLabel("随机事务延迟（秒）"))
        self.random_delay_min_spin = QSpinBox(self)
        self.random_delay_min_spin.setRange(0, 3600)
        self.random_delay_min_spin.valueChanged.connect(self.on_bulk_send_option_changed)
        delay_layout.addWidget(QLabel("最小"))
        delay_layout.addWidget(self.random_delay_min_spin)
        self.random_delay_max_spin = QSpinBox(self)
        self.random_delay_max_spin.setRange(0, 3600)
        self.random_delay_max_spin.valueChanged.connect(self.on_bulk_send_option_changed)
        delay_layout.addWidget(QLabel("最大"))
        delay_layout.addWidget(self.random_delay_max_spin)
        delay_layout.addStretch(1)
        layout.addLayout(delay_layout)

        report_layout = QHBoxLayout()
        report_layout.addWidget(QLabel("操作人"))
        self.operator_name_input = QLineEdit(self)
        self.operator_name_input.setPlaceholderText("用于任务汇报")
        self.operator_name_input.editingFinished.connect(self.on_bulk_send_option_changed)
        report_layout.addWidget(self.operator_name_input)
        report_layout.addWidget(QLabel("汇报微信号"))
        self.report_to_input = QLineEdit(self)
        self.report_to_input.setPlaceholderText(DEFAULT_REPORT_TARGET)
        self.report_to_input.editingFinished.connect(self.on_bulk_send_option_changed)
        report_layout.addWidget(self.report_to_input)
        self.auto_report_checkbox = QCheckBox("任务完成后自动汇报", self)
        self.auto_report_checkbox.toggled.connect(self.on_bulk_send_option_changed)
        report_layout.addWidget(self.auto_report_checkbox)
        layout.addLayout(report_layout)

        task_action_layout = QHBoxLayout()
        self.refresh_schedule_button = QPushButton("刷新定时任务")
        self.refresh_schedule_button.clicked.connect(self.refresh_scheduled_jobs)
        self.cancel_schedule_button = QPushButton("取消选中任务")
        self.cancel_schedule_button.clicked.connect(self.cancel_selected_scheduled_job)
        task_action_layout.addWidget(self.refresh_schedule_button)
        task_action_layout.addWidget(self.cancel_schedule_button)
        task_action_layout.addStretch(1)
        layout.addLayout(task_action_layout)

        self.schedule_status_label = QLabel("当前为立即发送模式。")
        self.schedule_status_label.setWordWrap(True)
        self.schedule_status_label.setStyleSheet("color:#555;")
        layout.addWidget(self.schedule_status_label)

        self.schedule_table = QTableWidget(0, 6, self)
        self.schedule_table.setHorizontalHeaderLabels(["任务ID", "计划时间", "状态", "人数", "来源", "内容摘要"])
        self.schedule_table.verticalHeader().setVisible(False)
        self.schedule_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.schedule_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.schedule_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.schedule_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.schedule_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.schedule_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.schedule_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.schedule_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        layout.addWidget(self.schedule_table)

        return group

    def build_local_store_group(self) -> QGroupBox:
        group = QGroupBox("本地库数据")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        tip_label = QLabel("这里展示当前 SQLite 本地库里的好友库和群聊库。导入新的好友文件不会覆盖群聊 current，反之亦然；筛选时按当前页签的数据范围执行。")
        tip_label.setWordWrap(True)
        tip_label.setStyleSheet("color:#555;")
        layout.addWidget(tip_label)

        action_layout = QHBoxLayout()
        self.refresh_local_store_button = QPushButton("刷新本地库")
        self.refresh_local_store_button.clicked.connect(self.refresh_local_store_page)
        self.use_local_store_button = QPushButton("筛选并导入发送计划")
        self.use_local_store_button.clicked.connect(self.filter_local_store_into_task)
        action_layout.addWidget(self.refresh_local_store_button)
        action_layout.addWidget(self.use_local_store_button)
        action_layout.addStretch(1)
        layout.addLayout(action_layout)

        self.local_store_summary_label = QLabel("本地库暂无数据。")
        self.local_store_summary_label.setWordWrap(True)
        layout.addWidget(self.local_store_summary_label)

        self.local_filter_scope_label = QLabel("当前筛选对象：好友库当前批次。")
        self.local_filter_scope_label.setWordWrap(True)
        self.local_filter_scope_label.setStyleSheet("color:#555;")
        layout.addWidget(self.local_filter_scope_label)

        self.local_store_tabs = QTabWidget(self)
        self.local_store_views: dict[str, dict[str, object]] = {}
        for dataset_type in (DATASET_FRIEND, DATASET_GROUP):
            tab, view_refs = self.build_local_store_dataset_panel(dataset_type)
            self.local_store_tabs.addTab(tab, DATASET_LABELS[dataset_type])
            self.local_store_views[dataset_type] = view_refs
        self.local_store_tabs.currentChanged.connect(self.on_local_store_tab_changed)
        layout.addWidget(self.local_store_tabs, stretch=1)

        return group

    def build_local_store_dataset_panel(self, dataset_type: str) -> tuple[QWidget, dict[str, object]]:
        panel = QWidget(self)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        summary_label = QLabel(f"{DATASET_LABELS[dataset_type]}库暂无数据。")
        summary_label.setWordWrap(True)
        layout.addWidget(summary_label)

        columns_view = QPlainTextEdit(self)
        columns_view.setReadOnly(True)
        columns_view.setFixedHeight(60)
        layout.addWidget(columns_view)

        table = QTableWidget(0, 0, self)
        table.verticalHeader().setVisible(False)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(table, stretch=1)

        return panel, {
            "summary_label": summary_label,
            "columns_view": columns_view,
            "table": table,
        }

    def build_excel_group(self) -> QGroupBox:
        group = QGroupBox("Excel 数据")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        path_layout = QHBoxLayout()
        self.excel_path_input = FileDropLineEdit(
            suffixes=[".xlsx", ".xls", ".csv"],
            parent=self,
        )
        self.excel_path_input.setPlaceholderText("选择或拖入 Excel 文件（支持 .xlsx / .xls / .csv）")
        self.excel_path_input.textChanged.connect(self.on_excel_path_changed)
        path_layout.addWidget(self.excel_path_input)

        choose_button = QPushButton("选择文件")
        choose_button.setMinimumWidth(120)
        choose_button.clicked.connect(self.select_excel_file)
        path_layout.addWidget(choose_button)

        load_button = QPushButton("读取数据")
        load_button.setMinimumWidth(120)
        load_button.clicked.connect(self.load_excel_data)
        path_layout.addWidget(load_button)
        self.load_excel_button = load_button

        import_button = QPushButton("导入到本地库")
        import_button.setMinimumWidth(140)
        import_button.clicked.connect(self.import_excel_to_local_store)
        path_layout.addWidget(import_button)
        self.import_local_button = import_button

        load_local_button = QPushButton("查看本地库")
        load_local_button.setMinimumWidth(120)
        load_local_button.clicked.connect(self.open_local_store_page)
        path_layout.addWidget(load_local_button)
        self.load_local_button = load_local_button

        layout.addLayout(path_layout)

        self.data_info_label = QLabel("尚未读取 Excel 数据。")
        self.data_info_label.setWordWrap(True)
        layout.addWidget(self.data_info_label)

        self.local_db_status_label = QLabel("本地库尚未导入数据。")
        self.local_db_status_label.setWordWrap(True)
        self.local_db_status_label.setStyleSheet("color:#555;")
        layout.addWidget(self.local_db_status_label)

        send_target_layout = QHBoxLayout()
        send_target_layout.addWidget(QLabel("发送识别列"))
        self.send_target_column_input = QLineEdit(self)
        self.send_target_column_input.setPlaceholderText("默认：微信号，也可以填写姓名、备注等列名")
        self.send_target_column_input.textChanged.connect(self.on_send_target_column_changed)
        send_target_layout.addWidget(self.send_target_column_input)
        layout.addLayout(send_target_layout)

        self.send_target_status_label = QLabel("当前发送时会使用“微信号”列作为微信搜索关键词。")
        self.send_target_status_label.setWordWrap(True)
        self.send_target_status_label.setStyleSheet("color:#555;")
        layout.addWidget(self.send_target_status_label)

        layout.addWidget(QLabel("检测到的列名"))
        self.columns_view = QPlainTextEdit(self)
        self.columns_view.setReadOnly(True)
        self.columns_view.setFixedHeight(80)
        layout.addWidget(self.columns_view)

        return group

    def build_template_group(self) -> QGroupBox:
        group = QGroupBox("消息模板")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        tip_label = QLabel("支持占位符语法 `{{列名}}`，例如：您好 {{姓名}}，您的课程 {{课程名}} 将于 {{到期时间}} 到期。")
        tip_label.setWordWrap(True)
        layout.addWidget(tip_label)

        self.template_input = QPlainTextEdit(self)
        self.template_input.setPlaceholderText("请输入要批量发送的模板消息。")
        self.template_input.textChanged.connect(self.on_template_changed)
        self.template_input.setMinimumHeight(180)
        layout.addWidget(self.template_input)

        self.placeholder_status_label = QLabel("当前模板未使用占位符。")
        self.placeholder_status_label.setWordWrap(True)
        layout.addWidget(self.placeholder_status_label)

        return group

    def build_filter_group(self) -> QGroupBox:
        group = QGroupBox("本地库筛选条件")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        tip_label = QLabel(
            "筛选作用于“本地库数据”页当前选中的页签。多个字段请用英文逗号分隔；规则留空时，会把当前页签全部数据带入确认弹窗。"
        )
        tip_label.setWordWrap(True)
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
        self.filter_status_label.setWordWrap(True)
        self.filter_status_label.setStyleSheet("color:#555;")
        layout.addWidget(self.filter_status_label)

        examples_label = QLabel(
            "场景规则示例：\n"
            "1. 到期提醒：到期|续费|复购\n"
            "2. 跟进客户：待跟进|未回复|沉默\n"
            "3. 地区筛选：上海|北京|深圳\n"
            "4. 精确匹配：^高意向客户$\n"
            "5. 排除空白场景：^(?!\\s*$).+"
        )
        examples_label.setWordWrap(True)
        examples_label.setStyleSheet("color:#555;")
        layout.addWidget(examples_label)

        return group

    def build_action_bar(self) -> QHBoxLayout:
        layout = QHBoxLayout()
        layout.setSpacing(12)

        interval_label = QLabel("发送间隔（秒）")
        self.interval_spin = QSpinBox(self)
        self.interval_spin.setRange(0, 3600)
        self.interval_spin.valueChanged.connect(self.on_interval_changed)

        self.preview_button = QPushButton("刷新预览")
        self.preview_button.clicked.connect(self.show_preview_results)

        self.confirm_task_button = QPushButton("从本地库导入计划")
        self.confirm_task_button.clicked.connect(self.confirm_current_selection)

        self.start_button = QPushButton("开始发送")
        self.start_button.clicked.connect(self.start_sending)

        self.stop_button = QPushButton("停止发送")
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_sending)

        self.send_status_label = QLabel("等待发送。")
        self.send_status_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

        layout.addWidget(interval_label)
        layout.addWidget(self.interval_spin)
        layout.addWidget(self.preview_button)
        layout.addWidget(self.confirm_task_button)
        layout.addWidget(self.start_button)
        layout.addWidget(self.stop_button)
        layout.addStretch(1)
        layout.addWidget(self.send_status_label)
        return layout

    def build_preview_group(self) -> QGroupBox:
        group = QGroupBox("发送预览（前 10 条）")
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
        header = self.preview_table.horizontalHeader()
        header_font = QFont(self.font())
        header_font.setPointSize(11)
        header_font.setBold(True)
        header.setFont(header_font)
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        layout.addWidget(self.preview_table)

        return group

    def build_log_group(self) -> QGroupBox:
        group = QGroupBox("发送日志")
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
        self.excel_path_input.setText(self.config["excel"]["path"])
        self.send_target_column_input.setText(self.config["excel"]["send_target_column"])
        self.filter_fields_input.setText(self.config["filter"]["fields"])
        self.filter_pattern_input.setText(self.config["filter"]["pattern"])
        self.filter_ignore_case_checkbox.setChecked(self.config["filter"]["ignore_case"])
        self.template_input.setPlainText(self.config["template"]["text"])
        self.interval_spin.setValue(self.config["settings"]["send_interval"])
        bulk_send = self.config["bulk_send"]
        self.random_delay_min_spin.setValue(int(bulk_send["random_delay_min"]))
        self.random_delay_max_spin.setValue(int(bulk_send["random_delay_max"]))
        self.operator_name_input.setText(str(bulk_send["operator_name"]))
        self.report_to_input.setText(str(bulk_send["report_to"]))
        self.auto_report_checkbox.setChecked(bool(bulk_send["auto_report_enabled"]))
        if bulk_send.get("send_mode") == "scheduled":
            self.scheduled_mode_radio.setChecked(True)
        else:
            self.immediate_mode_radio.setChecked(True)
        self.on_send_mode_changed()

        language = self.config["settings"]["language"]
        if language == "zh-TW":
            self.lang_zh_tw.setChecked(True)
        elif language == "en-US":
            self.lang_en.setChecked(True)
        else:
            self.lang_zh_cn.setChecked(True)

        self.update_local_db_status()
        self.refresh_local_store_page()
        self.refresh_scheduled_jobs()
        self.update_placeholder_status()
        self.update_send_target_column_status()
        self.update_action_button_state()
        self.main_tabs.setCurrentWidget(self.data_template_page)
        if self.excel_path_input.text().strip():
            self.load_excel_data(show_success=False)
        else:
            self.render_preview()

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
            self.local_db_status_label.setStyleSheet("color:#555;")
            self.local_db_status_label.setText("本地库尚未导入数据，可点击“查看本地库”页查看详情。")
            return

        parts = [
            f"{summary.dataset_label}：{summary.source_name}（{summary.row_count} 行）"
            for summary in summaries.values()
        ]
        text = "本地库当前批次：" + "；".join(parts) + "。"
        if self.is_local_db_mode():
            if self.current_task_id is not None:
                text += f" 当前任务快照 {len(self.records)} 行。"
            else:
                text += " 当前正在使用好友+群聊本地库数据。"
        self.local_db_status_label.setStyleSheet("color:#027a48;")
        self.local_db_status_label.setText(text)

    def refresh_local_store_page(self) -> None:
        summaries = self.local_store.get_current_import_summaries()
        if not summaries:
            self.local_store_summary_label.setStyleSheet("color:#555;")
            self.local_store_summary_label.setText("本地库暂无数据，请先导入 Excel/CSV。")
            for view_refs in self.local_store_views.values():
                summary_label = view_refs["summary_label"]
                columns_view = view_refs["columns_view"]
                table = view_refs["table"]
                assert isinstance(summary_label, QLabel)
                assert isinstance(columns_view, QPlainTextEdit)
                assert isinstance(table, QTableWidget)
                summary_label.setStyleSheet("color:#555;")
                summary_label.setText("暂无数据。")
                columns_view.clear()
                table.setColumnCount(0)
                table.setRowCount(0)
            self.update_local_filter_scope()
            return

        self.local_store_summary_label.setStyleSheet("color:#027a48;")
        self.local_store_summary_label.setText(
            "；".join(
                f"{summary.dataset_label}：{summary.source_name}（导入时间 {summary.imported_at}，共 {summary.row_count} 行）"
                for summary in summaries.values()
            )
        )
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
                summary_label.setStyleSheet("color:#555;")
                summary_label.setText(f"{DATASET_LABELS[dataset_type]}库暂无 current 批次。")
                columns_view.clear()
                table.setColumnCount(0)
                table.setRowCount(0)
                continue

            records, columns, _ = self.local_store.load_current_contacts(dataset_type)
            has_any_records = has_any_records or bool(records)
            summary_label.setStyleSheet("color:#027a48;")
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

            table.resizeColumnsToContents()
            table.resizeRowsToContents()

        self.use_local_store_button.setEnabled(has_any_records)
        self.update_local_filter_scope()

    def open_local_store_page(self) -> None:
        self.refresh_local_store_page()
        self.main_tabs.setCurrentWidget(self.local_store_page)

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
            self.local_filter_scope_label.setStyleSheet("color:#555;")
            self.local_filter_scope_label.setText(
                f"当前筛选对象：{DATASET_LABELS[dataset_type]}库当前批次（暂无数据）。"
            )
            self.use_local_store_button.setEnabled(False)
            return

        self.local_filter_scope_label.setStyleSheet("color:#027a48;")
        self.local_filter_scope_label.setText(
            f"当前筛选对象：{summary.dataset_label}库当前批次，来源 {summary.source_name}，共 {summary.row_count} 行。"
        )
        self.use_local_store_button.setEnabled(summary.row_count > 0)

    def activate_local_store_for_send(self) -> None:
        self.filter_local_store_into_task()

    def update_action_button_state(self) -> None:
        if not hasattr(self, "confirm_task_button"):
            return

        if self.send_thread is not None and self.send_thread.isRunning():
            self.confirm_task_button.setEnabled(False)
            return

        has_local_data = bool(self.local_store.get_current_import_summaries())
        self.confirm_task_button.setEnabled(has_local_data)
        if self.current_task_id is not None and self.is_local_db_mode():
            self.confirm_task_button.setText("重新从本地库筛选")
        else:
            self.confirm_task_button.setText("从本地库导入计划")

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
        self.source_records = self.attach_record_ids(records)
        self.filtered_records = list(self.source_records)
        self.records = list(self.filtered_records)
        self.columns = list(columns)
        self.records_loaded = True
        self.loaded_excel_path = loaded_path
        self.columns_view.setPlainText("、".join(self.columns))
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
        if normalized_path != self.loaded_excel_path and self.active_source_mode == SOURCE_MODE_FILE:
            self.records_loaded = False
        self.save_config()

    def on_template_changed(self) -> None:
        self.template_change_timer.start(500)

    def apply_template_changes(self) -> None:
        self.config["template"]["text"] = self.template_input.toPlainText()
        self.save_config()
        if self.current_task_id is not None:
            self.clear_task_snapshot("已修改消息模板，任务快照已失效，请重新从本地库筛选并导入发送计划。")
        self.update_placeholder_status()
        self.update_send_target_column_status()
        self.update_data_info_label()
        self.render_preview()

    def on_send_target_column_changed(self, value: str) -> None:
        self.config["excel"]["send_target_column"] = value.strip() or DEFAULT_SEND_TARGET_COLUMN
        self.save_config()
        if self.current_task_id is not None:
            self.clear_task_snapshot("已修改发送识别列，任务快照已失效，请重新从本地库筛选并导入发送计划。")
        self.update_preview_headers()
        self.update_send_target_column_status()
        self.update_data_info_label()
        self.render_preview()

    def on_filter_fields_changed(self, value: str) -> None:
        self.config["filter"]["fields"] = value
        self.save_config()

    def on_filter_pattern_changed(self, value: str) -> None:
        self.config["filter"]["pattern"] = value
        self.save_config()

    def on_filter_ignore_case_changed(self, checked: bool) -> None:
        self.config["filter"]["ignore_case"] = checked
        self.save_config()

    def on_interval_changed(self, value: int) -> None:
        self.config["settings"]["send_interval"] = value
        self.save_config()

    def on_bulk_send_option_changed(self, *_args) -> None:
        self.config["bulk_send"]["random_delay_min"] = self.random_delay_min_spin.value()
        self.config["bulk_send"]["random_delay_max"] = self.random_delay_max_spin.value()
        self.config["bulk_send"]["operator_name"] = self.operator_name_input.text().strip()
        self.config["bulk_send"]["report_to"] = self.report_to_input.text().strip() or DEFAULT_REPORT_TARGET
        self.config["bulk_send"]["auto_report_enabled"] = self.auto_report_checkbox.isChecked()
        self.save_config()

    def on_send_mode_changed(self, *_args) -> None:
        is_scheduled = self.scheduled_mode_radio.isChecked()
        self.scheduled_time_edit.setEnabled(is_scheduled)
        self.config["bulk_send"]["send_mode"] = "scheduled" if is_scheduled else "immediate"
        self.save_config()
        if is_scheduled:
            self.start_button.setText("创建定时任务")
            self.schedule_status_label.setStyleSheet("color:#b54708;")
            self.schedule_status_label.setText(
                f"当前为定时发送模式，计划时间：{self.scheduled_time_edit.dateTime().toString('yyyy-MM-dd HH:mm')}。"
            )
        else:
            self.start_button.setText("开始发送")
            self.schedule_status_label.setStyleSheet("color:#555;")
            self.schedule_status_label.setText("当前为立即发送模式。")

    def refresh_scheduled_jobs(self) -> None:
        jobs = self.local_store.list_scheduled_jobs()
        self.schedule_table.setRowCount(len(jobs))
        for row_index, job in enumerate(jobs):
            self.schedule_table.setItem(row_index, 0, QTableWidgetItem(str(job.job_id)))
            self.schedule_table.setItem(row_index, 1, QTableWidgetItem(job.scheduled_at))
            self.schedule_table.setItem(row_index, 2, QTableWidgetItem(self.get_schedule_status_text(job.status)))
            self.schedule_table.setItem(row_index, 3, QTableWidgetItem(str(job.total_count)))
            source_text = "Excel" if job.source_mode == SOURCE_MODE_FILE else job.dataset_label
            self.schedule_table.setItem(row_index, 4, QTableWidgetItem(source_text))
            self.schedule_table.setItem(row_index, 5, QTableWidgetItem(job.template_preview or ""))

        self.schedule_table.resizeRowsToContents()

    def get_schedule_status_text(self, status: str) -> str:
        mapping = {
            SCHEDULE_STATUS_PENDING: "待执行",
            SCHEDULE_STATUS_RUNNING: "执行中",
            SCHEDULE_STATUS_COMPLETED: "已完成",
            SCHEDULE_STATUS_CANCELLED: "已取消",
            SCHEDULE_STATUS_FAILED: "失败",
        }
        return mapping.get(status, status)

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

    def poll_scheduled_jobs(self) -> None:
        if self.send_thread is not None and self.send_thread.isRunning():
            return

        due_jobs = self.local_store.get_due_scheduled_jobs(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), limit=1)
        if not due_jobs:
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
            self.columns_view.clear()
            self.data_info_label.setText("读取失败。")
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
        self.filter_status_label.setStyleSheet("color:#555;")
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
        self.filter_status_label.setStyleSheet("color:#555;")
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
            self.filter_status_label.setStyleSheet("color:#b54708;")
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
            self.filter_status_label.setStyleSheet("color:#555;")
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
        self.columns_view.setPlainText("、".join(self.columns))
        self.filter_status_label.setStyleSheet("color:#027a48;")
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
        self.main_tabs.setCurrentWidget(self.execution_page)

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
            return

        source_total = len(self.source_records) if self.source_records else len(self.records)
        filtered_total = len(self.filtered_records) if self.filtered_records else len(self.records)
        current_total = len(self.records)
        valid_count = len([row for row in self.records if self.get_send_target_value(row)])

        if self.current_task_id is not None:
            self.data_info_label.setText(
                f"已载入 {source_total} 行本地数据，筛选后 {filtered_total} 行，当前任务快照 {current_total} 行，其中 {valid_count} 行可发送。"
            )
            return

        if current_total != source_total:
            self.data_info_label.setText(
                f"已读取 {source_total} 行数据，当前筛选后 {current_total} 行，其中 {valid_count} 行可发送。"
            )
            return

        if self.is_local_db_mode():
            self.data_info_label.setText(
                f"已读取本地库 {current_total} 行数据，其中 {valid_count} 行可生成微信搜索关键词。"
            )
            return

        self.data_info_label.setText(
            f"已读取 {current_total} 行数据，其中 {valid_count} 行包含可发送的“{self.get_send_target_column()}”值。"
        )

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
        first_header = LOCAL_DB_HEADER_TITLE if self.is_local_db_mode() else self.get_send_target_column()
        self.preview_table.setHorizontalHeaderLabels([first_header, "显示名称", "预览消息", "操作"])

    def update_send_target_column_status(self) -> None:
        if not hasattr(self, "send_target_status_label"):
            return

        target_column = self.get_send_target_column()
        if not self.columns:
            self.send_target_status_label.setStyleSheet("color:#555;")
            self.send_target_status_label.setText(
                f"当前发送时会使用“{target_column}”列作为微信搜索关键词。"
            )
            return

        valid_count = len([row for row in self.records if self.get_send_target_value(row)])
        if self.is_local_db_mode():
            if target_column in self.columns:
                self.send_target_status_label.setStyleSheet("color:#027a48;")
                self.send_target_status_label.setText(
                    f"当前识别列为“{target_column}”，本地库已匹配 {valid_count} 行微信搜索关键词；群聊会在空值时自动回退。"
                )
                return

            self.send_target_status_label.setStyleSheet("color:#b54708;")
            self.send_target_status_label.setText(
                f"当前发送识别列“{target_column}”不在导入列中，本地库会按联系人类型自动回退，当前已匹配 {valid_count} 行。"
            )
            return

        if target_column not in self.columns:
            self.send_target_status_label.setStyleSheet("color:#b42318;")
            self.send_target_status_label.setText(
                f"当前发送识别列为“{target_column}”，但已读取的 Excel 中没有这列。"
            )
            return

        self.send_target_status_label.setStyleSheet("color:#027a48;")
        self.send_target_status_label.setText(
            f"当前发送识别列为“{target_column}”，已匹配 {valid_count} 行可发送数据。"
        )

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
            self.placeholder_status_label.setStyleSheet("color:#555;")
            self.placeholder_status_label.setText("当前模板未使用占位符，将向所有联系人发送相同内容。")
            return

        missing_fields = find_missing_fields(placeholders, self.columns)
        text = f"识别到占位符：{', '.join(placeholders)}"
        if missing_fields:
            self.placeholder_status_label.setStyleSheet("color:#b42318;")
            self.placeholder_status_label.setText(
                text + f"\n缺少对应列：{', '.join(missing_fields)}"
            )
        else:
            self.placeholder_status_label.setStyleSheet("color:#027a48;")
            self.placeholder_status_label.setText(
                text + "\n所有占位符都能在 Excel 列中找到对应内容。"
            )

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

        preview_records = self.records[: self.preview_limit]
        self.preview_table.setRowCount(len(preview_records))
        allow_edit = self.can_edit_preview_rows()

        for row_index, row in enumerate(preview_records):
            target_value = self.get_send_target_value(row)
            display_name = self.get_display_name(row)
            preview_message = self.get_preview_message(row)

            wechat_item = QTableWidgetItem(target_value)
            display_item = QTableWidgetItem(display_name)
            message_item = QTableWidgetItem(preview_message)
            if not allow_edit:
                readonly_flags = wechat_item.flags() & ~Qt.ItemIsEditable
                wechat_item.setFlags(readonly_flags)
                display_item.setFlags(display_item.flags() & ~Qt.ItemIsEditable)
                message_item.setFlags(message_item.flags() & ~Qt.ItemIsEditable)

            self.preview_table.setItem(row_index, 0, wechat_item)
            self.preview_table.setItem(row_index, 1, display_item)
            self.preview_table.setItem(row_index, 2, message_item)

            delete_button = QPushButton("删除", self.preview_table)
            delete_button.clicked.connect(lambda _, index=row_index: self.delete_preview_row(index))
            delete_button.setEnabled(allow_edit)
            self.preview_table.setCellWidget(row_index, 3, delete_button)

        self.preview_table.resizeRowsToContents()
        self.preview_table.blockSignals(False)
        self._updating_preview_table = False

    def show_preview_results(self) -> None:
        self.render_preview()
        self.main_tabs.setCurrentWidget(self.execution_page)

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

    def build_snapshot_rows(self, records: list[dict[str, str]]) -> list[dict[str, str]]:
        snapshot_rows: list[dict[str, str]] = []
        for row in records:
            snapshot_row = dict(row)
            snapshot_row[TARGET_VALUE_KEY] = self.get_send_target_value(snapshot_row)
            snapshot_rows.append(snapshot_row)
        return snapshot_rows

    def create_task_snapshot_from_records(self, records: list[dict[str, str]]) -> int:
        dataset_type = self.get_current_dataset_type(records)
        snapshot_rows = self.build_snapshot_rows(records)
        return self.local_store.create_task_snapshot(
            rows=snapshot_rows,
            filter_fields=self.filter_fields_input.text().strip(),
            filter_pattern=self.filter_pattern_input.text().strip(),
            target_column=self.get_send_target_column(),
            template_text=self.template_input.toPlainText(),
            source_batch_id=self.get_source_batch_id_for_snapshot(dataset_type),
            source_mode=self.active_source_mode,
            dataset_type=dataset_type,
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
            if not has_custom_messages:
                return None, "请输入要发送的消息模板，或先在预览表中手动编辑每行消息。"
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
        if self.scheduled_mode_radio.isChecked():
            self.queue_scheduled_send()
            return

        if self.send_thread is not None and self.send_thread.isRunning():
            QMessageBox.information(self, "发送中", "当前已有发送任务正在执行。")
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
        )
        self.refresh_scheduled_jobs()
        self.append_log(
            f"已创建定时任务 {job_id}，计划于 {scheduled_at.toString('yyyy-MM-dd HH:mm')} 执行，发送对象 {len(records)} 个。"
        )
        QMessageBox.information(
            self,
            "定时任务已创建",
            f"任务ID：{job_id}\n计划时间：{scheduled_at.toString('yyyy-MM-dd HH:mm')}\n发送人数：{len(records)}",
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
    ) -> None:
        self.active_scheduled_job = scheduled_job
        self.send_thread = PersonalizedSendThread(
            records=records,
            template=template_text,
            interval_seconds=interval_seconds,
            target_column=target_column,
            locale=self.config["settings"]["language"],
            random_delay_min=random_delay_min,
            random_delay_max=random_delay_max,
            operator_name=operator_name,
            report_to=report_to,
            auto_report=auto_report,
        )
        self.send_thread.progress.connect(self.on_send_progress)
        self.send_thread.log.connect(self.append_log)
        self.send_thread.error.connect(self.on_send_error)
        self.send_thread.completed.connect(self.on_send_completed)
        self.send_thread.finished.connect(self.on_thread_finished)
        self.send_thread.start()

        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.preview_button.setEnabled(False)
        self.confirm_task_button.setEnabled(False)
        self.load_excel_button.setEnabled(False)
        self.import_local_button.setEnabled(False)
        self.load_local_button.setEnabled(False)
        self.preview_table.setEnabled(False)
        self.send_status_label.setText("发送中...")
        self.main_tabs.setCurrentWidget(self.execution_page)
        if scheduled_job is not None:
            self.append_log(f"开始执行定时任务 {scheduled_job.job_id}，任务快照 {scheduled_job.task_id}。")
        elif self.is_local_db_mode():
            self.append_log(f"开始执行本地库任务快照发送，任务ID={self.current_task_id}。")
        else:
            self.append_log("开始执行 Excel 个性化群发。")

    def stop_sending(self) -> None:
        if self.send_thread is not None and self.send_thread.isRunning():
            self.send_thread.request_stop()
            self.stop_button.setEnabled(False)
            self.send_status_label.setText("正在停止...")
            self.append_log("已收到停止请求，等待当前发送动作完成。")

    def on_send_progress(self, current: int, total: int, wechat_id: str) -> None:
        self.send_status_label.setText(f"发送进度：{current}/{total}")
        self.append_log(f"进度更新：{current}/{total} -> {wechat_id}")

    def on_send_error(self, error_message: str) -> None:
        self.append_log(f"发送线程异常：{error_message}")
        if self.active_scheduled_job is None:
            QMessageBox.warning(self, "发送异常", error_message)

    def on_send_completed(self, summary: dict) -> None:
        message = (
            f"总数：{summary['total']}\n"
            f"已发送：{summary['sent']}\n"
            f"失败：{summary['failed']}\n"
            f"跳过：{summary['skipped']}"
        )
        if summary.get("random_delay_count"):
            message += f"\n随机延迟事务：{summary['random_delay_count']} 次"
        if summary.get("report_sent"):
            message += "\n自动汇报：已发送"
        elif summary.get("report_error"):
            message += f"\n自动汇报失败：{summary['report_error']}"
        if summary.get("error"):
            message += f"\n线程异常：{summary['error']}"
        if summary.get("stopped"):
            message += "\n状态：已手动停止"
        else:
            message += "\n状态：已完成"

        self.send_status_label.setText("发送结束。")
        self.append_log("发送任务结束。")
        if self.active_scheduled_job is not None:
            if summary.get("error"):
                job_status = SCHEDULE_STATUS_FAILED
            elif summary.get("stopped"):
                job_status = SCHEDULE_STATUS_CANCELLED
            else:
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
        self.load_local_button.setEnabled(True)
        self.preview_table.setEnabled(True)
        self.send_thread = None
        self.active_scheduled_job = None
        self.refresh_scheduled_jobs()
        self.update_action_button_state()

    def append_log(self, message: str) -> None:
        self.log_view.appendPlainText(message)
        self.log_view.verticalScrollBar().setValue(self.log_view.verticalScrollBar().maximum())

    def showEvent(self, event) -> None:
        super().showEvent(event)
        if self._startup_layout_refreshed:
            return
        self._startup_layout_refreshed = True
        QTimer.singleShot(0, self.refresh_startup_layout)
        QTimer.singleShot(80, self.refresh_startup_layout)

    def refresh_startup_layout(self) -> None:
        root_layout = self.layout()
        if root_layout is not None:
            root_layout.activate()
        self.updateGeometry()
        self.main_tabs.updateGeometry()
        for widget in (self.data_template_page, self.local_store_page, self.execution_page):
            child_layout = widget.layout()
            if child_layout is not None:
                child_layout.activate()
            widget.updateGeometry()

        width = max(self.width(), self.minimumWidth(), 1120)
        height = max(self.height(), self.minimumHeight(), 860)
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
        super().closeEvent(event)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ExcelSenderGUI()
    window.show()
    sys.exit(app.exec_())
