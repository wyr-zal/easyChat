import re
import json
import os
import sys
from pathlib import Path

from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QButtonGroup,
    QCheckBox,
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
from excel_sender_service import CUSTOM_MESSAGE_OVERRIDE_KEY, PersonalizedSendThread
from excel_template import extract_placeholders, find_missing_fields, render_template
from module import FileDropLineEdit


DISPLAY_NAME_OVERRIDE_KEY = "__display_name_override"
RECORD_ID_KEY = "__record_id"


class ExcelSenderGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.config_path = "excel_sender_config.json"
        self.config = self.load_config()
        self.records: list[dict[str, str]] = []
        self.source_records: list[dict[str, str]] = []
        self.columns: list[str] = []
        self.send_thread: PersonalizedSendThread | None = None
        self.preview_limit = 10
        self.records_loaded = False
        self.loaded_excel_path = ""
        self._updating_preview_table = False
        self.template_change_timer = QTimer(self)
        self.template_change_timer.setSingleShot(True)
        self.template_change_timer.timeout.connect(self.apply_template_changes)

        self.init_ui()
        self.restore_initial_state()

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
        self.main_tabs.addTab(self.build_settings_page(), "基础设置")
        self.main_tabs.addTab(self.build_data_template_page(), "数据与模板")
        self.main_tabs.addTab(self.build_execution_page(), "发送执行")

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

    def build_settings_page(self) -> QWidget:
        page = QWidget(self)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)
        layout.addWidget(self.build_settings_group())
        layout.addStretch(1)
        return page

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
        layout.addWidget(self.build_filter_group())
        layout.addWidget(self.build_template_group())
        layout.addStretch(1)
        return page

    def build_execution_page(self) -> QWidget:
        page = QWidget(self)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)
        layout.addWidget(self.build_control_group())
        layout.addWidget(self.build_preview_group(), stretch=3)
        layout.addWidget(self.build_log_group(), stretch=2)
        return page

    def build_control_group(self) -> QGroupBox:
        group = QGroupBox("发送控制")
        layout = QVBoxLayout(group)
        layout.setSpacing(12)

        helper_label = QLabel("建议先完成 Excel 数据读取和模板检查，再执行预览或发送。")
        helper_label.setWordWrap(True)
        helper_label.setStyleSheet("color:#555;")
        layout.addWidget(helper_label)
        layout.addLayout(self.build_action_bar())

        detail_label = QLabel("“刷新预览”会渲染前 10 条消息；“开始发送”会重新执行发送前校验。")
        detail_label.setWordWrap(True)
        detail_label.setStyleSheet("color:#555;")
        layout.addWidget(detail_label)
        return group

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

        layout.addLayout(path_layout)

        self.data_info_label = QLabel("尚未读取 Excel 数据。")
        self.data_info_label.setWordWrap(True)
        layout.addWidget(self.data_info_label)

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
        group = QGroupBox("筛选配置")
        layout = QVBoxLayout(group)
        layout.setSpacing(10)

        tip_label = QLabel(
            "支持对 Excel 数据做正则筛选。默认字段为“场景”，多个字段请用英文逗号分隔，例如：场景,标签,备注。"
        )
        tip_label.setWordWrap(True)
        layout.addWidget(tip_label)

        fields_layout = QHBoxLayout()
        fields_layout.addWidget(QLabel("筛选字段"))
        self.filter_fields_input = QLineEdit(self)
        self.filter_fields_input.setPlaceholderText("例如：场景,标签,备注")
        self.filter_fields_input.textChanged.connect(self.on_filter_fields_changed)
        fields_layout.addWidget(self.filter_fields_input)
        layout.addLayout(fields_layout)

        pattern_layout = QHBoxLayout()
        pattern_layout.addWidget(QLabel("正则规则"))
        self.filter_pattern_input = QLineEdit(self)
        self.filter_pattern_input.setPlaceholderText("例如：到期|续费|复购")
        self.filter_pattern_input.textChanged.connect(self.on_filter_pattern_changed)
        pattern_layout.addWidget(self.filter_pattern_input)
        layout.addLayout(pattern_layout)

        action_layout = QHBoxLayout()
        self.filter_ignore_case_checkbox = QCheckBox("忽略大小写", self)
        self.filter_ignore_case_checkbox.toggled.connect(self.on_filter_ignore_case_changed)
        self.apply_filter_button = QPushButton("应用筛选")
        self.apply_filter_button.clicked.connect(self.apply_regex_filter)
        self.reset_filter_button = QPushButton("重置筛选")
        self.reset_filter_button.clicked.connect(self.reset_regex_filter)
        action_layout.addWidget(self.filter_ignore_case_checkbox)
        action_layout.addWidget(self.apply_filter_button)
        action_layout.addWidget(self.reset_filter_button)
        action_layout.addStretch(1)
        layout.addLayout(action_layout)

        self.filter_status_label = QLabel("未应用正则筛选。")
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

        language = self.config["settings"]["language"]
        if language == "zh-TW":
            self.lang_zh_tw.setChecked(True)
        elif language == "en-US":
            self.lang_en.setChecked(True)
        else:
            self.lang_zh_cn.setChecked(True)

        self.update_placeholder_status()
        self.update_send_target_column_status()
        self.main_tabs.setCurrentIndex(1)
        if self.excel_path_input.text().strip():
            self.load_excel_data(show_success=False)
        else:
            self.render_preview()

    def set_language(self, language: str) -> None:
        self.config["settings"]["language"] = language
        self.save_config()

    def on_excel_path_changed(self, path: str) -> None:
        normalized_path = path.strip()
        self.config["excel"]["path"] = normalized_path
        if normalized_path != self.loaded_excel_path:
            self.records_loaded = False
        self.save_config()

    def on_template_changed(self) -> None:
        self.template_change_timer.start(500)

    def apply_template_changes(self) -> None:
        self.config["template"]["text"] = self.template_input.toPlainText()
        self.save_config()
        self.update_placeholder_status()

    def on_send_target_column_changed(self, value: str) -> None:
        self.config["excel"]["send_target_column"] = value.strip() or DEFAULT_SEND_TARGET_COLUMN
        self.save_config()
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
            validate_contact_records(records, columns, self.get_send_target_column())
        except Exception as exc:
            self.records = []
            self.source_records = []
            self.columns = []
            self.records_loaded = False
            self.loaded_excel_path = ""
            self.columns_view.clear()
            self.data_info_label.setText("读取失败。")
            self.filter_status_label.setText("筛选配置待重新应用。")
            self.update_send_target_column_status()
            self.update_preview_headers()
            self.update_placeholder_status()
            self.render_preview()
            QMessageBox.warning(self, "读取失败", f"读取 Excel 失败！\n错误信息：{exc}")
            return False

        self.source_records = self.attach_record_ids(records)
        self.records = list(self.source_records)
        self.columns = columns
        self.records_loaded = True
        self.loaded_excel_path = path
        self.columns_view.setPlainText("、".join(columns))
        self.update_preview_headers()
        self.update_send_target_column_status()
        self.update_data_info_label()
        if self.filter_pattern_input.text().strip():
            self.filter_status_label.setText("已读取新数据，点击“应用筛选”可按当前规则筛选。")
        else:
            self.filter_status_label.setText("已读取数据，当前未应用正则筛选。")
        self.update_placeholder_status()
        self.render_preview()

        if show_success:
            QMessageBox.information(self, "读取成功", f"已成功读取 {len(self.records)} 行 Excel 数据。")
        return True

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
        current_total = len(self.records)
        target_column = self.get_send_target_column()
        valid_count = len([row for row in self.records if self.get_send_target_value(row)])

        if current_total != source_total:
            self.data_info_label.setText(
                f"已读取 {source_total} 行数据，当前筛选后 {current_total} 行，其中 {valid_count} 行包含可发送的“{target_column}”值。"
            )
        else:
            self.data_info_label.setText(
                f"已读取 {current_total} 行数据，其中 {valid_count} 行包含可发送的“{target_column}”值。"
            )

    def get_send_target_column(self) -> str:
        return self.send_target_column_input.text().strip() or DEFAULT_SEND_TARGET_COLUMN

    def get_send_target_value(self, row: dict[str, str]) -> str:
        return (row.get(self.get_send_target_column()) or "").strip()

    def update_preview_headers(self) -> None:
        if not hasattr(self, "preview_table"):
            return
        self.preview_table.setHorizontalHeaderLabels(
            [self.get_send_target_column(), "显示名称", "预览消息", "操作"]
        )

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

        if target_column not in self.columns:
            self.send_target_status_label.setStyleSheet("color:#b42318;")
            self.send_target_status_label.setText(
                f"当前发送识别列为“{target_column}”，但已读取的 Excel 中没有这列。"
            )
            return

        valid_count = len([row for row in self.records if (row.get(target_column) or "").strip()])
        self.send_target_status_label.setStyleSheet("color:#027a48;")
        self.send_target_status_label.setText(
            f"当前发送识别列为“{target_column}”，已匹配 {valid_count} 行可发送数据。"
        )

    def apply_regex_filter(self) -> None:
        if not self.records_loaded and not self.load_excel_data(show_success=False):
            return

        pattern_text = self.filter_pattern_input.text().strip()
        if pattern_text == "":
            QMessageBox.warning(self, "筛选配置", "请输入正则规则后再应用筛选。")
            return

        fields = [field.strip() for field in self.filter_fields_input.text().split(",") if field.strip()]
        if not fields:
            fields = ["场景"]
            self.filter_fields_input.setText("场景")

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

        self.records = [
            record for record in self.source_records
            if self.record_matches_regex(record, fields, regex)
        ]
        self.update_data_info_label()
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

        self.records = list(self.source_records)
        self.update_data_info_label()
        self.filter_status_label.setText("已重置筛选，当前显示全部发送数据。")
        self.append_log("已重置正则筛选，恢复全部发送数据。")
        self.render_preview()

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

        if not self.records:
            self.preview_table.blockSignals(False)
            self._updating_preview_table = False
            return

        preview_records = self.records[: self.preview_limit]
        self.preview_table.setRowCount(len(preview_records))

        for row_index, row in enumerate(preview_records):
            target_value = self.get_send_target_value(row)
            display_name = self.get_display_name(row)
            preview_message = self.get_preview_message(row)

            wechat_item = QTableWidgetItem(target_value)
            display_item = QTableWidgetItem(display_name)
            message_item = QTableWidgetItem(preview_message)

            self.preview_table.setItem(row_index, 0, wechat_item)
            self.preview_table.setItem(row_index, 1, display_item)
            self.preview_table.setItem(row_index, 2, message_item)

            delete_button = QPushButton("删除", self.preview_table)
            delete_button.clicked.connect(lambda _, index=row_index: self.delete_preview_row(index))
            self.preview_table.setCellWidget(row_index, 3, delete_button)

        self.preview_table.resizeRowsToContents()
        self.preview_table.blockSignals(False)
        self._updating_preview_table = False

    def show_preview_results(self) -> None:
        self.render_preview()
        self.main_tabs.setCurrentIndex(2)

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
        if self._updating_preview_table:
            return

        row_index = item.row()
        if row_index >= len(self.records):
            return

        record = self.records[row_index]
        text = item.text()

        if item.column() == 0:
            record[self.get_send_target_column()] = text.strip()
        elif item.column() == 1:
            record[DISPLAY_NAME_OVERRIDE_KEY] = text.strip()
        elif item.column() == 2:
            record[CUSTOM_MESSAGE_OVERRIDE_KEY] = text

        self.preview_table.resizeRowsToContents()

    def delete_preview_row(self, row_index: int) -> None:
        if self.send_thread is not None and self.send_thread.isRunning():
            return

        if row_index >= len(self.records):
            return

        deleted_record = self.records.pop(row_index)
        record_id = deleted_record.get(RECORD_ID_KEY)
        if record_id:
            self.source_records = [
                record for record in self.source_records
                if record.get(RECORD_ID_KEY) != record_id
            ]
            self.records = [
                record for record in self.records
                if record.get(RECORD_ID_KEY) != record_id
            ]
        deleted_name = self.get_display_name(deleted_record) or self.get_send_target_value(deleted_record)
        self.update_data_info_label()
        self.update_send_target_column_status()
        self.append_log(f"已从发送名单中删除：{deleted_name}")
        self.render_preview()

    def validate_before_send(self) -> tuple[list[dict[str, str]] | None, str | None]:
        if not self.records_loaded and not self.load_excel_data(show_success=False):
            return None, "Excel 数据读取失败。"

        template = self.template_input.toPlainText()
        if template.strip() == "":
            has_custom_messages = any(
                str(row.get(CUSTOM_MESSAGE_OVERRIDE_KEY, "")).strip()
                for row in self.records
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
        if self.columns and target_column not in self.columns:
            return None, f"当前发送识别列“{target_column}”不在 Excel 列名中。"

        valid_records = [row for row in self.records if self.get_send_target_value(row)]
        if not valid_records:
            return None, f"Excel 中没有可发送的“{target_column}”数据。"

        return valid_records, None

    def start_sending(self) -> None:
        if self.send_thread is not None and self.send_thread.isRunning():
            QMessageBox.information(self, "发送中", "当前已有发送任务正在执行。")
            return

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

        self.send_thread = PersonalizedSendThread(
            records=records,
            template=self.template_input.toPlainText(),
            interval_seconds=self.interval_spin.value(),
            target_column=self.get_send_target_column(),
            locale=self.config["settings"]["language"],
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
        self.load_excel_button.setEnabled(False)
        self.preview_table.setEnabled(False)
        self.send_status_label.setText("发送中...")
        self.main_tabs.setCurrentIndex(2)
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
        QMessageBox.warning(self, "发送异常", error_message)

    def on_send_completed(self, summary: dict) -> None:
        message = (
            f"总数：{summary['total']}\n"
            f"已发送：{summary['sent']}\n"
            f"失败：{summary['failed']}\n"
            f"跳过：{summary['skipped']}"
        )
        if summary.get("stopped"):
            message += "\n状态：已手动停止"
        else:
            message += "\n状态：已完成"

        self.send_status_label.setText("发送结束。")
        self.append_log("发送任务结束。")
        QMessageBox.information(self, "发送结果", message)

    def on_thread_finished(self) -> None:
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.preview_button.setEnabled(True)
        self.load_excel_button.setEnabled(True)
        self.preview_table.setEnabled(True)
        self.send_thread = None

    def append_log(self, message: str) -> None:
        self.log_view.appendPlainText(message)
        self.log_view.verticalScrollBar().setValue(self.log_view.verticalScrollBar().maximum())

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
