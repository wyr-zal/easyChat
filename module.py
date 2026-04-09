import sys
import time
import datetime
import threading
import keyboard
from pathlib import Path

from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

STANDARD_DIALOG_BUTTON_STYLE = """
QPushButton {
    font-size: 12pt;
    font-weight: 500;
    min-height: 36px;
    padding: 4px 12px;
}
QTableWidget QPushButton {
    min-height: 30px;
    padding: 2px 10px;
}
"""

STANDARD_UI_FONT_SIZE = 12
HELPER_UI_FONT_SIZE = 11


def build_ui_font(widget: QWidget | None = None, *, point_size: int = STANDARD_UI_FONT_SIZE, bold: bool = False) -> QFont:
    font = QFont(widget.font() if widget is not None else QFont())
    font.setPointSize(point_size)
    font.setBold(bold)
    return font


def apply_window_font_scaling(widget: QWidget) -> None:
    widget.setFont(build_ui_font(widget, point_size=STANDARD_UI_FONT_SIZE))


def style_helper_label(
    label: QLabel,
    *,
    color: str | None = "#555",
    point_size: int = HELPER_UI_FONT_SIZE,
) -> QLabel:
    label.setWordWrap(True)
    label.setFont(build_ui_font(label, point_size=point_size))
    if color:
        label.setStyleSheet(f"color:{color};")
    return label


# 定时发送子线程类
class ClockThread(QThread):
    def __init__(self):
        super().__init__()
        # 是否正在定时
        self.time_counting = False
        # 发送信息的函数
        self.send_func = None
        # 定时列表
        self.clocks = None
        # 是否防止自动下线
        self.prevent_offline = False
        self.prevent_func = None
        # 每隔多少分钟进行一次防止自动下线操作
        self.prevent_count = 60

        # 新增：用于存储已执行过的任务标识，防止重复执行
        self.executed_tasks = set()

        # 用于防止掉线的内部计时器
        self._prevent_timer = 0

    def __del__(self):
        self.wait()

    def run(self):
        import uiautomation as auto
        with auto.UIAutomationInitializerInThread():
            # 初始化防止掉线的计时器，设置为 prevent_count 分钟对应的秒数
            self._prevent_timer = self.prevent_count * 60

            while self.time_counting:
                now = datetime.datetime.now()
                next_event_time = None

                # --- 1. 遍历列表，查找最近的下一个闹钟时间 ---
                try:
                    for i in range(self.clocks.count()):
                        task_id = self.clocks.item(i).text()
                        # 如果任务已经执行过，则跳过
                        if task_id in self.executed_tasks:
                            continue

                        parts = task_id.split(" ")
                        clock_str = " ".join(parts[:5])
                        dt_obj = datetime.datetime.strptime(clock_str, "%Y %m %d %H %M")

                        # 只关心未来的任务
                        if dt_obj > now:
                            # 如果是第一个找到的未来任务，或者比已知的下一个任务更早
                            if next_event_time is None or dt_obj < next_event_time:
                                next_event_time = dt_obj
                except Exception as e:
                    # 在UI更新列表时，直接读取可能会有瞬时错误，做个保护
                    print(f"读取闹钟列表时出错: {e}")
                    time.sleep(1)  # 出错时短暂休眠后重试
                    continue

                # --- 2. 计算休眠时间 ---
                sleep_seconds = 0  # 默认休眠0秒，如果没有找到任何未来任务

                if next_event_time:
                    delta = (next_event_time - now).total_seconds()
                    # 确保休眠时间不为负
                    sleep_seconds = max(0, delta)

                print(sleep_seconds)

                # --- 3. 整合“防止掉线”的逻辑 ---
                if self.prevent_offline:
                    # 取“下一个闹钟”和“下一次防掉线”中更早发生的一个
                    sleep_seconds = min(sleep_seconds, self._prevent_timer)

                # --- 4. 执行休眠 ---
                # sleep_seconds 可能是小数，time.sleep支持
                time.sleep(sleep_seconds)

                # 更新防止掉线的内部计时器
                self._prevent_timer -= sleep_seconds
                if self._prevent_timer <= 0:
                    self._prevent_timer = 0  # 避免变为很大的负数

                # --- 5. 休眠结束，检查并执行到期的任务 ---
                now = datetime.datetime.now()  # 获取唤醒后的精确时间

                # 检查并执行到期的闹钟
                try:
                    for i in range(self.clocks.count()):
                        task_id = self.clocks.item(i).text()
                        print(task_id)
                        if task_id in self.executed_tasks:
                            continue

                        parts = task_id.split(" ")
                        st_ed = parts[5]
                        st, ed = st_ed.split('-')
                        clock_str = " ".join(parts[:5])
                        dt_obj = datetime.datetime.strptime(clock_str, "%Y %m %d %H %M")

                        # 只执行刚刚到期的任务（时间窗口：60秒内）
                        time_diff = (now - dt_obj).total_seconds()
                        if 0 <= time_diff <= 60:
                            if self.send_func:
                                self.send_func(st=int(st), ed=int(ed))
                            # 记录为已执行
                            self.executed_tasks.add(task_id)
                        elif time_diff > 60:
                            # 超过60秒的任务标记为已过期，不再执行
                            self.executed_tasks.add(task_id)

                except Exception as e:
                    print(f"执行任务时读取闹钟列表出错: {e}")

                # 检查并执行防止掉线
                if self.prevent_offline and self._prevent_timer <= 0:
                    if self.prevent_func:
                        self.prevent_func()
                    # 重置计时器
                    self._prevent_timer = self.prevent_count * 60


class MyListWidget(QListWidget):
    """支持双击可编辑的QListWidget"""
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setSelectionMode(QAbstractItemView.ExtendedSelection)  # 设置选择多个

        # 双击可编辑
        self.edited_item = self.currentItem()
        self.close_flag = True
        self.doubleClicked.connect(self.item_double_clicked)
        self.currentItemChanged.connect(self.close_edit)

    def keyPressEvent(self, e: QKeyEvent) -> None:
        """回车事件，关闭edit"""
        super().keyPressEvent(e)
        if e.key() == Qt.Key_Return:
            if self.close_flag:
                self.close_edit()
            self.close_flag = True

    def edit_new_item(self) -> None:
        """edit一个新的item"""
        self.close_flag = False
        self.close_edit()
        count = self.count()
        self.addItem('')
        item = self.item(count)
        self.edited_item = item
        self.openPersistentEditor(item)
        self.editItem(item)

    def item_double_clicked(self, modelindex: QModelIndex) -> None:
        """双击事件"""
        self.close_edit()
        item = self.item(modelindex.row())
        self.edited_item = item
        self.openPersistentEditor(item)
        self.editItem(item)

    def close_edit(self, *_) -> None:
        """关闭edit"""
        if self.edited_item and self.isPersistentEditorOpen(self.edited_item):
            self.closePersistentEditor(self.edited_item)


class MultiInputDialog(QDialog):
    """
    用于用户输入的输入框，可以根据传入的参数自动创建输入框
    """
    def __init__(self, inputs: list, default_values: list = None, parent=None) -> None:
        """
        inputs: list, 代表需要input的标签，如['姓名', '年龄']
        default_values: list, 代表默认值，如['张三', '18']
        """
        super().__init__(parent)
        apply_window_font_scaling(self)
        self.setStyleSheet(STANDARD_DIALOG_BUTTON_STYLE)
        
        layout = QVBoxLayout(self)
        self.inputs = []
        for n, i in enumerate(inputs):
            layout.addWidget(QLabel(i))
            input = QLineEdit(self)

            # 设置默认值
            if default_values is not None:
                input.setText(default_values[n])

            layout.addWidget(input)
            self.inputs.append(input)
            
        ok_button = QPushButton("确认")
        ok_button.clicked.connect(self.accept)
        
        cancel_button = QPushButton("取消")
        cancel_button.clicked.connect(self.reject)
        
        button_layout = QHBoxLayout()
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)
    
    def get_input(self):
        """获取用户输入"""
        return [i.text() for i in self.inputs]


class FileDropLineEdit(QLineEdit):
    """支持从资源管理器拖入文件路径的输入框"""

    def __init__(self, allow_multiple: bool = False, suffixes: list[str] | None = None, parent=None) -> None:
        super().__init__(parent)
        self.allow_multiple = allow_multiple
        self.suffixes = [suffix.lower() for suffix in (suffixes or [])]
        self.setAcceptDrops(True)

    def _extract_paths(self, mime_data: QMimeData) -> list[str]:
        if not mime_data.hasUrls():
            return []

        paths = []
        for url in mime_data.urls():
            if url.isLocalFile():
                path = url.toLocalFile()
                if self.suffixes:
                    lower_path = path.lower()
                    if not any(lower_path.endswith(suffix) for suffix in self.suffixes):
                        continue
                paths.append(path)
        return paths

    def dragEnterEvent(self, event) -> None:
        paths = self._extract_paths(event.mimeData())
        if paths:
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event) -> None:
        paths = self._extract_paths(event.mimeData())
        if not paths:
            event.ignore()
            return

        if self.allow_multiple:
            self.setText(";".join(paths))
        else:
            self.setText(paths[0])
        event.acceptProposedAction()


class FileDialog(QDialog):
    """
    文件选择框
    """
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        apply_window_font_scaling(self)
        self.setStyleSheet(STANDARD_DIALOG_BUTTON_STYLE)
        self.inputs = []
        layout = QVBoxLayout(self)
        
        target_tip_label = QLabel("请指定发送给哪些用户(1,2,3代表发送给前三位用户)，如需全部发送请忽略此项")
        style_helper_label(target_tip_label)
        layout.addWidget(target_tip_label)
        input = QLineEdit(self)
        layout.addWidget(input)
        self.inputs.append(input)
        
        # 选择文件
        choose_layout = QHBoxLayout()

        path = FileDropLineEdit(allow_multiple=True, parent=self)
        path.setPlaceholderText("可点击右侧选择文件，或直接把文件拖到这里")
        choose_layout.addWidget(path)
        self.inputs.append(path)

        file_button = QPushButton("选择文件")
        file_button.clicked.connect(self.select)
        choose_layout.addWidget(file_button)

        layout.addLayout(choose_layout)
        
        # 确认按钮
        ok_button = QPushButton("确认")
        ok_button.clicked.connect(self.accept)

        # 取消按钮
        cancel_button = QPushButton("取消")
        cancel_button.clicked.connect(self.reject)

        # 按钮布局
        button_layout = QHBoxLayout()
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)
    
    def select(self):
        path_input = self.inputs[1]
        # 修改为支持多文件选择
        paths = QFileDialog.getOpenFileNames(self, '打开文件', '/home')[0]
        if paths:
            # 将多个文件路径用分号连接显示
            path_input.setText(";".join(paths))
    
    def get_input(self):
        """获取用户输入"""
        return [i.text() for i in self.inputs]


class ContactFilterDialog(QDialog):
    """联系人 CSV 筛选对话框（支持多CSV、通配符、分类型搜索字段配置）"""

    SEARCH_FIELDS = ["备注", "显示名称", "昵称", "微信号", "用户名"]

    def __init__(
        self,
        csv_paths: list = None,
        pattern: str = "",
        fields: str = "",
        contact_type: str = "",
        ignore_case: bool = False,
        friend_search_field: str = "备注",
        group_search_field: str = "显示名称",
        parent=None,
    ) -> None:
        super().__init__(parent)
        apply_window_font_scaling(self)
        self.setStyleSheet(STANDARD_DIALOG_BUTTON_STYLE)
        self.setWindowTitle("从CSV导入联系人")
        self.resize(800, 480)
        self.setMinimumSize(720, 440)
        self.setSizeGripEnabled(True)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(8)

        helper_label = QLabel("支持多 CSV 拖入；筛选模式支持通配符或正则；规则留空时会按当前范围全量预览。")
        style_helper_label(helper_label)
        layout.addWidget(helper_label)

        # ── CSV 文件选择（多文件）──
        layout.addWidget(QLabel("联系人CSV文件"))
        csv_layout = QHBoxLayout()
        self.csv_input = FileDropLineEdit(allow_multiple=True, suffixes=[".csv"], parent=self)
        self.csv_input.setText(";".join(csv_paths) if csv_paths else "")
        self.csv_input.setPlaceholderText("可点击右侧选择CSV，或直接把文件拖到这里（多文件用分号隔开）")
        csv_layout.addWidget(self.csv_input)
        choose_btn = QPushButton("选择CSV")
        choose_btn.setMinimumWidth(100)
        choose_btn.clicked.connect(self.select_csv)
        csv_layout.addWidget(choose_btn)
        layout.addLayout(csv_layout)

        # ── 筛选模式输入 ──
        layout.addWidget(QLabel("筛选模式"))
        self.pattern_input = QLineEdit(self)
        self.pattern_input.setText(pattern)
        self.pattern_input.setPlaceholderText("例：*陈* 或 ^张.* 或留空全选")
        layout.addWidget(self.pattern_input)

        # ── 参与匹配的字段 ──
        layout.addWidget(QLabel("匹配字段"))
        self.fields_input = QLineEdit(self)
        self.fields_input.setText(fields)
        self.fields_input.setPlaceholderText("显示名称,备注,昵称")
        layout.addWidget(self.fields_input)

        # ── 联系人类型 ──
        layout.addWidget(QLabel("联系人类型"))
        self.contact_type_input = QLineEdit(self)
        self.contact_type_input.setText(contact_type)
        self.contact_type_input.setPlaceholderText("留空则好友和群聊都导入")
        layout.addWidget(self.contact_type_input)

        # ── 分类型搜索识别字段 ──
        search_field_layout = QGridLayout()
        search_field_layout.setHorizontalSpacing(16)

        search_field_layout.addWidget(QLabel("好友 微信搜索框用的字段："), 0, 0)
        self.friend_search_combo = QComboBox(self)
        self.friend_search_combo.addItems(self.SEARCH_FIELDS)
        idx = self.friend_search_combo.findText(friend_search_field)
        self.friend_search_combo.setCurrentIndex(max(idx, 0))
        search_field_layout.addWidget(self.friend_search_combo, 0, 1)

        search_field_layout.addWidget(QLabel("群聊 微信搜索框用的字段："), 0, 2)
        self.group_search_combo = QComboBox(self)
        self.group_search_combo.addItems(self.SEARCH_FIELDS)
        idx2 = self.group_search_combo.findText(group_search_field)
        self.group_search_combo.setCurrentIndex(max(idx2, 0))
        search_field_layout.addWidget(self.group_search_combo, 0, 3)

        layout.addLayout(search_field_layout)

        # ── 忽略大小写 ──
        self.ignore_case_checkbox = QCheckBox("忽略大小写")
        self.ignore_case_checkbox.setChecked(ignore_case)
        layout.addWidget(self.ignore_case_checkbox)

        # ── 按钮 ──
        button_layout = QHBoxLayout()
        ok_button = QPushButton("筛选并预览")
        ok_button.setMinimumWidth(130)
        ok_button.clicked.connect(self.accept)
        cancel_button = QPushButton("取消")
        cancel_button.setMinimumWidth(100)
        cancel_button.clicked.connect(self.reject)
        button_layout.addStretch()
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)

    def select_csv(self):
        paths, _ = QFileDialog.getOpenFileNames(self, "选择联系人CSV", "", "CSV文件(*.csv)")
        if paths:
            existing = self.csv_input.text().strip()
            all_paths = [p for p in existing.split(";") if p.strip()] + paths
            self.csv_input.setText(";".join(all_paths))

    def get_input(self) -> dict:
        return {
            "csv_paths": [p.strip() for p in self.csv_input.text().split(";") if p.strip()],
            "pattern": self.pattern_input.text().strip(),
            "fields": self.fields_input.text().strip(),
            "contact_type": self.contact_type_input.text().strip(),
            "ignore_case": self.ignore_case_checkbox.isChecked(),
            "friend_search_field": self.friend_search_combo.currentText(),
            "group_search_field": self.group_search_combo.currentText(),
        }


class ContactConfirmDialog(QDialog):
    """联系人确认弹窗：展示筛选结果，支持逐行移除、全选/取消，最终返回确认列表"""

    def __init__(self, contacts: list[dict], parent=None) -> None:
        """
        contacts: 每项为 dict，包含 显示名称/备注/微信号/类型/_search_key
        """
        super().__init__(parent)
        apply_window_font_scaling(self)
        self.setStyleSheet(STANDARD_DIALOG_BUTTON_STYLE)
        self.setWindowTitle("确认导入联系人")
        self.resize(860, 520)
        self.setMinimumSize(700, 400)
        self.setSizeGripEnabled(True)

        # 内部维护一份数据副本（不含已移除行）
        self._contacts = list(contacts)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)

        # ── 顶部信息栏 ──
        top_bar = QHBoxLayout()
        self.count_label = QLabel()
        self.count_label.setFont(build_ui_font(self.count_label, point_size=HELPER_UI_FONT_SIZE))
        top_bar.addWidget(self.count_label)
        top_bar.addStretch()

        select_all_btn = QPushButton("全选")
        select_all_btn.setFixedWidth(70)
        select_all_btn.clicked.connect(self._select_all)
        deselect_all_btn = QPushButton("全不选")
        deselect_all_btn.setFixedWidth(70)
        deselect_all_btn.clicked.connect(self._deselect_all)
        remove_selected_btn = QPushButton("删除选中")
        remove_selected_btn.setFixedWidth(80)
        remove_selected_btn.clicked.connect(self._remove_selected)
        top_bar.addWidget(select_all_btn)
        top_bar.addWidget(deselect_all_btn)
        top_bar.addWidget(remove_selected_btn)
        layout.addLayout(top_bar)

        # ── 表格 ──
        self.table = QTableWidget(self)
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["", "显示名称", "备注", "微信号", "类型", "搜索字段值"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Fixed)
        self.table.setColumnWidth(0, 30)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table)

        self._populate_table()

        # ── 底部按钮 ──
        btn_layout = QHBoxLayout()
        self.ok_btn = QPushButton("确认录入")
        self.ok_btn.setMinimumWidth(120)
        self.ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("取消")
        cancel_btn.setMinimumWidth(100)
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addStretch()
        btn_layout.addWidget(self.ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

    def _populate_table(self):
        self.table.setRowCount(len(self._contacts))
        for row_i, contact in enumerate(self._contacts):
            # 勾选框列
            chk = QCheckBox()
            chk.setChecked(True)
            chk_widget = QWidget()
            chk_layout = QHBoxLayout(chk_widget)
            chk_layout.addWidget(chk)
            chk_layout.setAlignment(Qt.AlignCenter)
            chk_layout.setContentsMargins(0, 0, 0, 0)
            self.table.setCellWidget(row_i, 0, chk_widget)

            def _make_item(text):
                item = QTableWidgetItem(text or "")
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                return item

            self.table.setItem(row_i, 1, _make_item(contact.get("显示名称", "")))
            self.table.setItem(row_i, 2, _make_item(contact.get("备注", "")))
            self.table.setItem(row_i, 3, _make_item(contact.get("微信号", "")))
            self.table.setItem(row_i, 4, _make_item(contact.get("类型", "")))
            self.table.setItem(row_i, 5, _make_item(contact.get("_search_key", "")))

        self._update_count()

    def _get_checkbox(self, row: int) -> QCheckBox | None:
        widget = self.table.cellWidget(row, 0)
        if widget:
            return widget.findChild(QCheckBox)
        return None

    def _select_all(self):
        for i in range(self.table.rowCount()):
            chk = self._get_checkbox(i)
            if chk:
                chk.setChecked(True)

    def _deselect_all(self):
        for i in range(self.table.rowCount()):
            chk = self._get_checkbox(i)
            if chk:
                chk.setChecked(False)

    def _remove_selected(self):
        rows_to_remove = []
        for i in range(self.table.rowCount()):
            chk = self._get_checkbox(i)
            if chk and chk.isChecked():
                rows_to_remove.append(i)
        for i in reversed(rows_to_remove):
            self.table.removeRow(i)
            self._contacts.pop(i)
        self._update_count()

    def _update_count(self):
        self.count_label.setText(f"共 {self.table.rowCount()} 位联系人")

    def get_confirmed_contacts(self) -> list[dict]:
        """返回表格中所有勾选的联系人（_contacts 对应行）"""
        result = []
        for i in range(self.table.rowCount()):
            chk = self._get_checkbox(i)
            if chk and chk.isChecked():
                result.append(self._contacts[i])
        return result


class FilterResultDialog(QDialog):
    """筛选结果弹窗：逐条删除后，再导入发送计划。"""

    def __init__(self, contacts: list[dict], parent=None) -> None:
        super().__init__(parent)
        apply_window_font_scaling(self)
        self.setStyleSheet(STANDARD_DIALOG_BUTTON_STYLE)
        self.setWindowTitle("筛选结果")
        self.resize(980, 560)
        self.setMinimumSize(820, 420)
        self.setSizeGripEnabled(True)

        self._contacts = list(contacts)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)

        header_layout = QHBoxLayout()
        self.count_label = QLabel()
        self.count_label.setFont(build_ui_font(self.count_label, point_size=HELPER_UI_FONT_SIZE))
        header_layout.addWidget(self.count_label)
        header_layout.addStretch()
        layout.addLayout(header_layout)

        helper_label = QLabel("请先核对筛选结果；不想发送的对象可直接点行尾“删除”，确认无误后再导入发送计划。")
        style_helper_label(helper_label)
        layout.addWidget(helper_label)

        self.table = QTableWidget(self)
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["显示名称", "备注", "微信号", "类型", "搜索字段值", "操作"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table)

        button_layout = QHBoxLayout()
        self.ok_btn = QPushButton("导入发送计划")
        self.ok_btn.setMinimumWidth(140)
        self.ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("取消")
        cancel_btn.setMinimumWidth(100)
        cancel_btn.clicked.connect(self.reject)
        button_layout.addStretch()
        button_layout.addWidget(self.ok_btn)
        button_layout.addWidget(cancel_btn)
        layout.addLayout(button_layout)

        self._populate_table()

    def _make_item(self, text: str) -> QTableWidgetItem:
        item = QTableWidgetItem(text or "")
        item.setFlags(item.flags() & ~Qt.ItemIsEditable)
        return item

    def _populate_table(self) -> None:
        self.table.setRowCount(len(self._contacts))
        for row_index, contact in enumerate(self._contacts):
            self.table.setItem(row_index, 0, self._make_item(contact.get("显示名称", "")))
            self.table.setItem(row_index, 1, self._make_item(contact.get("备注", "")))
            self.table.setItem(row_index, 2, self._make_item(contact.get("微信号", "")))
            self.table.setItem(row_index, 3, self._make_item(contact.get("类型", "")))
            self.table.setItem(row_index, 4, self._make_item(contact.get("_search_key", "")))

            delete_button = QPushButton("删除")
            delete_button.clicked.connect(lambda _, index=row_index: self._remove_row(index))
            self.table.setCellWidget(row_index, 5, delete_button)

        self.table.resizeRowsToContents()
        self._update_count()

    def _remove_row(self, row_index: int) -> None:
        if row_index < 0 or row_index >= len(self._contacts):
            return
        self._contacts.pop(row_index)
        self._populate_table()

    def _update_count(self) -> None:
        total = len(self._contacts)
        self.count_label.setText(f"当前筛选结果共 {total} 条。")
        self.ok_btn.setEnabled(total > 0)

    def get_remaining_contacts(self) -> list[dict]:
        return list(self._contacts)


class AttachmentManageDialog(QDialog):
    """行附件管理弹窗：支持查看、追加、删除、清空与恢复通用附件。"""

    SUPPORTED_SUFFIXES = {".pdf", ".jpg", ".jpeg", ".png", ".bmp", ".webp"}
    IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}

    def __init__(self, attachments: list[dict] | None = None, *, start_dir: str = "", parent=None) -> None:
        super().__init__(parent)
        apply_window_font_scaling(self)
        self.setStyleSheet(STANDARD_DIALOG_BUTTON_STYLE)
        self.setWindowTitle("管理当前目标附件")
        self.resize(860, 520)
        self.setMinimumSize(720, 420)
        self.setSizeGripEnabled(True)

        self._attachments = [self._normalize_attachment_item(item) for item in (attachments or [])]
        self._start_dir = str(start_dir or "")
        self.use_common_attachments = False

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)

        self.count_label = QLabel()
        self.count_label.setFont(build_ui_font(self.count_label, point_size=HELPER_UI_FONT_SIZE))
        layout.addWidget(self.count_label)

        helper_label = QLabel("这里会先展示当前已选附件。你可以继续追加多个附件、删除选中附件，或恢复为使用通用附件。")
        style_helper_label(helper_label)
        layout.addWidget(helper_label)

        self.table = QTableWidget(self)
        self.table.setColumnCount(2)
        self.table.setHorizontalHeaderLabels(["类型", "路径"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table, stretch=1)

        action_layout = QHBoxLayout()
        self.add_button = QPushButton("添加附件")
        self.add_button.setMinimumWidth(120)
        self.add_button.clicked.connect(self._add_attachments)
        self.remove_button = QPushButton("删除选中附件")
        self.remove_button.setMinimumWidth(140)
        self.remove_button.clicked.connect(self._remove_selected)
        self.clear_button = QPushButton("清空自定义附件")
        self.clear_button.setMinimumWidth(140)
        self.clear_button.clicked.connect(self._clear_attachments)
        self.restore_button = QPushButton("恢复通用附件")
        self.restore_button.setMinimumWidth(140)
        self.restore_button.clicked.connect(self._restore_common_attachments)
        action_layout.addWidget(self.add_button)
        action_layout.addWidget(self.remove_button)
        action_layout.addWidget(self.clear_button)
        action_layout.addWidget(self.restore_button)
        action_layout.addStretch(1)
        layout.addLayout(action_layout)

        button_layout = QHBoxLayout()
        self.ok_button = QPushButton("保存")
        self.ok_button.setMinimumWidth(120)
        self.ok_button.clicked.connect(self.accept)
        cancel_button = QPushButton("取消")
        cancel_button.setMinimumWidth(100)
        cancel_button.clicked.connect(self.reject)
        button_layout.addStretch()
        button_layout.addWidget(self.ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)

        self._refresh_table()

    def _infer_attachment_type(self, file_path: str) -> str:
        suffix = Path(file_path).suffix.lower()
        if suffix == ".pdf":
            return "pdf"
        if suffix in self.IMAGE_SUFFIXES:
            return "image"
        raise ValueError(f"附件类型不合法：{file_path}")

    def _normalize_attachment_item(self, item: str | dict) -> dict[str, str]:
        if isinstance(item, dict):
            file_path = str(item.get("file_path") or item.get("path") or "").strip()
            file_type = str(item.get("file_type") or "").strip().lower()
        else:
            file_path = str(item or "").strip()
            file_type = ""

        if file_path == "":
            raise ValueError("附件路径不能为空。")

        resolved_path = str(Path(file_path).expanduser().resolve(strict=False))
        suffix = Path(resolved_path).suffix.lower()
        if suffix not in self.SUPPORTED_SUFFIXES:
            raise ValueError(f"附件类型不合法：{resolved_path}")
        if not Path(resolved_path).exists():
            raise ValueError(f"附件不存在：{resolved_path}")
        return {
            "file_path": resolved_path,
            "file_type": file_type or self._infer_attachment_type(resolved_path),
        }

    def _refresh_table(self) -> None:
        self.table.setRowCount(len(self._attachments))
        for row_index, item in enumerate(self._attachments):
            self.table.setItem(row_index, 0, QTableWidgetItem(str(item.get("file_type") or "")))
            self.table.setItem(row_index, 1, QTableWidgetItem(str(item.get("file_path") or "")))
        self.table.resizeRowsToContents()
        count = len(self._attachments)
        self.count_label.setText(f"当前已选择 {count} 个自定义附件。")
        has_items = count > 0
        self.remove_button.setEnabled(has_items)
        self.clear_button.setEnabled(has_items)

    def _add_attachments(self) -> None:
        start_dir = self._start_dir
        if self._attachments:
            start_dir = str(Path(self._attachments[-1]["file_path"]).parent)
        paths, _ = QFileDialog.getOpenFileNames(
            self,
            "选择当前目标的自定义附件",
            start_dir,
            "附件文件(*.pdf *.jpg *.jpeg *.png *.bmp *.webp)",
        )
        if not paths:
            return

        existing_paths = {str(item.get("file_path") or "") for item in self._attachments}
        for raw_path in paths:
            item = self._normalize_attachment_item(raw_path)
            if item["file_path"] not in existing_paths:
                self._attachments.append(item)
                existing_paths.add(item["file_path"])
        self._start_dir = str(Path(paths[0]).parent)
        self._refresh_table()

    def _remove_selected(self) -> None:
        rows = sorted({index.row() for index in self.table.selectionModel().selectedRows()}, reverse=True)
        if not rows:
            QMessageBox.information(self, "未选择附件", "请先在附件表中选择要删除的附件。")
            return
        for row in rows:
            if 0 <= row < len(self._attachments):
                self._attachments.pop(row)
        self._refresh_table()

    def _clear_attachments(self) -> None:
        if not self._attachments:
            return
        self._attachments = []
        self._refresh_table()

    def _restore_common_attachments(self) -> None:
        self.use_common_attachments = True
        self.accept()

    def get_attachments(self) -> list[dict[str, str]]:
        return [dict(item) for item in self._attachments]


class MySpinBox(QWidget):
    def __init__(self, desc: str, **kwargs):
        """
        附带标签的SpinBox
        Args:
            desc: 默认的标签
        """
        super().__init__(**kwargs)

        layout = QHBoxLayout()

        # 初始化标签
        self.desc = desc
        self.label = QLabel(desc)
        # self.label.setAlignment(Qt.AlignCenter)

        # 初始化计数器
        self.spin_box = QSpinBox()
        # self.spin_box.valueChanged.connect(self.valuechange)

        layout.addWidget(self.label)
        layout.addWidget(self.spin_box)
        self.setLayout(layout)

    # def valuechange(self):
    #     self.label.setText(f"{self.desc}: {self.spin_box.value()}")
