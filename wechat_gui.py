import sys
import time
import os
import csv
import random
import datetime
import itertools
import json
from pathlib import Path

from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *
# from ui_auto_wechat import WeChat
from csv_filter_contacts import (
    DEFAULT_FIELDS,
    export_wechat_ids,
    filter_contacts_from_csv,
    filter_contacts_from_multiple_csvs,
    format_preview_rows,
    normalize_fields,
)
from ui_auto_wechat import WeChat
from module import *
from wechat_locale import WeChatLocale

PRIMARY_UI_FONT_SIZE = 11
HELPER_UI_FONT_SIZE = 10


class WechatGUI(QWidget):

    def __init__(self):
        super().__init__()

        # 读取之前保存的配置文件，如果没有则新建一个
        self.config_path = "wechat_config.json"
        if os.path.exists(self.config_path):
            with open(self.config_path, "r", encoding="utf-8") as r:
                self.config = json.load(r)

        else:
            # 默认配置
            self.config = {
                "settings": {
                    "wechat_path": "",
                    "send_interval": 0,
                    "system_version": "new",
                    "language": "zh-CN",
                },
                "contacts": [],
                "messages": [],
                "schedules": [],
            }

        if self.ensure_config_defaults():
            self.save_config()

        self.wechat = WeChat(
            path=self.config["settings"]["wechat_path"],
            locale=self.config["settings"]["language"],
        )
        self.clock = ClockThread()

        # 发消息的用户列表
        self.contacts = []

        # 初始化图形界面
        self.initUI()

        # 判断全局热键是否被按下
        self.hotkey_pressed = False
        keyboard.add_hotkey('ctrl+alt+q', self.hotkey_press)
        
        # 自动打开提示
        self.show_wechat_open_notice()

    def ensure_config_defaults(self):
        changed = False

        settings = self.config.setdefault("settings", {})
        for key, value in {
            "wechat_path": "",
            "send_interval": 0,
            "system_version": "new",
            "language": "zh-CN",
        }.items():
            if key not in settings:
                settings[key] = value
                changed = True

        for key in ("contacts", "messages", "schedules"):
            if key not in self.config:
                self.config[key] = []
                changed = True

        contact_filter = self.config.setdefault("contact_filter", {})
        for key, value in {
            "csv_paths": [],
            "pattern": "",
            "fields": "显示名称,备注,昵称",
            "contact_type": "",
            "ignore_case": False,
            "friend_search_field": "备注",
            "group_search_field": "显示名称",
        }.items():
            if key not in contact_filter:
                contact_filter[key] = value
                changed = True

        # 迁移旧版单 csv_path 字段
        if "csv_path" in contact_filter:
            old_path = contact_filter.pop("csv_path")
            if not contact_filter.get("csv_paths"):
                contact_filter["csv_paths"] = [old_path] if old_path else []
            changed = True

        bulk_send = self.config.setdefault("bulk_send", {})
        for key, value in {
            "random_delay_min": 30,
            "random_delay_max": 180,
            "report_to": "科学-陈老师",
            "operator_name": "",
        }.items():
            if key not in bulk_send:
                bulk_send[key] = value
                changed = True

        return changed

    # 显示微信打开方式变更提示
    def show_wechat_open_notice(self):
        msg_box = QMessageBox(self)
        msg_box.setIcon(QMessageBox.Information)
        msg_box.setWindowTitle("重要提示")
        msg_box.setText("微信打开方式已变更")
        msg_box.setInformativeText(
            "由于微信版本更新，我们现在使用微信内置的快捷键来打开/隐藏微信窗口，请确保你的微信打开快捷键为Ctrl+Alt+w。具体查看方式为“设置”->“快捷键”->“显示/隐藏窗口”\n\n"
            "⚠️ 注意事项：\n"
            "• 如果微信已经打开且在前台，再次按快捷键会导致微信窗口被隐藏\n"
            "• 为避免此问题，建议在使用定时发送功能前，先手动关闭或最小化微信窗口\n"
            "• 这样可以确保程序能够正常打开微信并发送消息\n\n"
        )
        msg_box.setStandardButtons(QMessageBox.Ok)
        msg_box.exec_()

    # 保存当前的配置
    def save_config(self):
        with open(self.config_path, "w", encoding="utf8") as w:
            json.dump(self.config, w, indent=4, ensure_ascii=False)

    def hotkey_press(self):
        print("hotkey pressed")
        self.hotkey_pressed = True

    def apply_font_scaling(self):
        base_font = QFont(self.font())
        base_font.setPointSize(PRIMARY_UI_FONT_SIZE)
        self.setFont(base_font)
        self.setStyleSheet(
            """
            QPushButton {
                font-size: 11pt;
                font-weight: 500;
                min-height: 36px;
                padding: 4px 12px;
            }
            QSpinBox,
            QLineEdit {
                min-height: 34px;
            }
            """
        )

    def build_page_from_layout(self, layout: QLayout) -> QWidget:
        page = QWidget(self)
        page.setLayout(layout)
        return page

    def build_helper_font(self, point_size: int = HELPER_UI_FONT_SIZE) -> QFont:
        helper_font = QFont(self.font())
        helper_font.setPointSize(point_size)
        return helper_font

    def style_helper_label(self, label: QLabel, *, color: str | None = None, point_size: int = HELPER_UI_FONT_SIZE) -> QLabel:
        label.setWordWrap(True)
        label.setFont(self.build_helper_font(point_size))
        if color:
            label.setStyleSheet(f"color:{color};")
        return label

    # 选择用户界面的初始化
    def init_choose_contacts(self):
        # 在联系人有变化后更新配置文件
        def update_contacts():
            contacts = []
            for i in range(self.contacts_view.count()):
                contacts.append(self.contacts_view.item(i).text())

            self.config["contacts"] = contacts
            self.save_config()

        def refresh_contact_numbers():
            for i in range(self.contacts_view.count()):
                item_text = self.contacts_view.item(i).text()
                name = item_text.split(':', 1)[1] if ':' in item_text else item_text
                self.contacts_view.item(i).setText(f"{i+1}:{name}")

        def get_existing_contact_names():
            names = []
            for i in range(self.contacts_view.count()):
                item_text = self.contacts_view.item(i).text()
                names.append(item_text.split(':', 1)[1] if ':' in item_text else item_text)
            return names

        # 读取联系人列表并保存
        def save_contacts():
            # 先弹出一个提示词告诉用户这个提取并不保证可靠，因为微信组织信息的方式本身就有歧义
            QMessageBox.information(self, "注意", "提取联系人列表功能并不保证完全可靠，因为微信组织信息的方式本身就有歧义。"
                                                  "如果想要提取更可靠，请不需要在给用户的备注和设置的分组标签里面加空格。")

            path = QFileDialog.getSaveFileName(self, "保存联系人列表", "contacts.csv", "表格文件(*.csv)")[0]
            if not path == "":
                contacts = self.wechat.find_all_contacts()
                contacts.to_csv(path, index=False, encoding='utf_8_sig')
                # with open(path, 'w', encoding='utf-8') as f:
                #     for contact in contacts:
                #         f.write(contact + '\n')

                QMessageBox.information(self, "保存成功", "联系人列表保存成功！")

        # 保存群聊列表
        def save_groups():
            path = QFileDialog.getSaveFileName(self, "保存群聊列表", "groups.txt", "文本文件(*.txt)")[0]
            if not path == "":
                contacts = self.wechat.find_all_groups()
                with open(path, 'w', encoding='utf-8') as f:
                    for contact in contacts:
                        f.write(contact + '\n')

                QMessageBox.information(self, "保存成功", "群聊列表保存成功！")

        # 读取联系人列表并加载
        def load_contacts():
            path = QFileDialog.getOpenFileName(self, "加载联系人列表", "", "文本文件(*.txt)")[0]
            if not path == "":
                with open(path, 'r', encoding='utf-8') as f:
                    for line in f.readlines():
                        self.contacts_view.addItem(f"{self.contacts_view.count()+1}:{line.strip()}")

                update_contacts()
                QMessageBox.information(self, "加载成功", "联系人列表加载成功！")

        def import_contacts_from_csv():
            filter_config = self.config["contact_filter"]
            dialog = ContactFilterDialog(
                csv_paths=filter_config.get("csv_paths", []),
                pattern=filter_config.get("pattern", ""),
                fields=filter_config.get("fields", "显示名称,备注,昵称"),
                contact_type=filter_config.get("contact_type", ""),
                ignore_case=filter_config.get("ignore_case", False),
                friend_search_field=filter_config.get("friend_search_field", "备注"),
                group_search_field=filter_config.get("group_search_field", "显示名称"),
                parent=self,
            )

            if dialog.exec_() != QDialog.Accepted:
                return

            filter_data = dialog.get_input()
            if not filter_data["csv_paths"]:
                QMessageBox.warning(self, "输入错误", "请选择至少一个 CSV 文件！")
                return

            self.config["contact_filter"] = filter_data
            self.save_config()

            try:
                fields = normalize_fields(filter_data["fields"]) if filter_data["fields"].strip() else ["显示名称", "备注", "昵称"]
                pattern_text = filter_data["pattern"] if filter_data["pattern"].strip() else ".*"
                contact_type = filter_data["contact_type"]
                csv_paths = [Path(p) for p in filter_data["csv_paths"]]

                all_matched: list = []
                # 好友
                if contact_type in ("", "好友"):
                    rows = filter_contacts_from_multiple_csvs(
                        csv_paths=csv_paths,
                        pattern_text=pattern_text,
                        fields=fields,
                        contact_type="好友",
                        ignore_case=filter_data["ignore_case"],
                        search_key_field=filter_data["friend_search_field"],
                    )
                    all_matched.extend(rows)
                # 群聊
                if contact_type in ("", "群聊"):
                    rows = filter_contacts_from_multiple_csvs(
                        csv_paths=csv_paths,
                        pattern_text=pattern_text,
                        fields=fields,
                        contact_type="群聊",
                        ignore_case=filter_data["ignore_case"],
                        search_key_field=filter_data["group_search_field"],
                    )
                    all_matched.extend(rows)

            except Exception as e:
                QMessageBox.warning(self, "筛选失败", f"筛选联系人失败！\n错误信息：{e}")
                return

            if not all_matched:
                QMessageBox.information(self, "筛选结果", "未匹配到任何联系人。")
                return

            # 弹出确认弹窗
            confirm_dialog = ContactConfirmDialog(all_matched, parent=self)
            if confirm_dialog.exec_() != QDialog.Accepted:
                return

            confirmed = confirm_dialog.get_confirmed_contacts()
            if not confirmed:
                QMessageBox.information(self, "无选中", "没有勾选任何联系人。")
                return

            # 选择覆盖/追加
            mode_box = QMessageBox(self)
            mode_box.setWindowTitle("选择导入方式")
            mode_box.setText(f"共确认 {len(confirmed)} 人，请选择导入方式：")
            replace_btn2 = mode_box.addButton("覆盖导入", QMessageBox.AcceptRole)
            append_btn2 = mode_box.addButton("追加导入", QMessageBox.ActionRole)
            mode_box.addButton("取消", QMessageBox.RejectRole)
            mode_box.exec_()
            clicked = mode_box.clickedButton()
            if clicked not in (replace_btn2, append_btn2):
                return

            existing_keys = set(get_existing_contact_names())
            if clicked is replace_btn2:
                self.contacts_view.clear()
                existing_keys.clear()

            added_count = 0
            for row in confirmed:
                search_key = row.get("_search_key", "").strip()
                if not search_key or search_key in existing_keys:
                    continue
                self.contacts_view.addItem(f"{self.contacts_view.count()+1}:{search_key}")
                existing_keys.add(search_key)
                added_count += 1

            refresh_contact_numbers()
            update_contacts()
            QMessageBox.information(self, "导入成功", f"已导入 {added_count} 个联系人。")


        # 增加用户列表信息
        def add_contact():
            name_list, ok = QInputDialog.getText(self, '添加用户', '输入添加的用户名(可添加多个人名，用英文逗号,分隔):')
            if ok:
                if name_list != "":
                    names = name_list.split(',')
                    for name in names:
                        id = f"{self.contacts_view.count() + 1}"
                        self.contacts_view.addItem(f"{id}:{str(name).strip()}")
                    update_contacts()

        # 删除用户信息
        def del_contact():
            # 删除选中的用户
            for i in range(self.contacts_view.count()-1, -1, -1):
                if self.contacts_view.item(i).isSelected():
                    self.contacts_view.takeItem(i)

            # 为所有剩余的用户重新编号
            refresh_contact_numbers()

            update_contacts()

        hbox = QHBoxLayout()

        # 左边的用户列表
        self.contacts_view = MyListWidget()

        # 加载配置文件里保存的用户
        for contact in self.config["contacts"]:
            self.contacts_view.addItem(contact)

        self.clock.contacts = self.contacts_view
        for name in self.contacts:
            self.contacts_view.addItem(name)

        hbox.addWidget(self.contacts_view)

        # 右边的按钮界面
        vbox = QVBoxLayout()
        vbox.stretch(1)

        # 用户界面的按钮
        info = QLabel("待发送用户列表")

        save_btn = QPushButton("保存微信好友列表")
        save_btn.clicked.connect(save_contacts)

        save_group_btn = QPushButton("保存微信群聊列表")
        save_group_btn.clicked.connect(save_groups)

        load_btn = QPushButton("加载用户txt文件")
        load_btn.clicked.connect(load_contacts)

        import_csv_btn = QPushButton("从CSV按正则导入")
        import_csv_btn.clicked.connect(import_contacts_from_csv)

        add_btn = QPushButton("添加用户")
        add_btn.clicked.connect(add_contact)

        del_btn = QPushButton("删除用户")
        del_btn.clicked.connect(del_contact)

        vbox.addWidget(info)
        vbox.addWidget(save_btn)
        vbox.addWidget(save_group_btn)
        vbox.addWidget(load_btn)
        vbox.addWidget(import_csv_btn)
        vbox.addWidget(add_btn)
        vbox.addWidget(del_btn)
        hbox.addLayout(vbox)

        return hbox

    # 定时功能界面的初始化
    def init_clock(self):
        # 在定时列表有变化后更新配置文件
        def update_schedules():
            schedules = []
            for i in range(self.time_view.count()):
                schedules.append(self.time_view.item(i).text())

            self.config["schedules"] = schedules
            self.save_config()
            
        # 按钮响应：增加时间
        def add_contact():
            inputs = [
                "注：在每一个时间输入框内都可以使用英文逗号“,“来一次性区分多个数值进行多次定时。\n(例：分钟框输入 10,20,30,40)",
                "月 (1~12)",
                "日 (1~31)",
                "小时（0~23）",
                "分钟 (0~59)",
                "发送信息的起点（从哪一条开始发）",
                "发送信息的终点（到哪一条结束，包括该条）",
            ]

            # 设置默认值为当前时间
            local_time = time.localtime(time.time())
            default_values = [
                str(local_time.tm_year),
                str(local_time.tm_mon),
                str(local_time.tm_mday),
                str(local_time.tm_hour),
                str(local_time.tm_min),
                "",
                "",
            ]

            dialog = MultiInputDialog(inputs, default_values)
            if dialog.exec_() == QDialog.Accepted:
                year, month, day, hour, min, st, ed = dialog.get_input()
                if year == "" or month == "" or day == "" or hour == "" or min == "" or st == "" or ed == "":
                    QMessageBox.warning(self, "输入错误", "输入不能为空！")
                    return

                else:
                    year_list = [y.strip() for y in year.split(',')]
                    month_list = [m.strip() for m in month.split(',')]
                    day_list = [d.strip() for d in day.split(',')]
                    hour_list = [h.strip() for h in hour.split(',')]
                    min_list = [m.strip() for m in min.split(',')]

                    for year, month, day, hour, min in itertools.product(year_list, month_list, day_list, hour_list, min_list):
                        input = f"{year} {month} {day} {hour} {min} {st}-{ed}"
                        self.time_view.addItem(input)
                    
                    update_schedules()

        # 按钮响应：删除时间
        def del_contact():
            for i in range(self.time_view.count() - 1, -1, -1):
                if self.time_view.item(i).isSelected():
                    self.time_view.takeItem(i)
            
            update_schedules()

        # 按钮响应：开始定时
        def start_counting():
            if self.clock.time_counting is True:
                return
            else:
                self.clock.time_counting = True

            info.setStyleSheet("color:red")
            info.setText("定时发送（目前已开始）")
            self.clock.start()

        # 按钮响应：结束定时
        def end_counting():
            self.clock.time_counting = False
            info.setStyleSheet("color:black")
            info.setText("定时发送（目前未开始）")

        # 按钮相应：开启防止自动下线。开启后每隔一小时自动点击微信窗口，防止自动下线
        def prevent_offline():
            if self.clock.prevent_offline is True:
                self.clock.prevent_offline = False
                prevent_btn.setStyleSheet("color:black")
                prevent_btn.setText("防止自动下线：（目前关闭）")

            else:
                # 弹出提示框
                QMessageBox.information(self, "防止自动下线", "防止自动下线已开启！每隔一小时自动点击微信窗口，防"
                                                              "止自动下线。请不要在正常使用电脑时使用该策略。")

                self.clock.prevent_offline = True
                prevent_btn.setStyleSheet("color:red")
                prevent_btn.setText("防止自动下线：（目前开启）")

        hbox = QHBoxLayout()

        # 左边的时间列表
        self.time_view = MyListWidget()
        # 加载配置文件里保存的用户
        for schedule in self.config["schedules"]:
            self.time_view.addItem(schedule)
            
        self.clock.clocks = self.time_view
        hbox.addWidget(self.time_view)

        # 右边的按钮界面
        vbox = QVBoxLayout()
        vbox.stretch(1)

        info = QLabel("定时发送（目前未开始）")
        self.style_helper_label(info, color="#555")
        add_btn = QPushButton("添加时间")
        add_btn.clicked.connect(add_contact)
        del_btn = QPushButton("删除时间")
        del_btn.clicked.connect(del_contact)
        start_btn = QPushButton("开始定时")
        start_btn.clicked.connect(start_counting)
        end_btn = QPushButton("结束定时")
        end_btn.clicked.connect(end_counting)
        prevent_btn = QPushButton("防止自动下线：（目前关闭）")
        prevent_btn.clicked.connect(prevent_offline)

        vbox.addWidget(info)
        vbox.addWidget(add_btn)
        vbox.addWidget(del_btn)
        vbox.addWidget(start_btn)
        vbox.addWidget(end_btn)
        vbox.addWidget(prevent_btn)
        hbox.addLayout(vbox)

        return hbox

    # 发送消息内容界面的初始化
    def init_send_msg(self):
        # 在发送消息有变化后更新配置文件
        def update_messages():
            messages = []
            for i in range(self.msg.count()):
                messages.append(self.msg.item(i).text())

            self.config["messages"] = messages
            self.save_config()

        # 从txt中加载消息内容
        def load_text():
            path = QFileDialog.getOpenFileName(self, "加载内容文本", "", "文本文件(*.txt)")[0]
            if not path == "":
                with open(path, 'r', encoding='utf-8') as f:
                    for line in f.readlines():
                        self.msg.addItem(f"{self.msg.count()+1}:text:{line.strip()}")

                QMessageBox.information(self, "加载成功", "内容文本加载成功！")

        # 增加一条文本信息
        def add_text():
            inputs = [
                "是否需要at他人(无则不填，有则填写所有你要at的人名，用英文逗号分隔。要at所有人就填写'所有人')",
                "请输入发送的文本内容(如果需要换行则输入\\n，例如你好\\n吃饭了吗？)",
                "请指定发送给哪些用户(1,2,3代表发送给前三位用户)，如需全部发送请忽略此项",
            ]
            dialog = MultiInputDialog(inputs)
            if dialog.exec_() == QDialog.Accepted:
                at, text, to = dialog.get_input()
                to = "all" if to == "" else to
                if text != "":
                    # 消息的序号
                    rank = self.msg.count() + 1

                    self.msg.addItem(f"{rank}:text:{to}:{at}:{str(text)}")
                    update_messages()

        # 增加一个文件
        def add_file():
            dialog = FileDialog()
            if dialog.exec_() == QDialog.Accepted:
                to, paths = dialog.get_input()
                to = "all" if to == "" else to
                if paths != "":
                    # 将多个文件路径按分号分隔
                    path_list = paths.split(";")
                    # 循环添加每个文件
                    for path in path_list:
                        path = path.strip()
                        if path != "":
                            self.msg.addItem(f"{self.msg.count()+1}:file:{to}:{str(path)}")
                    update_messages()

        # 删除一条发送的信息
        def del_content():
            # 删除选中的信息
            for i in range(self.msg.count() - 1, -1, -1):
                if self.msg.item(i).isSelected():
                    self.msg.takeItem(i)

            # 为所有剩余的信息重新设置编号
            for i in range(self.msg.count()):
                self.msg.item(i).setText(f"{i+1}:"+self.msg.item(i).text().split(':', 1)[1])

            update_messages()

        # 发送按钮响应事件
        def send_msg(gap=None, st=None, ed=None):
            """发送消息。支持随机延迟、日志记录、完成后向陈老师汇报。"""
            self.hotkey_pressed = False
            interval = send_interval.spin_box.value()
            bulk_cfg = self.config.get("bulk_send", {})
            delay_min = int(bulk_cfg.get("random_delay_min", 30))
            delay_max = int(bulk_cfg.get("random_delay_max", 180))
            report_to = bulk_cfg.get("report_to", "科学-陈老师")
            operator_name = bulk_cfg.get("operator_name", "")

            success_count = 0
            fail_count = 0
            start_time = datetime.datetime.now()
            log_path = Path("send_log.csv")
            log_exists = log_path.exists()

            # 获取消息内容摘要（前50字）
            msg_preview = ""
            for mi in range(self.msg.count()):
                raw = self.msg.item(mi).text()
                parts = raw.split(":", 3)
                if len(parts) >= 3 and parts[1] == "text":
                    content_part = parts[3] if len(parts) > 3 else ""
                    # content_part 格式为 at:text
                    text_part = content_part.split(":", 1)[-1]
                    msg_preview += text_part[:50]
                    break

            try:
                if st is None:
                    st = 1
                    ed = self.msg.count()

                total = self.contacts_view.count()

                with open(log_path, "a", encoding="utf-8-sig", newline="") as log_f:
                    writer = csv.writer(log_f)
                    if not log_exists:
                        writer.writerow(["时间", "收件人", "搜索字段", "消息摘要", "结果"])

                    for user_i in range(total):
                        # 固定间隔（原有逻辑）
                        if user_i > 0:
                            time.sleep(int(interval))

                        item_text = self.contacts_view.item(user_i).text()
                        rank, name = item_text.split(":", 1)
                        search_user = True

                        try:
                            for msg_i in range(st - 1, ed):
                                if self.hotkey_pressed:
                                    QMessageBox.warning(self, "发送中止", "热键已按下，已停止发送！")
                                    return

                                msg = self.msg.item(msg_i).text().replace("\\n", "\n")
                                _, mtype, to, content = msg.split(":", 3)

                                if to == "all" or str(rank) in to.split(","):
                                    if mtype == "text":
                                        at_names, text = content.split(":", 1)
                                        at_names = at_names.split(",")
                                        self.wechat.send_msg(name, at_names, text, search_user)
                                    elif mtype == "file":
                                        self.wechat.send_file(name, content, search_user)
                                    search_user = False

                            success_count += 1
                            writer.writerow([
                                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                name, name, msg_preview[:50], "成功"
                            ])

                        except Exception as e_user:
                            fail_count += 1
                            writer.writerow([
                                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                name, name, msg_preview[:50], f"失败: {e_user}"
                            ])

                        # 随机延迟（非最后一人）
                        if user_i < total - 1 and delay_max > 0:
                            delay = random.randint(
                                min(delay_min, delay_max),
                                max(delay_min, delay_max)
                            )
                            time.sleep(delay)

                # 发送完成后向陈老师汇报
                end_time = datetime.datetime.now()
                elapsed = str(end_time - start_time).split(".")[0]
                op_name = operator_name or "未知"
                time_fmt = start_time.strftime("%Y-%m-%d %H:%M")
                end_fmt = end_time.strftime("%H:%M")
                report_lines = [
                    f"【群发汇报】",
                    f"操作人：{op_name}",
                    f"发送总人数：{total}",
                    f"成功：{success_count} / 失败：{fail_count}",
                    f"内容摘要：{msg_preview[:30]}...",
                    f"执行时间：{time_fmt} ~ {end_fmt}（历时{elapsed}）",
                ]
                report_text = "\n".join(report_lines)
                try:
                    self.wechat.send_msg(report_to, [""], report_text, True)
                except Exception as e_report:
                    print(f"汇报失败: {e_report}")

            except Exception as e:
                QMessageBox.warning(self, "发送失败", f"发送失败！请检查内容格式或是否有遗漏步骤！\n错误信息：{e}")
                return


        # 左边的布局
        vbox_left = QVBoxLayout()

        # 提示信息
        info = QLabel("添加要发送的内容")
        helper_label = QLabel("程序会按消息列表中的顺序依次发送。")
        self.style_helper_label(helper_label, color="#555")

        # 输入内容框
        self.msg = MyListWidget()
        # 加载配置文件里保存的内容
        for message in self.config["messages"]:
            self.msg.addItem(message)

        self.clock.send_func = send_msg
        self.clock.prevent_func = self.wechat.prevent_offline

        # 发送按钮
        send_btn = QPushButton("发送")
        send_btn.clicked.connect(send_msg)

        # 发送不同用户时的间隔
        send_interval = MySpinBox("发送不同用户时的间隔（秒）")
        send_interval.spin_box.setValue(self.config["settings"]["send_interval"])

        # 添加修改间隔的响应
        def change_spin_box():
            interval = send_interval.spin_box.value()
            self.config["settings"]["send_interval"] = interval
            self.save_config()

        send_interval.spin_box.valueChanged.connect(change_spin_box)

        # 随机延迟配置（群发时在每位用户之间随机等待）
        bulk_cfg = self.config.get("bulk_send", {})
        delay_row = QHBoxLayout()
        delay_row.addWidget(QLabel("群发随机延迟（秒）：最小"))
        self.delay_min_spin = QSpinBox()
        self.delay_min_spin.setRange(0, 3600)
        self.delay_min_spin.setValue(bulk_cfg.get("random_delay_min", 30))
        delay_row.addWidget(self.delay_min_spin)
        delay_row.addWidget(QLabel("最大"))
        self.delay_max_spin = QSpinBox()
        self.delay_max_spin.setRange(0, 3600)
        self.delay_max_spin.setValue(bulk_cfg.get("random_delay_max", 180))
        delay_row.addWidget(self.delay_max_spin)
        delay_row.addStretch()

        operator_row = QHBoxLayout()
        operator_row.addWidget(QLabel("操作人名称（用于汇报）："))
        self.operator_input = QLineEdit()
        self.operator_input.setText(bulk_cfg.get("operator_name", ""))
        self.operator_input.setPlaceholderText("填写你的名字")
        operator_row.addWidget(self.operator_input)

        def save_bulk_config():
            self.config.setdefault("bulk_send", {})
            self.config["bulk_send"]["random_delay_min"] = self.delay_min_spin.value()
            self.config["bulk_send"]["random_delay_max"] = self.delay_max_spin.value()
            self.config["bulk_send"]["operator_name"] = self.operator_input.text().strip()
            self.save_config()

        self.delay_min_spin.valueChanged.connect(save_bulk_config)
        self.delay_max_spin.valueChanged.connect(save_bulk_config)
        self.operator_input.editingFinished.connect(save_bulk_config)


        vbox_left.addWidget(info)
        vbox_left.addWidget(helper_label)
        vbox_left.addWidget(self.msg)
        vbox_left.addWidget(send_interval)
        vbox_left.addLayout(delay_row)
        vbox_left.addLayout(operator_row)
        vbox_left.addWidget(send_btn)

        # 右边的选择内容界面
        vbox_right = QVBoxLayout()
        vbox_right.stretch(1)


        load_btn = QPushButton("加载内容txt文件")
        load_btn.clicked.connect(load_text)

        text_btn = QPushButton("添加文本内容")
        text_btn.clicked.connect(add_text)

        file_btn = QPushButton("添加文件")
        file_btn.clicked.connect(add_file)

        del_btn = QPushButton("删除内容")
        del_btn.clicked.connect(del_content)

        vbox_right.addWidget(text_btn)
        vbox_right.addWidget(file_btn)
        vbox_right.addWidget(del_btn)
        vbox_right.addWidget(load_btn)

        # 整体布局
        hbox = QHBoxLayout()
        hbox.addLayout(vbox_left)
        hbox.addLayout(vbox_right)

        return hbox


    def initUI(self):
        # 垂直布局
        vbox = QVBoxLayout()
        self.apply_font_scaling()

        # 关于自动打开微信界面的按钮
        self.wechat_notice_btn = QPushButton("关于自动打开微信界面", self)
        self.wechat_notice_btn.resize(self.wechat_notice_btn.sizeHint())
        self.wechat_notice_btn.clicked.connect(self.show_wechat_open_notice)


        # 用户选择界面
        contacts_page = self.build_page_from_layout(self.init_choose_contacts())

        # 发送内容界面
        msg_page = self.build_page_from_layout(self.init_send_msg())

        # 定时界面
        clock_page = self.build_page_from_layout(self.init_clock())

        self.main_tabs = QTabWidget(self)
        self.main_tabs.setDocumentMode(True)
        self.main_tabs.addTab(contacts_page, "联系人管理")
        self.main_tabs.addTab(msg_page, "发送内容")
        self.main_tabs.addTab(clock_page, "定时任务")

        vbox.addWidget(self.wechat_notice_btn)
        vbox.addWidget(self.main_tabs, stretch=1)

        self.setLayout(vbox)
        self.resize(1100, 760)
        self.setMinimumSize(760, 560)
        self.setWindowTitle('EasyChat微信助手(作者：LTEnjoy)')
        self.show()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = WechatGUI()
    sys.exit(app.exec_())
