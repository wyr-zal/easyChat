# -*- coding: utf-8 -*-
"""
群聊管理后端服务 — 自动化执行线程
提供创建群聊、移除成员、退出群聊的 uiautomation 操作。
"""

import time
from ctypes import windll

import pyperclip
import uiautomation as auto

from PyQt5.QtCore import QThread, pyqtSignal

from wechat_locale import WeChatLocale


def _click(element):
    x, y = element.GetPosition()
    auto.Click(x, y)


def _move(element):
    x, y = element.GetPosition()
    auto.SetCursorPos(x, y)


def _click_at(x: int, y: int):
    auto.Click(x, y)


PICKER_WINDOW_CLS = "mmui::SessionPickerWindow"
MEMBER_INFO_CLS = "mmui::ChatRoomMemberInfoView"
CHAT_MORE_ENTRY_AUTOMATION_ID = "chat_more_entry"


def _find_chat_more_menu_item(item_name: str, timeout: float = 2.0):
    """在“快捷操作”弹出菜单容器内定位菜单项。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        menu = auto.ListControl(
            AutomationId=CHAT_MORE_ENTRY_AUTOMATION_ID,
            searchDepth=15,
        )
        if menu.Exists(0, 0):
            item = menu.ListItemControl(Name=item_name, searchDepth=5)
            if item.Exists(0, 0):
                return item

        item = auto.ListItemControl(
            Name=item_name,
            ClassName="mmui::ChatMoreCellView",
            searchDepth=15,
        )
        if item.Exists(0, 0):
            return item

        time.sleep(0.1)

    raise RuntimeError(f"未找到 '{item_name}' 菜单项")


def _iter_descendants(control, max_depth: int = 12, _depth: int = 0):
    if _depth > max_depth:
        return
    try:
        children = control.GetChildren()
    except Exception:
        return
    for child in children:
        yield child
        yield from _iter_descendants(child, max_depth, _depth + 1)


def _find_picker_contact_result(picker, name: str):
    """在发起群聊选人窗口中定位可点击的联系人搜索结果。"""
    candidates = []
    for automation_id in (
        "sp_search_new_chat_result_list",
        "sp_to_select_contact_list",
    ):
        contact_list = picker.ListControl(
            AutomationId=automation_id, searchDepth=20
        )
        if contact_list.Exists(0, 0):
            candidates.extend(contact_list.GetChildren())
            candidates.extend(_iter_descendants(contact_list, max_depth=8))
    candidates.extend(_iter_descendants(picker, max_depth=12))

    fallback_item = None
    clickable_types = {"CheckBoxControl", "ListItemControl"}
    for item in candidates:
        try:
            control_type = item.ControlTypeName or ""
            item_name = item.Name or ""
        except Exception:
            continue
        if control_type not in clickable_types:
            continue
        if item_name and name in item_name:
            return item
        if fallback_item is None and item_name:
            fallback_item = item

    return fallback_item


def _collect_picker_contact_checkboxes(picker):
    """收集选人/移出窗口中的可勾选联系人组件。"""
    candidates = []
    for automation_id in (
        "sp_search_new_chat_result_list",
        "sp_to_select_contact_list",
    ):
        contact_list = picker.ListControl(
            AutomationId=automation_id, searchDepth=20
        )
        if contact_list.Exists(0, 0):
            candidates.extend(contact_list.GetChildren())
            candidates.extend(_iter_descendants(contact_list, max_depth=8))
    candidates.extend(_iter_descendants(picker, max_depth=12))

    result = []
    seen = set()
    for item in candidates:
        marker = id(item)
        if marker in seen:
            continue
        seen.add(marker)
        try:
            control_type = item.ControlTypeName or ""
            item_name = (item.Name or "").strip()
        except Exception:
            continue
        if control_type in {"CheckBoxControl", "ListItemControl"} and item_name:
            result.append(item)
    return result


def _find_descendant_by_name(control, name: str, max_depth: int = 12):
    for item in _iter_descendants(control, max_depth=max_depth):
        try:
            if (item.Name or "") == name:
                return item
        except Exception:
            continue
    return None


def _has_ancestor_class(control, class_name: str) -> bool:
    current = control
    while current is not None:
        try:
            if (current.ClassName or "") == class_name:
                return True
            current = current.GetParentControl()
        except Exception:
            return False
    return False


def _find_main_search_box(win, search_name: str):
    candidates = []
    for item in _iter_descendants(win, max_depth=25):
        try:
            if (item.ControlTypeName or "") != "EditControl":
                continue
            if (item.Name or "") != search_name:
                continue
            if _has_ancestor_class(item, MEMBER_INFO_CLS):
                continue
            rect = item.BoundingRectangle
            candidates.append((rect.left, rect.top, item))
        except Exception:
            continue

    if candidates:
        candidates.sort(key=lambda candidate: (candidate[1], candidate[0]))
        return candidates[0][2]

    return auto.EditControl(Depth=13, Name=search_name)


def _find_group_action_control(control, names: tuple[str, ...], max_depth: int = 12):
    """在群信息侧栏内按组件名称定位操作项，如“移出”“退出群聊”。"""
    normalized_names = tuple(name for name in names if name)
    for item in _iter_descendants(control, max_depth=max_depth):
        try:
            item_name = (item.Name or "").strip()
        except Exception:
            continue
        if item_name in normalized_names:
            return item
    return None


class GroupManagerThread(QThread):
    """群聊批量操作执行线程"""

    progress = pyqtSignal(int, int, str)
    log = pyqtSignal(str)
    completed = pyqtSignal(object)
    error = pyqtSignal(str)

    MODE_CREATE = "create"
    MODE_REMOVE = "remove"
    MODE_EXIT = "exit"

    def __init__(
        self,
        mode: str,
        tasks: list[dict],
        locale: str = "zh-CN",
        interval: float = 2.0,
        parent=None,
    ):
        super().__init__(parent)
        self.mode = mode
        self.tasks = tasks
        self.lc = WeChatLocale(locale)
        self.interval = max(0.5, interval)
        self._stop_requested = False

    def request_stop(self):
        self._stop_requested = True

    def run(self):
        auto_init = auto.UIAutomationInitializerInThread()
        try:
            if self.mode == self.MODE_CREATE:
                summary = self._run_create()
            elif self.mode == self.MODE_REMOVE:
                summary = self._run_remove()
            elif self.mode == self.MODE_EXIT:
                summary = self._run_exit()
            else:
                summary = {"error": f"unknown mode: {self.mode}"}
            self.completed.emit(summary)
        except Exception as e:
            self.error.emit(str(e))
        finally:
            del auto_init

    # ── helpers ──────────────────────────────────────────────

    def _log(self, msg: str):
        self.log.emit(msg)

    def _find_wechat(self):
        win = auto.WindowControl(Depth=1, Name=self.lc.weixin, searchDepth=1)
        if not win.Exists(3, 1):
            raise RuntimeError("未找到微信窗口，请确保微信已打开并登录")
        return win

    def _is_wechat_visible(self) -> bool:
        """窗口存在 且 可见 且 未最小化。"""
        try:
            win = auto.WindowControl(Depth=1, Name=self.lc.weixin, searchDepth=1)
            if not win.Exists(0, 0):
                return False
            hwnd = win.NativeWindowHandle
            user32 = windll.user32
            return bool(user32.IsWindowVisible(hwnd)) and not bool(user32.IsIconic(hwnd))
        except Exception:
            return False

    def _open_wechat(self):
        # 不可见或最小化时，先用全局快捷键唤起（对齐 ui_auto_wechat.open_wechat 行为）
        if not self._is_wechat_visible():
            try:
                auto.SendKeys("{Ctrl}{Alt}w")
            except Exception:
                pass
            time.sleep(0.6)

        win = self._find_wechat()
        # 强制还原 + 置顶 + 聚焦，避免被遮挡或最小化
        try:
            hwnd = win.NativeWindowHandle
            user32 = windll.user32
            if user32.IsIconic(hwnd):
                user32.ShowWindow(hwnd, 9)  # SW_RESTORE
            user32.SetForegroundWindow(hwnd)
        except Exception:
            pass
        win.SetFocus()
        time.sleep(0.3)
        return win

    def _search_contact(self, name: str):
        win = self._open_wechat()
        search_box = _find_main_search_box(win, self.lc.search)
        if not search_box.Exists(2, 1):
            raise RuntimeError("未找到左侧会话搜索框")
        _click(search_box)
        time.sleep(0.2)
        pyperclip.copy(name)
        auto.SendKeys("{Ctrl}v")
        time.sleep(0.5)
        list_control = auto.ListControl(Depth=4)
        for item in list_control.GetChildren():
            if "XTableCell" not in (item.ClassName or ""):
                _click(item)
                break
        time.sleep(0.3)

    def _wait_picker_window(self, name_contains: str, timeout: float = 5.0) -> auto.WindowControl:
        deadline = time.time() + timeout
        while time.time() < deadline:
            selectors = (
                {"Depth": 1, "ClassName": PICKER_WINDOW_CLS, "searchDepth": 1},
                {"ClassName": PICKER_WINDOW_CLS, "searchDepth": 10},
            )
            for kwargs in selectors:
                win = auto.WindowControl(**kwargs)
                if win.Exists(0, 0) and name_contains in (win.Name or ""):
                    return win
            time.sleep(0.3)
        raise RuntimeError(f"等待窗口 '{name_contains}' 超时")

    def _picker_search_and_check(self, picker: auto.WindowControl, name: str):
        search_edit = picker.EditControl(Name=self.lc.search, searchDepth=10)
        if not search_edit.Exists(2, 1):
            raise RuntimeError("在选人窗口中未找到搜索框")
        _click(search_edit)
        time.sleep(0.2)
        auto.SendKeys("{Ctrl}a")
        pyperclip.copy(name)
        auto.SendKeys("{Ctrl}v")
        time.sleep(0.5)

        result_item = _find_picker_contact_result(picker, name)
        if result_item is None:
            raise RuntimeError(f"搜索 '{name}' 后未找到可点击联系人结果")

        _click(result_item)
        time.sleep(0.3)

    def _click_picker_confirm(self, picker: auto.WindowControl, button_name: str):
        btn = picker.ButtonControl(AutomationId="confirm_btn", searchDepth=10)
        if not btn.Exists(2, 1):
            btn = picker.ButtonControl(Name=button_name, searchDepth=10)
        if not btn.Exists(2, 1):
            raise RuntimeError(f"未找到 '{button_name}' 按钮")
        _click(btn)
        time.sleep(0.5)

    # ── create ───────────────────────────────────────────────

    def _run_create(self) -> dict:
        total = len(self.tasks)
        success = 0
        failed = 0

        for idx, task in enumerate(self.tasks):
            if self._stop_requested:
                self._log("用户终止操作")
                break

            group_name = task.get("group_name", "")
            members = task.get("members", [])
            label = group_name or f"第{idx+1}个群"
            self.progress.emit(idx, total, f"正在创建: {label}")
            self._log(f"[{idx+1}/{total}] 创建群聊: {label}  成员: {', '.join(members)}")

            try:
                self._create_one_group(members, group_name)
                success += 1
                task["status"] = "success"
                self._log(f"  -> 创建成功")
            except Exception as e:
                failed += 1
                task["status"] = "failed"
                task["error"] = str(e)
                self._log(f"  -> 创建失败: {e}")

            if idx < total - 1 and not self._stop_requested:
                time.sleep(self.interval)

        self.progress.emit(total, total, "完成")
        return {"total": total, "success": success, "failed": failed, "stopped": self._stop_requested}

    def _create_one_group(self, members: list[str], group_name: str = ""):
        if len(members) < 2:
            raise ValueError("创建群聊至少需要2个成员")

        self._open_wechat()
        time.sleep(0.3)

        plus_btn = auto.ButtonControl(
            Name=self.lc.quick_action,
            ClassName="mmui::XButton",
            searchDepth=20,
        )
        if not plus_btn.Exists(3, 1):
            raise RuntimeError("未找到 '+' (快捷操作) 按钮")
        _click(plus_btn)
        time.sleep(0.5)

        menu_item = _find_chat_more_menu_item(self.lc.initiate_group)
        _click(menu_item)
        time.sleep(0.5)

        picker = self._wait_picker_window("发起群聊")

        selected_count = 0
        skipped_members = []
        for member in members:
            member = member.strip()
            if not member:
                continue
            try:
                self._picker_search_and_check(picker, member)
                selected_count += 1
            except Exception as e:
                skipped_members.append(member)
                self._log(f"  -> 跳过成员 '{member}': {e}")

        if selected_count < 2:
            skipped_text = "、".join(skipped_members) if skipped_members else "无"
            raise RuntimeError(
                f"可创建成员不足2个，已选择 {selected_count} 个，跳过: {skipped_text}"
            )

        self._click_picker_confirm(picker, self.lc.done)
        time.sleep(1)

        group_name = (group_name or "").strip()
        if group_name:
            self._rename_current_group(group_name)
            time.sleep(0.5)

    def _rename_current_group(self, group_name: str):
        group_name = (group_name or "").strip()
        if not group_name:
            return

        self._log(f"  -> 修改群名称: {group_name}")
        self._open_wechat()
        time.sleep(0.5)

        chat_info_btn = auto.ButtonControl(Name=self.lc.chat_info, searchDepth=25)
        if not chat_info_btn.Exists(3, 1):
            raise RuntimeError("未找到 '聊天信息' 按钮，无法修改群名称")
        _click(chat_info_btn)
        time.sleep(0.8)

        panel = auto.Control(ClassName=MEMBER_INFO_CLS, searchDepth=25)
        if not panel.Exists(3, 1):
            raise RuntimeError("未找到群信息侧面板，无法修改群名称")

        name_label = _find_descendant_by_name(panel, "群聊名称", max_depth=14)
        if name_label is not None:
            rect = name_label.BoundingRectangle
            target_x = rect.left + 40
            target_y = rect.bottom + 110
        else:
            rect = panel.BoundingRectangle
            target_x = rect.left + 90
            target_y = rect.top + 460

        _click_at(target_x, target_y)
        time.sleep(0.3)
        auto.SendKeys("{Ctrl}a")
        pyperclip.copy(group_name)
        auto.SendKeys("{Ctrl}v")
        time.sleep(0.2)
        auto.SendKeys("{Enter}")
        time.sleep(0.8)

        modify_btn = auto.ButtonControl(Name="修改", searchDepth=15)
        if not modify_btn.Exists(2, 1):
            raise RuntimeError("未找到 '修改' 确认按钮，群名称可能未提交")
        _click(modify_btn)
        time.sleep(0.5)
        self._log("  -> 群名称修改完成")

    # ── remove members ───────────────────────────────────────

    def _run_remove(self) -> dict:
        total = len(self.tasks)
        success = 0
        failed = 0

        for idx, task in enumerate(self.tasks):
            if self._stop_requested:
                self._log("用户终止操作")
                break

            group_name = task.get("group_name", "")
            members = task.get("members", [])
            self.progress.emit(idx, total, f"正在移除: {group_name}")
            self._log(f"[{idx+1}/{total}] 群聊: {group_name}  移除: {', '.join(members)}")

            try:
                self._remove_members_from_group(group_name, members)
                success += 1
                task["status"] = "success"
                self._log(f"  -> 移除成功")
            except Exception as e:
                failed += 1
                task["status"] = "failed"
                task["error"] = str(e)
                self._log(f"  -> 移除失败: {e}")

            if idx < total - 1 and not self._stop_requested:
                time.sleep(self.interval)

        self.progress.emit(total, total, "完成")
        return {"total": total, "success": success, "failed": failed, "stopped": self._stop_requested}

    def _remove_members_from_group(self, group_name: str, members: list[str]):
        self._search_contact(group_name)
        time.sleep(0.5)

        panel = self._open_group_info_panel()
        self._click_remove_members_entry(panel)
        time.sleep(0.8)

        picker = self._wait_picker_window("移出群成员")

        for member in members:
            member = member.strip()
            if not member:
                continue
            self._picker_search_and_check(picker, member)

        self._click_picker_confirm(picker, self.lc.remove)
        time.sleep(1)

    def _click_remove_members_entry(self, panel):
        remove_btn = _find_group_action_control(
            panel, (self.lc.remove, "移出", "移除", "-"), max_depth=14
        )
        if remove_btn is not None:
            _click(remove_btn)
            return

        member_list = panel.ListControl(AutomationId="chat_member_list", searchDepth=20)
        if not member_list.Exists(2, 1):
            raise RuntimeError("未找到成员列表，无法定位移除成员按钮")

        member_cells = []
        for child in member_list.GetChildren():
            try:
                if "ChatMemberCell" in (child.ClassName or ""):
                    member_cells.append(child)
            except Exception:
                continue
        if not member_cells:
            raise RuntimeError("成员列表为空，无法推断移除成员按钮")

        rects = []
        for cell in member_cells:
            try:
                rect = cell.BoundingRectangle
                rects.append((rect.left, rect.top, rect.right, rect.bottom))
            except Exception:
                continue
        if not rects:
            raise RuntimeError("成员组件缺少位置，无法推断移除成员按钮")

        try:
            list_rect = member_list.BoundingRectangle
            list_right = list_rect.right
        except Exception:
            list_right = max(rect[2] for rect in rects)

        xs = sorted({rect[0] for rect in rects})
        ys = sorted({rect[1] for rect in rects})
        cell_w = rects[0][2] - rects[0][0]
        cell_h = rects[0][3] - rects[0][1]
        step_x = (xs[1] - xs[0]) if len(xs) > 1 else cell_w + 16
        step_y = (ys[1] - ys[0]) if len(ys) > 1 else cell_h
        cols = max(1, int((list_right - xs[0]) // step_x))

        # 微信 4.1 的加号/移出按钮不暴露为独立 UIA 节点，只出现在成员网格尾部：
        # 当前成员后第 1 格是“添加”，第 2 格是“移出”。用 chat_member_list 与
        # ChatMemberCell 的网格位置推断，避免硬编码屏幕绝对坐标。
        remove_index = len(member_cells) + 1
        remove_col = remove_index % cols
        remove_row = remove_index // cols
        target_x = xs[0] + remove_col * step_x + cell_w // 2
        target_y = ys[0] + remove_row * step_y + cell_h // 2
        _click_at(target_x, target_y)

    def _open_group_info_panel(self):
        panel = auto.Control(ClassName=MEMBER_INFO_CLS, searchDepth=25)
        if panel.Exists(0, 0):
            return panel

        chat_info_btn = auto.ButtonControl(Name=self.lc.chat_info, searchDepth=25)
        if not chat_info_btn.Exists(3, 1):
            raise RuntimeError("未找到 '聊天信息' 按钮")
        _click(chat_info_btn)
        time.sleep(0.8)

        panel = auto.Control(ClassName=MEMBER_INFO_CLS, searchDepth=25)
        if not panel.Exists(3, 1):
            raise RuntimeError("未找到群信息侧面板 (ChatRoomMemberInfoView)")
        return panel

    def _remove_all_members_from_group(self, panel):
        total_selected = 0
        max_rounds = 30

        for _round in range(max_rounds):
            if self._stop_requested:
                self._log("用户终止操作")
                return

            self._click_remove_members_entry(panel)
            time.sleep(0.8)

            picker = self._wait_picker_window("移出群成员")
            members = _collect_picker_contact_checkboxes(picker)
            if not members:
                self._close_picker_window(picker)
                self._log(f"  -> 没有可继续移出的成员，已累计选择 {total_selected} 个")
                return

            for member in members:
                _click(member)
                time.sleep(0.2)

            total_selected += len(members)
            self._log(f"  -> 已选择 {len(members)} 个成员，准备移出")
            self._click_picker_confirm(picker, self.lc.remove)
            time.sleep(1)
            panel = self._open_group_info_panel()

        raise RuntimeError("连续移出成员超过 30 轮，疑似成员列表未刷新，已停止")

    def _close_picker_window(self, picker):
        cancel_btn = picker.ButtonControl(Name="取消", searchDepth=10)
        if cancel_btn.Exists(1, 1):
            _click(cancel_btn)
            time.sleep(0.3)
        else:
            auto.SendKeys("{Esc}")
            time.sleep(0.3)

    def _exit_current_group_from_panel(self, panel):
        exit_btn = None
        for _ in range(12):
            exit_btn = _find_group_action_control(
                panel, (self.lc.exit_group, "退出群聊", "Exit Group"), max_depth=14
            )
            if exit_btn is not None:
                break
            _move(panel)
            auto.WheelDown(waitTime=0.1)
            time.sleep(0.2)

        if exit_btn is None:
            raise RuntimeError("未找到 '退出群聊' 按钮")

        _click(exit_btn)
        time.sleep(1.0)

        confirm_btn = auto.ButtonControl(Name="确定", searchDepth=10)
        if confirm_btn.Exists(2, 1):
            _click(confirm_btn)
            time.sleep(0.5)
            self._log("  -> 已点击退出确认")
        else:
            self._log("  -> 未检测到确认弹窗，可能已退出或需要手动确认")

    # ── exit group ───────────────────────────────────────────

    def _run_exit(self) -> dict:
        total = len(self.tasks)
        success = 0
        failed = 0

        for idx, task in enumerate(self.tasks):
            if self._stop_requested:
                self._log("用户终止操作")
                break

            group_name = task.get("group_name", "")
            self.progress.emit(idx, total, f"正在退出: {group_name}")
            self._log(f"[{idx+1}/{total}] 退出群聊: {group_name}")

            try:
                self._exit_one_group(group_name)
                success += 1
                task["status"] = "success"
                self._log(f"  -> 退出成功")
            except Exception as e:
                failed += 1
                task["status"] = "failed"
                task["error"] = str(e)
                self._log(f"  -> 退出失败: {e}")

            if idx < total - 1 and not self._stop_requested:
                time.sleep(self.interval)

        self.progress.emit(total, total, "完成")
        return {"total": total, "success": success, "failed": failed, "stopped": self._stop_requested}

    def _exit_one_group(self, group_name: str):
        self._search_contact(group_name)
        time.sleep(0.5)

        panel = self._open_group_info_panel()
        self._remove_all_members_from_group(panel)
        self._exit_current_group_from_panel(panel)
