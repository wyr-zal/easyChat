# -*- coding: utf-8 -*-
"""
备注批量修改后端服务。

通过微信左侧搜索框定位联系人，再进入聊天信息侧栏和联系人资料卡，
批量把 Excel / CSV 中的“原始名”修改为“新备注”。
"""

from __future__ import annotations

import time
from ctypes import windll
from enum import Enum
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyperclip
import uiautomation as auto
from PyQt5.QtCore import QThread, pyqtSignal

from wechat_locale import WeChatLocale


MEMBER_INFO_CLS = "mmui::ChatRoomMemberInfoView"
SINGLE_CHAT_INFO_AUTOMATION_ID = "single_chat_info_view"
SINGLE_CHAT_MEMBER_AUTOMATION_ID = "single_chat_member_cell"
PERSONAL_INFO_LANDMARKS = (
    "查找聊天内容",
    "消息免打扰",
    "置顶聊天",
    "Search Chat History",
    "Mute Notifications",
    "Sticky on Top",
)
CONTACT_PROFILE_CLASS_CANDIDATES = (
    "mmui::ContactProfileView",
    "mmui::ContactProfileWnd",
    "mmui::ProfileCardView",
    "mmui::ContactCardView",
    "mmui::ProfileDialog",
)


class ChatInfoPanelState(Enum):
    OPEN = "open"
    CLOSED = "closed"


def _click(element):
    x, y = element.GetPosition()
    auto.Click(x, y)


def _click_at(x: int, y: int):
    auto.Click(x, y)


def _iter_descendants(control, max_depth: int = 12, _depth: int = 0):
    if control is None or _depth > max_depth:
        return
    try:
        children = control.GetChildren()
    except Exception:
        return
    for child in children:
        yield child
        yield from _iter_descendants(child, max_depth, _depth + 1)


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


def _find_descendant_by_name(control, name: str, max_depth: int = 12):
    for item in _iter_descendants(control, max_depth=max_depth):
        try:
            if (item.Name or "").strip() == name:
                return item
        except Exception:
            continue
    return None


def _control_exists(control) -> bool:
    try:
        return bool(control is not None and control.Exists(0, 0))
    except Exception:
        return False


def _count_descendant_names(control, names: tuple[str, ...], max_depth: int = 8) -> int:
    count = 0
    wanted = {name for name in names if name}
    for item in _iter_descendants(control, max_depth=max_depth):
        try:
            item_name = (item.Name or "").strip()
        except Exception:
            continue
        if item_name in wanted:
            count += 1
    return count


def _find_personal_info_panel_by_landmarks(win):
    """按单聊信息侧栏的稳定文案定位侧栏。

    单聊侧栏与群聊侧栏的 ClassName 可能不同；但当前界面通常包含
    “查找聊天内容 / 消息免打扰 / 置顶聊天”等文案。用这些 landmark
    找到右侧最像侧栏的容器，避免把单聊也写死成群聊组件。
    """
    candidates = []
    for index, item in enumerate([win, *_iter_descendants(win, max_depth=14)]):
        try:
            rect = item.BoundingRectangle
            width = max(1, rect.right - rect.left)
            height = max(1, rect.bottom - rect.top)
            area = width * height
            score = _count_descendant_names(item, PERSONAL_INFO_LANDMARKS, max_depth=8)
        except Exception:
            continue
        if score >= 2:
            candidates.append((score, rect.left, area, index, item))

    if not candidates:
        return None
    candidates.sort(key=lambda candidate: (-candidate[1], candidate[2], -candidate[0], candidate[3]))
    return candidates[0][4]


def _find_chat_info_panel(win):
    """兼容群聊侧栏和个人联系人侧栏。"""
    try:
        panel = win.Control(AutomationId=SINGLE_CHAT_INFO_AUTOMATION_ID, searchDepth=25)
        if _control_exists(panel):
            return panel
    except Exception:
        pass

    panel = auto.Control(AutomationId=SINGLE_CHAT_INFO_AUTOMATION_ID, searchDepth=25)
    if _control_exists(panel):
        return panel

    try:
        panel = win.Control(ClassName=MEMBER_INFO_CLS, searchDepth=25)
        if _control_exists(panel):
            return panel
    except Exception:
        pass

    panel = auto.Control(ClassName=MEMBER_INFO_CLS, searchDepth=25)
    if _control_exists(panel):
        return panel

    return _find_personal_info_panel_by_landmarks(win)


def _find_chat_info_button(win, button_name: str):
    try:
        btn = win.ButtonControl(Name=button_name, searchDepth=25)
        if _control_exists(btn):
            return btn
    except Exception:
        pass

    btn = auto.ButtonControl(Name=button_name, searchDepth=25)
    if _control_exists(btn):
        return btn
    return None


def _get_chat_info_panel_state(win) -> tuple[ChatInfoPanelState, object | None]:
    panel = _find_chat_info_panel(win)
    if panel is not None:
        return ChatInfoPanelState.OPEN, panel
    return ChatInfoPanelState.CLOSED, None


def _control_center(control) -> tuple[int, int]:
    rect = control.BoundingRectangle
    return ((rect.left + rect.right) // 2, (rect.top + rect.bottom) // 2)


def _rect_vertical_overlap(a, b) -> int:
    return max(0, min(a.bottom, b.bottom) - max(a.top, b.top))


def _get_remark_edit_click_point(profile, remark_label: str = "备注") -> tuple[int, int]:
    """根据资料卡中的“备注”标签计算可编辑值区域的点击点。"""
    label = _find_descendant_by_name(profile, remark_label, max_depth=16)
    if label is None:
        try:
            rect = profile.BoundingRectangle
            return (rect.left + 250, rect.top + 300)
        except Exception as exc:
            raise RuntimeError("未找到备注组件") from exc

    label_rect = label.BoundingRectangle
    same_row_values = []
    for item in _iter_descendants(profile, max_depth=16):
        if item is label:
            continue
        try:
            item_name = (item.Name or "").strip()
            control_type = item.ControlTypeName or ""
            rect = item.BoundingRectangle
        except Exception:
            continue
        if not item_name or item_name == remark_label:
            continue
        if control_type not in {"TextControl", "EditControl", "ButtonControl"}:
            continue
        if rect.left <= label_rect.right:
            continue
        if _rect_vertical_overlap(label_rect, rect) <= 0:
            continue
        same_row_values.append((rect.left, rect, item))

    if same_row_values:
        same_row_values.sort(key=lambda candidate: candidate[0])
        return _control_center(same_row_values[0][2])

    return (label_rect.right + 120, (label_rect.top + label_rect.bottom) // 2)


def _first_non_blank(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def build_remark_tasks(
    df: pd.DataFrame,
    original_column: str = "",
    remark_column: str = "",
) -> list[dict]:
    """从 DataFrame 构建备注修改任务。默认使用前两列。"""
    if len(df.columns) < 2:
        raise ValueError("Excel 至少需要两列：原始名、新备注")

    columns = [str(col) for col in df.columns]
    original_column = original_column.strip()
    remark_column = remark_column.strip()

    if original_column:
        if original_column not in columns:
            raise ValueError(f"Excel 中未找到列 '{original_column}'")
        source_col = original_column
    else:
        source_col = columns[0]

    if remark_column:
        if remark_column not in columns:
            raise ValueError(f"Excel 中未找到列 '{remark_column}'")
        target_col = remark_column
    else:
        target_col = columns[1]

    tasks = []
    for _, row in df.iterrows():
        original_name = _first_non_blank(row.get(source_col, ""))
        new_remark = _first_non_blank(row.get(target_col, ""))
        if not original_name or not new_remark:
            continue
        tasks.append(
            {
                "original_name": original_name,
                "new_remark": new_remark,
                "status": "",
            }
        )
    return tasks


def read_remark_tasks(
    path: str,
    original_column: str = "",
    remark_column: str = "",
) -> list[dict]:
    file_path = Path(path.strip())
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")
    if file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path, dtype=str).fillna("")
    else:
        df = pd.read_excel(file_path, dtype=str).fillna("")
    return build_remark_tasks(df, original_column, remark_column)


class RemarkManagerThread(QThread):
    """备注批量修改执行线程。"""

    progress = pyqtSignal(int, int, str)
    log = pyqtSignal(str)
    completed = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(
        self,
        tasks: list[dict],
        locale: str = "zh-CN",
        interval: float = 2.0,
        parent=None,
    ):
        super().__init__(parent)
        self.tasks = tasks
        self.lc = WeChatLocale(locale)
        self.interval = max(0.5, float(interval))
        self._stop_requested = False
        self.remark_label = {
            "zh-CN": "备注",
            "zh-TW": "備註",
            "en-US": "Remark",
        }.get(locale, "备注")

    def request_stop(self):
        self._stop_requested = True

    def run(self):
        auto_init = auto.UIAutomationInitializerInThread()
        try:
            summary = self._run_update()
            self.completed.emit(summary)
        except Exception as exc:
            self.error.emit(str(exc))
        finally:
            del auto_init

    def _log(self, msg: str):
        self.log.emit(msg)

    def _find_wechat(self):
        win = auto.WindowControl(Depth=1, Name=self.lc.weixin, searchDepth=1)
        if not win.Exists(3, 1):
            raise RuntimeError("未找到微信窗口，请确保微信已打开并登录")
        return win

    def _is_wechat_visible(self) -> bool:
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
        if not self._is_wechat_visible():
            try:
                auto.SendKeys("{Ctrl}{Alt}w")
            except Exception:
                pass
            time.sleep(0.6)

        win = self._find_wechat()
        try:
            hwnd = win.NativeWindowHandle
            user32 = windll.user32
            if user32.IsIconic(hwnd):
                user32.ShowWindow(hwnd, 9)
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
        auto.SendKeys("{Ctrl}a")
        pyperclip.copy(name)
        auto.SendKeys("{Ctrl}v")
        time.sleep(0.5)

        list_control = auto.ListControl(Depth=4)
        clicked = False
        for item in list_control.GetChildren():
            try:
                class_name = item.ClassName or ""
            except Exception:
                class_name = ""
            if "XTableCell" not in class_name:
                _click(item)
                clicked = True
                break
        if not clicked:
            raise RuntimeError(f"搜索 '{name}' 后未找到可点击结果")
        time.sleep(0.4)

    def _open_chat_info_panel(self):
        win = self._open_wechat()
        state, panel = _get_chat_info_panel_state(win)
        if state is ChatInfoPanelState.OPEN:
            self._log("  -> 聊天信息侧栏已打开，进入下一步")
            return panel

        self._log("  -> 聊天信息侧栏未打开，准备点击聊天信息/三个点")
        chat_info_btn = _find_chat_info_button(win, self.lc.chat_info)
        if chat_info_btn is None:
            raise RuntimeError("未找到 '聊天信息' / 三个点按钮")
        self._log("  -> 点击聊天信息/三个点")
        _click(chat_info_btn)

        for _ in range(10):
            time.sleep(0.2)
            # 点击三点后重新获取窗口控件树，避免复用点击前的缓存状态。
            win = self._open_wechat()
            state, panel = _get_chat_info_panel_state(win)
            if state is ChatInfoPanelState.OPEN:
                self._log("  -> 已打开聊天信息侧栏")
                return panel
        raise RuntimeError("未找到聊天信息侧栏")

    def _open_contact_profile_from_panel(self, panel):
        entry = self._find_profile_entry_in_panel(panel)
        if entry is None:
            self._log("  -> 未识别到联系人头像组件，已停止本条任务")
            raise RuntimeError("未识别到联系人头像组件")

        self._log("  -> 准备点击联系人头像")
        _click(entry)
        time.sleep(0.8)
        self._log("  -> 已点击联系人头像，等待资料卡")
        return self._wait_contact_profile()

    def _find_profile_entry_in_panel(self, panel):
        try:
            member_cell = panel.ButtonControl(
                AutomationId=SINGLE_CHAT_MEMBER_AUTOMATION_ID, searchDepth=12
            )
            if _control_exists(member_cell):
                return member_cell
        except Exception:
            pass

        try:
            member_cell = panel.Control(
                AutomationId=SINGLE_CHAT_MEMBER_AUTOMATION_ID, searchDepth=12
            )
            if _control_exists(member_cell):
                return member_cell
        except Exception:
            pass

        member_list = panel.ListControl(AutomationId="chat_member_list", searchDepth=20)
        candidates: Iterable = []
        if member_list.Exists(0, 0):
            try:
                candidates = member_list.GetChildren()
            except Exception:
                candidates = []
        try:
            panel_rect = panel.BoundingRectangle
            top_limit = panel_rect.top + 220
        except Exception:
            top_limit = None

        top_entries = []
        add_names = {"添加", "Add", "+", "新增"}
        for item in list(candidates) + list(_iter_descendants(panel, max_depth=10)):
            try:
                class_name = item.ClassName or ""
                control_type = item.ControlTypeName or ""
                rect = item.BoundingRectangle
                item_name = (item.Name or "").strip()
            except Exception:
                continue
            if "ChatMemberCell" in class_name:
                top_entries.append((100, rect.left, rect.top, item))
                continue
            if control_type in {"ButtonControl", "ImageControl", "ListItemControl"}:
                if top_limit is not None and rect.top >= top_limit:
                    continue
                score = {
                    "ImageControl": 90,
                    "ListItemControl": 80,
                    "ButtonControl": 60,
                }.get(control_type, 0)
                if item_name in add_names:
                    score -= 100
                top_entries.append((score, rect.left, rect.top, item))

        if top_entries:
            top_entries.sort(key=lambda entry: (-entry[0], entry[1], entry[2]))
            best = top_entries[0]
            if best[0] > 0:
                return best[3]
        return None

    def _wait_contact_profile(self, timeout: float = 4.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for class_name in CONTACT_PROFILE_CLASS_CANDIDATES:
                profile = auto.Control(ClassName=class_name, searchDepth=30)
                if profile.Exists(0, 0):
                    return profile

            remark_label = auto.TextControl(Name=self.remark_label, searchDepth=30)
            if remark_label.Exists(0, 0):
                profile = self._profile_parent_from_remark_label(remark_label)
                if profile is not None:
                    return profile
            time.sleep(0.2)
        raise RuntimeError("未打开联系人资料卡，无法修改备注")

    def _profile_parent_from_remark_label(self, remark_label):
        current = remark_label
        for _ in range(6):
            try:
                parent = current.GetParentControl()
            except Exception:
                return current
            if parent is None:
                return current
            current = parent
        return current

    def _set_profile_remark(self, profile, new_remark: str):
        target_x, target_y = _get_remark_edit_click_point(profile, self.remark_label)
        _click_at(target_x, target_y)
        time.sleep(0.3)
        auto.SendKeys("{Ctrl}a")
        pyperclip.copy(new_remark)
        auto.SendKeys("{Ctrl}v")
        time.sleep(0.2)
        auto.SendKeys("{Enter}")
        time.sleep(0.6)

    def _update_one_remark(self, original_name: str, new_remark: str):
        self._search_contact(original_name)
        time.sleep(0.4)
        panel = self._open_chat_info_panel()
        profile = self._open_contact_profile_from_panel(panel)
        self._set_profile_remark(profile, new_remark)

    def _run_update(self) -> dict:
        total = len(self.tasks)
        success = 0
        failed = 0

        for idx, task in enumerate(self.tasks):
            if self._stop_requested:
                self._log("用户终止操作")
                break

            original_name = task.get("original_name", "")
            new_remark = task.get("new_remark", "")
            self.progress.emit(idx, total, f"正在修改: {original_name}")
            self._log(f"[{idx+1}/{total}] {original_name} -> {new_remark}")

            try:
                self._update_one_remark(original_name, new_remark)
                success += 1
                task["status"] = "success"
                self._log("  -> 修改成功")
            except Exception as exc:
                failed += 1
                task["status"] = "failed"
                task["error"] = str(exc)
                self._log(f"  -> 修改失败: {exc}")

            if idx < total - 1 and not self._stop_requested:
                time.sleep(self.interval)

        self.progress.emit(total, total, "完成")
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "stopped": self._stop_requested,
        }
