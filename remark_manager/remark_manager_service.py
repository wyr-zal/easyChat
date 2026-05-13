# -*- coding: utf-8 -*-
"""
备注批量修改后端服务。

通过微信左侧搜索框定位联系人，再进入聊天信息侧栏和联系人资料卡，
批量把 Excel / CSV 中的“原始名”修改为“新备注”。
"""

from __future__ import annotations

import time
from ctypes import windll
from datetime import datetime
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
CACHE_CHAT_INFO_BUTTON = "chat_info_button"
CACHE_PROFILE_ENTRY = "profile_entry"
STATUS_COLUMN = "执行状态"
ERROR_COLUMN = "错误信息"
RESULT_DIR_NAME = "result"
STATUS_TO_TEXT = {
    "": "",
    "success": "成功",
    "failed": "失败",
}
TEXT_TO_STATUS = {
    "success": "success",
    "成功": "success",
    "已成功": "success",
    "完成": "success",
    "已完成": "success",
    "failed": "failed",
    "failure": "failed",
    "失败": "failed",
    "执行失败": "failed",
}


class UiPositionCache:
    """Session-only cache for UI click points relative to a stable reference rect."""

    def __init__(self):
        self._points: dict[str, dict[str, int]] = {}

    def remember_point(self, key: str, point: tuple[int, int], reference) -> None:
        try:
            rect = reference.BoundingRectangle
            self._points[key] = {
                "x": int(point[0]),
                "y": int(point[1]),
                "dx": int(point[0] - rect.left),
                "dy": int(point[1] - rect.top),
            }
        except Exception:
            self.forget(key)

    def get_point(self, key: str, reference) -> tuple[int, int] | None:
        cached = self._points.get(key)
        if not cached:
            return None
        try:
            rect = reference.BoundingRectangle
            x = int(rect.left + cached["dx"])
            y = int(rect.top + cached["dy"])
            if x < rect.left or x > rect.right or y < rect.top or y > rect.bottom:
                self.forget(key)
                return None
            return x, y
        except Exception:
            self.forget(key)
            return None

    def forget(self, key: str) -> None:
        self._points.pop(key, None)

    def get_absolute_point(self, key: str) -> tuple[int, int] | None:
        cached = self._points.get(key)
        if not cached:
            return None
        try:
            return int(cached["x"]), int(cached["y"])
        except Exception:
            self.forget(key)
            return None


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


def _find_chat_info_panel_fast(win):
    """Fast, shallow check for an already opened chat-info sidebar.

    This intentionally avoids landmark traversal and global deep search. It is
    used only before clicking a cached three-dot position, where a quick
    "probably closed" answer is more valuable than a slow exhaustive scan.
    """
    checks = (
        {"AutomationId": SINGLE_CHAT_INFO_AUTOMATION_ID, "searchDepth": 10},
        {"ClassName": MEMBER_INFO_CLS, "searchDepth": 10},
    )
    for kwargs in checks:
        try:
            panel = win.Control(**kwargs)
            if _control_exists(panel):
                return panel
        except Exception:
            continue
    return None


def _get_chat_info_panel_state_fast(win) -> tuple[ChatInfoPanelState, object | None]:
    panel = _find_chat_info_panel_fast(win)
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


def _normalize_task_status(value) -> str:
    text = _first_non_blank(value)
    return TEXT_TO_STATUS.get(text, "")


def _status_text(status: str) -> str:
    return STATUS_TO_TEXT.get(status, status)


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
    for row_index, row in df.iterrows():
        original_name = _first_non_blank(row.get(source_col, ""))
        new_remark = _first_non_blank(row.get(target_col, ""))
        if not original_name or not new_remark:
            continue
        status = _normalize_task_status(row.get(STATUS_COLUMN, ""))
        error = _first_non_blank(row.get(ERROR_COLUMN, ""))
        task = {
            "original_name": original_name,
            "new_remark": new_remark,
            "status": status,
            "source_row_number": int(row_index) + 2,
        }
        if error:
            task["error"] = error
        tasks.append(
            task
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


def _read_import_dataframe(path: str | Path) -> pd.DataFrame:
    file_path = Path(path)
    if file_path.suffix.lower() == ".csv":
        return pd.read_csv(file_path, dtype=str).fillna("")
    return pd.read_excel(file_path, dtype=str).fillna("")


def _result_dir_for(path: str | Path) -> Path:
    file_path = Path(path)
    if file_path.parent.name.lower() == RESULT_DIR_NAME:
        return file_path.parent
    return file_path.parent / RESULT_DIR_NAME


def create_remark_result_file(source_path: str | Path) -> Path:
    """Copy imported rows into a new result workbook without modifying the source file."""
    source = Path(str(source_path).strip())
    if not source.exists():
        raise FileNotFoundError(f"文件不存在: {source}")

    result_dir = _result_dir_for(source)
    result_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_path = result_dir / f"{source.stem}_结果_{timestamp}.xlsx"

    df = _read_import_dataframe(source)
    if STATUS_COLUMN not in df.columns:
        df[STATUS_COLUMN] = ""
    if ERROR_COLUMN not in df.columns:
        df[ERROR_COLUMN] = ""
    df.to_excel(result_path, index=False)
    return result_path


def write_remark_status_to_file(result_path: str | Path, task: dict) -> None:
    """Write one task status into the generated result workbook."""
    file_path = Path(result_path)
    if not file_path.exists():
        raise FileNotFoundError(f"结果文件不存在: {file_path}")

    df = pd.read_excel(file_path, dtype=str).fillna("")
    if STATUS_COLUMN not in df.columns:
        df[STATUS_COLUMN] = ""
    if ERROR_COLUMN not in df.columns:
        df[ERROR_COLUMN] = ""

    row_number = int(task.get("source_row_number") or 0)
    if row_number < 2:
        raise ValueError("任务缺少有效的 Excel 行号，无法写入执行状态")
    row_index = row_number - 2
    if row_index < 0 or row_index >= len(df):
        raise IndexError(f"任务行号超出结果文件范围: {row_number}")

    df.loc[row_index, STATUS_COLUMN] = _status_text(task.get("status", ""))
    df.loc[row_index, ERROR_COLUMN] = _first_non_blank(task.get("error", ""))
    df.to_excel(file_path, index=False)


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
        result_path: str | Path | None = None,
        fast_mode: bool = False,
        parent=None,
    ):
        super().__init__(parent)
        self.tasks = tasks
        self.lc = WeChatLocale(locale)
        self.interval = max(0.0, float(interval))
        self.result_path = Path(result_path) if result_path else None
        self.fast_mode = bool(fast_mode)
        self._stop_requested = False
        self.ui_cache = UiPositionCache()
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

    def _try_click_cached_point(self, key: str, reference, label: str) -> bool:
        point = self.ui_cache.get_point(key, reference)
        if point is None:
            return False
        try:
            self._log(f"  -> 使用缓存位置点击{label}")
            _click_at(point[0], point[1])
            return True
        except Exception:
            self.ui_cache.forget(key)
            return False

    def _try_click_cached_absolute_point(self, key: str, label: str) -> bool:
        point = self.ui_cache.get_absolute_point(key)
        if point is None:
            return False
        try:
            self._log(f"  -> 快速模式：直接点击缓存{label}位置")
            _click_at(point[0], point[1])
            return True
        except Exception:
            self.ui_cache.forget(key)
            return False

    def _remember_control_point(self, key: str, control, reference) -> None:
        try:
            self.ui_cache.remember_point(key, control.GetPosition(), reference)
        except Exception:
            self.ui_cache.forget(key)

    def _write_task_status(self, task: dict) -> None:
        if not self.result_path:
            return
        try:
            write_remark_status_to_file(self.result_path, task)
        except Exception as exc:
            self._log(f"  -> 写入结果 Excel 失败: {exc}")

    def _wait_chat_info_panel(self, timeout: float = 1.2):
        poll_interval = 0.1
        attempts = max(1, int(timeout / poll_interval))
        for _ in range(attempts):
            time.sleep(poll_interval)
            try:
                win = self._find_wechat()
            except Exception:
                win = self._open_wechat()
            state, panel = _get_chat_info_panel_state(win)
            if state is ChatInfoPanelState.OPEN:
                return panel
        return None

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
        cached_chat_info_point = self.ui_cache.get_point(CACHE_CHAT_INFO_BUTTON, win)
        if cached_chat_info_point is not None:
            state, panel = _get_chat_info_panel_state_fast(win)
            if state is ChatInfoPanelState.OPEN:
                self._log("  -> 聊天信息侧栏已打开，进入下一步")
                return panel
        else:
            state, panel = _get_chat_info_panel_state(win)
            if state is ChatInfoPanelState.OPEN:
                self._log("  -> 聊天信息侧栏已打开，进入下一步")
                return panel

        self._log("  -> 聊天信息侧栏未打开，准备点击聊天信息/三个点")
        if cached_chat_info_point is not None and self._try_click_cached_point(
            CACHE_CHAT_INFO_BUTTON, win, "聊天信息/三个点"
        ):
            panel = self._wait_chat_info_panel()
            if panel is not None:
                self._log("  -> 缓存位置已打开聊天信息侧栏")
                return panel
            self._log("  -> 缓存位置未打开侧栏，回退遍历聊天信息按钮")
            self.ui_cache.forget(CACHE_CHAT_INFO_BUTTON)

        chat_info_btn = _find_chat_info_button(win, self.lc.chat_info)
        if chat_info_btn is None:
            raise RuntimeError("未找到 '聊天信息' / 三个点按钮")
        self._log("  -> 点击聊天信息/三个点")
        _click(chat_info_btn)

        panel = self._wait_chat_info_panel()
        if panel is not None:
            self._remember_control_point(CACHE_CHAT_INFO_BUTTON, chat_info_btn, win)
            self._log("  -> 已打开聊天信息侧栏")
            return panel
        raise RuntimeError("未找到聊天信息侧栏")

    def _open_contact_profile_from_panel(self, panel):
        if self._try_click_cached_point(CACHE_PROFILE_ENTRY, panel, "联系人头像"):
            try:
                profile = self._wait_contact_profile(timeout=1.2)
                self._log("  -> 缓存头像位置已打开资料卡")
                return profile
            except RuntimeError:
                self._log("  -> 缓存头像位置未打开资料卡，回退遍历头像组件")
                self.ui_cache.forget(CACHE_PROFILE_ENTRY)

        entry = self._find_profile_entry_in_panel(panel)
        if entry is None:
            self._log("  -> 未识别到联系人头像组件，已停止本条任务")
            raise RuntimeError("未识别到联系人头像组件")

        self._log("  -> 准备点击联系人头像")
        _click(entry)
        time.sleep(0.8)
        self._log("  -> 已点击联系人头像，等待资料卡")
        profile = self._wait_contact_profile()
        self._remember_control_point(CACHE_PROFILE_ENTRY, entry, panel)
        return profile

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

    def _open_contact_profile_fast(self):
        clicked_chat_info = self._try_click_cached_absolute_point(
            CACHE_CHAT_INFO_BUTTON, "聊天信息/三个点"
        )
        clicked_avatar = False
        if clicked_chat_info:
            time.sleep(0.15)
            clicked_avatar = self._try_click_cached_absolute_point(
                CACHE_PROFILE_ENTRY, "联系人头像"
            )

        if not clicked_chat_info or not clicked_avatar:
            return None

        time.sleep(0.25)
        return self._wait_contact_profile(timeout=1.5)

    def _update_one_remark(self, original_name: str, new_remark: str):
        self._search_contact(original_name)
        time.sleep(0.4)

        if self.fast_mode:
            try:
                profile = self._open_contact_profile_fast()
                if profile is not None:
                    self._set_profile_remark(profile, new_remark)
                    return
                self._log("  -> 快速模式缺少缓存位置，回退常规定位并学习位置")
            except RuntimeError as exc:
                self._log(f"  -> 快速模式点击失败，回退常规定位: {exc}")
                self.ui_cache.forget(CACHE_CHAT_INFO_BUTTON)
                self.ui_cache.forget(CACHE_PROFILE_ENTRY)

        panel = self._open_chat_info_panel()
        profile = self._open_contact_profile_from_panel(panel)
        self._set_profile_remark(profile, new_remark)

    def _run_update(self) -> dict:
        total = len(self.tasks)
        success = 0
        failed = 0
        skipped = 0

        for idx, task in enumerate(self.tasks):
            if self._stop_requested:
                self._log("用户终止操作")
                break

            original_name = task.get("original_name", "")
            new_remark = task.get("new_remark", "")
            if task.get("status") == "success":
                skipped += 1
                self.progress.emit(idx + 1, total, f"跳过已成功: {original_name}")
                self._log(f"[{idx+1}/{total}] {original_name} 已成功，跳过")
                continue

            self.progress.emit(idx, total, f"正在修改: {original_name}")
            self._log(f"[{idx+1}/{total}] {original_name} -> {new_remark}")

            try:
                self._update_one_remark(original_name, new_remark)
                success += 1
                task["status"] = "success"
                task.pop("error", None)
                self._log("  -> 修改成功")
            except Exception as exc:
                failed += 1
                task["status"] = "failed"
                task["error"] = str(exc)
                self._log(f"  -> 修改失败: {exc}")
            finally:
                self._write_task_status(task)

            if idx < total - 1 and not self._stop_requested and self.interval > 0:
                time.sleep(self.interval)

        self.progress.emit(total, total, "完成")
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "skipped": skipped,
            "stopped": self._stop_requested,
        }
