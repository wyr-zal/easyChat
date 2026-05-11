# -*- coding: utf-8 -*-

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import remark_manager.remark_manager_service as service


class FakeRect:
    def __init__(self, left=100, top=100, right=300, bottom=200):
        self.left = left
        self.top = top
        self.right = right
        self.bottom = bottom


class FakeControl:
    def __init__(
        self,
        exists=True,
        name="",
        control_type="",
        class_name="",
        automation_id="",
        children=None,
        rect=None,
    ):
        self._exists = exists
        self.Name = name
        self.ControlTypeName = control_type
        self.ClassName = class_name
        self.AutomationId = automation_id
        self.children = children or []
        self.BoundingRectangle = rect or FakeRect()
        for child in self.children:
            child.parent = self

    def Exists(self, *_args, **_kwargs):
        return self._exists

    def GetChildren(self):
        return self.children

    def GetPosition(self):
        rect = self.BoundingRectangle
        return ((rect.left + rect.right) // 2, (rect.top + rect.bottom) // 2)

    def Control(self, **kwargs):
        class_name = kwargs.get("ClassName")
        automation_id = kwargs.get("AutomationId")
        for child in self.children:
            if class_name and child.ClassName != class_name:
                continue
            if automation_id and child.AutomationId != automation_id:
                continue
            if class_name or automation_id:
                return child
        return FakeControl(False)

    def ListControl(self, **kwargs):
        automation_id = kwargs.get("AutomationId")
        for child in self.children:
            if child.ControlTypeName == "ListControl" and (
                not automation_id or getattr(child, "AutomationId", "") == automation_id
            ):
                return child
        return FakeControl(False)

    def ButtonControl(self, **kwargs):
        name = kwargs.get("Name")
        automation_id = kwargs.get("AutomationId")
        for child in self.children:
            if child.ControlTypeName != "ButtonControl":
                continue
            if name and child.Name != name:
                continue
            if automation_id and child.AutomationId != automation_id:
                continue
            if name or automation_id:
                return child
        return FakeControl(False)


class RemarkTaskBuildTest(unittest.TestCase):
    def test_build_remark_tasks_defaults_to_first_two_columns_and_skips_blank_rows(self):
        df = pd.DataFrame(
            {
                "原始名": ["科学-陈老师", "", "日乌", "A阳茹最好"],
                "新备注": ["科学-陈老师-新", "空原始名", "", "A阳茹-2026"],
                "其他列": ["x", "y", "z", "w"],
            }
        )

        tasks = service.build_remark_tasks(df)

        self.assertEqual(
            tasks,
            [
                {
                    "original_name": "科学-陈老师",
                    "new_remark": "科学-陈老师-新",
                    "status": "",
                },
                {
                    "original_name": "A阳茹最好",
                    "new_remark": "A阳茹-2026",
                    "status": "",
                },
            ],
        )

    def test_build_remark_tasks_can_use_custom_columns(self):
        df = pd.DataFrame(
            {
                "微信昵称": ["科学-陈老师"],
                "目标备注": ["2026-科学老师"],
            }
        )

        tasks = service.build_remark_tasks(df, "微信昵称", "目标备注")

        self.assertEqual(tasks[0]["original_name"], "科学-陈老师")
        self.assertEqual(tasks[0]["new_remark"], "2026-科学老师")


class RemarkProfileLocatorTest(unittest.TestCase):
    def test_remark_click_point_uses_value_text_on_same_row_when_available(self):
        label = FakeControl(
            True,
            name="备注",
            control_type="TextControl",
            rect=FakeRect(390, 440, 470, 482),
        )
        value = FakeControl(
            True,
            name="科学-陈老师",
            control_type="TextControl",
            rect=FakeRect(535, 440, 760, 482),
        )
        profile = FakeControl(
            True,
            control_type="GroupControl",
            children=[label, value],
            rect=FakeRect(340, 160, 905, 1275),
        )

        point = service._get_remark_edit_click_point(profile, "备注")

        self.assertEqual(point, (647, 461))

    def test_remark_click_point_falls_back_to_right_side_of_remark_label(self):
        label = FakeControl(
            True,
            name="备注",
            control_type="TextControl",
            rect=FakeRect(390, 440, 470, 482),
        )
        profile = FakeControl(
            True,
            control_type="GroupControl",
            children=[label],
            rect=FakeRect(340, 160, 905, 1275),
        )

        point = service._get_remark_edit_click_point(profile, "备注")

        self.assertEqual(point, (590, 461))


class PersonalChatInfoPanelLocatorTest(unittest.TestCase):
    def test_open_chat_info_panel_accepts_personal_sidebar_landmarks(self):
        thread = service.RemarkManagerThread(tasks=[])
        thread.lc = SimpleNamespace(chat_info="聊天信息")
        logs = []
        thread._log = logs.append

        chat_info_btn = FakeControl(True, name="聊天信息", control_type="ButtonControl")
        landmark_a = FakeControl(True, name="查找聊天内容", control_type="TextControl")
        landmark_b = FakeControl(True, name="置顶聊天", control_type="TextControl")
        sidebar = FakeControl(
            True,
            control_type="GroupControl",
            class_name="mmui::ContactInfoView",
            children=[landmark_a, landmark_b],
            rect=FakeRect(1640, 84, 2016, 1200),
        )
        win = FakeControl(
            True,
            name="微信",
            control_type="WindowControl",
            class_name="mmui::MainWindow",
            children=[chat_info_btn, sidebar],
            rect=FakeRect(856, 0, 2016, 1200),
        )
        thread._open_wechat = lambda: win

        clicked = []

        with (
            patch.object(service.auto, "Control", return_value=FakeControl(False)),
            patch.object(service, "_click", lambda control: clicked.append(control)),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            panel = service.RemarkManagerThread._open_chat_info_panel(thread)

        self.assertIs(panel, sidebar)
        self.assertEqual(clicked, [])
        self.assertTrue(any("聊天信息侧栏" in msg for msg in logs))

    def test_open_chat_info_panel_skips_three_dot_when_sidebar_already_open(self):
        thread = service.RemarkManagerThread(tasks=[])
        thread.lc = SimpleNamespace(chat_info="聊天信息")
        logs = []
        thread._log = logs.append

        chat_info_btn = FakeControl(True, name="聊天信息", control_type="ButtonControl")
        sidebar = FakeControl(
            True,
            control_type="GroupControl",
            class_name="mmui::XView",
            automation_id="single_chat_info_view",
            children=[
                FakeControl(True, name="查找聊天内容", control_type="TextControl"),
                FakeControl(True, name="置顶聊天", control_type="TextControl"),
            ],
            rect=FakeRect(934, 138, 1434, 1702),
        )
        win = FakeControl(
            True,
            name="微信",
            control_type="WindowControl",
            class_name="mmui::MainWindow",
            children=[chat_info_btn, sidebar],
            rect=FakeRect(2, 0, 1438, 1702),
        )
        thread._open_wechat = lambda: win
        clicked = []

        with (
            patch.object(service, "_click", lambda control: clicked.append(control)),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            panel = service.RemarkManagerThread._open_chat_info_panel(thread)

        self.assertIs(panel, sidebar)
        self.assertEqual(clicked, [])
        self.assertTrue(any("侧栏已打开" in msg for msg in logs))

    def test_open_chat_info_panel_clicks_three_dot_only_when_sidebar_closed(self):
        thread = service.RemarkManagerThread(tasks=[])
        thread.lc = SimpleNamespace(chat_info="聊天信息")
        logs = []
        thread._log = logs.append

        chat_info_btn = FakeControl(True, name="聊天信息", control_type="ButtonControl")
        closed_win = FakeControl(
            True,
            name="微信",
            control_type="WindowControl",
            class_name="mmui::MainWindow",
            children=[chat_info_btn],
            rect=FakeRect(2, 0, 1438, 1702),
        )
        sidebar = FakeControl(
            True,
            control_type="GroupControl",
            class_name="mmui::XView",
            automation_id="single_chat_info_view",
            children=[
                FakeControl(True, name="查找聊天内容", control_type="TextControl"),
                FakeControl(True, name="置顶聊天", control_type="TextControl"),
            ],
            rect=FakeRect(934, 138, 1434, 1702),
        )
        opened_win = FakeControl(
            True,
            name="微信",
            control_type="WindowControl",
            class_name="mmui::MainWindow",
            children=[chat_info_btn, sidebar],
            rect=FakeRect(2, 0, 1438, 1702),
        )
        windows = [closed_win, opened_win]
        thread._open_wechat = lambda: windows.pop(0) if windows else opened_win
        clicked = []

        with (
            patch.object(service, "_click", lambda control: clicked.append(control)),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            panel = service.RemarkManagerThread._open_chat_info_panel(thread)

        self.assertIs(panel, sidebar)
        self.assertEqual(clicked, [chat_info_btn])
        self.assertTrue(any("侧栏未打开" in msg for msg in logs))
        self.assertTrue(any("点击聊天信息" in msg for msg in logs))

    def test_personal_sidebar_landmark_sort_does_not_compare_uia_controls(self):
        landmarks = [
            FakeControl(True, name="查找聊天内容", control_type="TextControl"),
            FakeControl(True, name="置顶聊天", control_type="TextControl"),
        ]
        first_sidebar = FakeControl(
            True,
            control_type="GroupControl",
            class_name="mmui::ContactInfoView",
            children=landmarks,
            rect=FakeRect(1640, 84, 2016, 1200),
        )
        second_sidebar = FakeControl(
            True,
            control_type="GroupControl",
            class_name="mmui::ContactInfoView",
            children=[
                FakeControl(True, name="查找聊天内容", control_type="TextControl"),
                FakeControl(True, name="置顶聊天", control_type="TextControl"),
            ],
            rect=FakeRect(1640, 84, 2016, 1200),
        )
        win = FakeControl(
            True,
            name="微信",
            control_type="WindowControl",
            children=[first_sidebar, second_sidebar],
            rect=FakeRect(856, 0, 2016, 1200),
        )

        panel = service._find_personal_info_panel_by_landmarks(win)

        self.assertIn(panel, [first_sidebar, second_sidebar])

    def test_profile_entry_prefers_avatar_over_add_button_in_personal_sidebar(self):
        thread = service.RemarkManagerThread(tasks=[])
        add_button = FakeControl(
            True,
            name="添加",
            control_type="ButtonControl",
            rect=FakeRect(1768, 108, 1838, 178),
        )
        avatar = FakeControl(
            True,
            name="2026-科学-陈老师",
            control_type="ImageControl",
            rect=FakeRect(1664, 108, 1734, 178),
        )
        sidebar = FakeControl(
            True,
            control_type="GroupControl",
            class_name="mmui::ContactInfoView",
            children=[add_button, avatar],
            rect=FakeRect(1640, 84, 2016, 1200),
        )

        found = service.RemarkManagerThread._find_profile_entry_in_panel(thread, sidebar)

        self.assertIs(found, avatar)

    def test_profile_entry_prefers_single_chat_member_cell_automation_id(self):
        thread = service.RemarkManagerThread(tasks=[])
        avatar = FakeControl(
            True,
            name="日乌",
            control_type="ButtonControl",
            class_name="mmui::ChatMemberCell",
            automation_id="single_chat_member_cell",
            rect=FakeRect(970, 166, 1042, 278),
        )
        sidebar = FakeControl(
            True,
            control_type="GroupControl",
            automation_id="single_chat_info_view",
            children=[avatar],
            rect=FakeRect(934, 138, 1434, 1702),
        )

        found = service.RemarkManagerThread._find_profile_entry_in_panel(thread, sidebar)

        self.assertIs(found, avatar)

    def test_open_contact_profile_fails_without_avatar_component_instead_of_fallback_click(self):
        thread = service.RemarkManagerThread(tasks=[])
        logs = []
        thread._log = logs.append
        panel = FakeControl(
            True,
            control_type="GroupControl",
            class_name="mmui::ContactInfoView",
            children=[
                FakeControl(
                    True,
                    name="添加",
                    control_type="ButtonControl",
                    rect=FakeRect(1768, 108, 1838, 178),
                )
            ],
            rect=FakeRect(1640, 84, 2016, 1200),
        )
        clicked = []

        with patch.object(service, "_click_at", lambda x, y: clicked.append((x, y))):
            with self.assertRaisesRegex(RuntimeError, "未识别到联系人头像组件"):
                service.RemarkManagerThread._open_contact_profile_from_panel(thread, panel)

        self.assertEqual(clicked, [])
        self.assertTrue(any("未识别到联系人头像组件" in msg for msg in logs))


class RemarkUpdateFlowTest(unittest.TestCase):
    def test_update_one_remark_searches_opens_profile_then_sets_remark(self):
        thread = service.RemarkManagerThread(tasks=[])
        panel = object()
        profile = object()
        calls = []
        thread._search_contact = lambda name: calls.append(("search", name))
        thread._open_chat_info_panel = lambda: calls.append("open_panel") or panel
        thread._open_contact_profile_from_panel = (
            lambda current_panel: calls.append(("open_profile", current_panel)) or profile
        )
        thread._set_profile_remark = (
            lambda current_profile, remark: calls.append(("set_remark", current_profile, remark))
        )

        with patch.object(service.time, "sleep", lambda _seconds: None):
            service.RemarkManagerThread._update_one_remark(
                thread, "科学-陈老师", "科学-陈老师-新"
            )

        self.assertEqual(
            calls,
            [
                ("search", "科学-陈老师"),
                "open_panel",
                ("open_profile", panel),
                ("set_remark", profile, "科学-陈老师-新"),
            ],
        )

    def test_run_update_continues_after_single_row_failure(self):
        tasks = [
            {"original_name": "不存在", "new_remark": "失败备注", "status": ""},
            {"original_name": "科学-陈老师", "new_remark": "成功备注", "status": ""},
        ]
        thread = service.RemarkManagerThread(tasks=tasks, interval=0.5)
        logs = []
        progresses = []
        thread._log = logs.append
        thread.progress = SimpleNamespace(emit=lambda *args: progresses.append(args))

        def update_one(original_name, new_remark):
            if original_name == "不存在":
                raise RuntimeError("未找到联系人")

        thread._update_one_remark = update_one

        with patch.object(service.time, "sleep", lambda _seconds: None):
            summary = service.RemarkManagerThread._run_update(thread)

        self.assertEqual(summary["total"], 2)
        self.assertEqual(summary["success"], 1)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(tasks[0]["status"], "failed")
        self.assertEqual(tasks[1]["status"], "success")
        self.assertIn("未找到联系人", tasks[0]["error"])
        self.assertTrue(any("修改失败" in msg for msg in logs))


if __name__ == "__main__":
    unittest.main()
