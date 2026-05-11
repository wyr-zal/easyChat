# -*- coding: utf-8 -*-

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import group_manager_service as service


class FakeControl:
    def __init__(self, exists=True):
        self._exists = exists
        self.Name = ""
        self.ClassName = ""
        self.AutomationId = ""
        self.ControlTypeName = ""
        self.parent = None

    def Exists(self, *args, **kwargs):
        return self._exists

    def GetParentControl(self):
        return self.parent


class FakeRect:
    def __init__(self, left=100, top=100, right=300, bottom=200):
        self.left = left
        self.top = top
        self.right = right
        self.bottom = bottom


class FakeChatMoreMenu(FakeControl):
    def __init__(self, scoped_item):
        super().__init__(True)
        self.scoped_item = scoped_item
        self.list_item_calls = []

    def ListItemControl(self, **kwargs):
        self.list_item_calls.append(kwargs)
        return self.scoped_item


class FakeAuto:
    def __init__(self, menu):
        self.menu = menu
        self.list_control_calls = []
        self.global_list_item_calls = []

    def ListControl(self, **kwargs):
        self.list_control_calls.append(kwargs)
        return self.menu

    def ListItemControl(self, **kwargs):
        self.global_list_item_calls.append(kwargs)
        return FakeControl(False)


class FakeWindowControl(FakeControl):
    def __init__(self, exists=True, name=""):
        super().__init__(exists)
        self.Name = name


class FakePickerSearchControl(FakeControl):
    def __init__(
        self,
        exists=True,
        name="",
        control_type="",
        children=None,
        automation_id="",
        class_name="",
    ):
        super().__init__(exists)
        self.Name = name
        self.ControlTypeName = control_type
        self.AutomationId = automation_id
        self.ClassName = class_name
        self.children = children or []
        for child in self.children:
            child.parent = self

    def GetChildren(self):
        return self.children

    def ListControl(self, **kwargs):
        automation_id = kwargs.get("AutomationId")
        for child in self.children:
            if (
                child.ControlTypeName == "ListControl"
                and (not automation_id or child.AutomationId == automation_id)
            ):
                return child
        return FakePickerSearchControl(False)


class FakeSearchEdit(FakePickerSearchControl):
    pass


class FakePickerForVisibleResult(FakePickerSearchControl):
    def __init__(self, visible_result, contact_list=None):
        super().__init__(True, name="微信发起群聊", control_type="WindowControl")
        self.search_edit = FakeSearchEdit(True, name="搜索", control_type="EditControl")
        self.visible_result = visible_result
        self.contact_list = contact_list or FakePickerSearchControl(False)
        self.children = [visible_result]

    def EditControl(self, **_kwargs):
        return self.search_edit

    def ListControl(self, **_kwargs):
        return self.contact_list


class FakeCancelablePicker(FakePickerForVisibleResult):
    def __init__(self, contact_list, cancel_btn=None):
        first_child = contact_list.GetChildren()[0] if contact_list.GetChildren() else FakeControl(False)
        super().__init__(first_child, contact_list=contact_list)
        self.cancel_btn = cancel_btn or FakeControl(False)

    def ButtonControl(self, **kwargs):
        if kwargs.get("Name") == "取消":
            return self.cancel_btn
        return FakeControl(False)


class FakePickerAuto:
    def __init__(self):
        self.window_control_calls = []

    def WindowControl(self, **kwargs):
        self.window_control_calls.append(kwargs)
        if (
            kwargs.get("ClassName") == service.PICKER_WINDOW_CLS
            and kwargs.get("searchDepth") == 10
            and "Depth" not in kwargs
        ):
            return FakeWindowControl(True, "微信发起群聊")
        return FakeWindowControl(False, "")


class ChatMoreMenuLocatorTest(unittest.TestCase):
    def test_prefers_chat_more_entry_menu_container_for_initiate_group(self):
        scoped_item = FakeControl(True)
        menu = FakeChatMoreMenu(scoped_item)
        fake_auto = FakeAuto(menu)

        with patch.object(service, "auto", fake_auto):
            item = service._find_chat_more_menu_item("发起群聊", timeout=0.01)

        self.assertIs(item, scoped_item)
        self.assertEqual(
            fake_auto.list_control_calls[0],
            {"AutomationId": "chat_more_entry", "searchDepth": 15},
        )
        self.assertEqual(
            menu.list_item_calls[0],
            {"Name": "发起群聊", "searchDepth": 5},
        )
        self.assertEqual(fake_auto.global_list_item_calls, [])


class PickerWindowLocatorTest(unittest.TestCase):
    def test_finds_deeper_weixin_prefixed_session_picker_window(self):
        fake_auto = FakePickerAuto()

        with (
            patch.object(service, "auto", fake_auto),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            picker = service.GroupManagerThread._wait_picker_window(
                object(), "发起群聊", timeout=0.01
            )

        self.assertEqual(picker.Name, "微信发起群聊")
        self.assertIn(
            {"ClassName": service.PICKER_WINDOW_CLS, "searchDepth": 10},
            fake_auto.window_control_calls,
        )


class MainSearchLocatorTest(unittest.TestCase):
    def test_prefers_left_chat_search_over_group_member_panel_search(self):
        left_search = FakePickerSearchControl(
            True,
            name="搜索",
            control_type="EditControl",
            class_name="mmui::XValidatorTextEdit",
        )
        left_search.BoundingRectangle = FakeRect(1600, 60, 1820, 105)
        panel_search = FakePickerSearchControl(
            True,
            name="搜索",
            control_type="EditControl",
            class_name="mmui::XValidatorTextEdit",
        )
        panel_search.BoundingRectangle = FakeRect(2450, 160, 2810, 205)
        panel = FakePickerSearchControl(
            True,
            control_type="GroupControl",
            class_name=service.MEMBER_INFO_CLS,
            children=[panel_search],
        )
        panel.BoundingRectangle = FakeRect(2350, 138, 2850, 1702)
        win = FakePickerSearchControl(
            True,
            control_type="WindowControl",
            class_name="mmui::MainWindow",
            children=[left_search, panel],
        )
        win.BoundingRectangle = FakeRect(1412, 0, 2850, 1702)

        found = service._find_main_search_box(win, "搜索")

        self.assertIs(found, left_search)


class CreateGroupMemberSelectionTest(unittest.TestCase):
    def test_skips_missing_member_and_continues_with_remaining_members(self):
        thread = service.GroupManagerThread(service.GroupManagerThread.MODE_CREATE, [])
        thread.lc = SimpleNamespace(
            quick_action="快捷操作",
            initiate_group="发起群聊",
            done="完成",
        )
        logs = []
        thread._log = logs.append
        thread._open_wechat = lambda: None
        thread._wait_picker_window = lambda _title: object()
        confirmed = []
        thread._click_picker_confirm = lambda picker, name: confirmed.append((picker, name))
        checked_members = []

        def search_and_check(_picker, member):
            checked_members.append(member)
            if member == "周安乐":
                raise RuntimeError("搜索 '周安乐' 后未找到联系人列表")

        thread._picker_search_and_check = search_and_check

        with (
            patch.object(service.auto, "ButtonControl", return_value=FakeControl(True)),
            patch.object(service, "_find_chat_more_menu_item", return_value=FakeControl(True)),
            patch.object(service, "_click", lambda _control: None),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            service.GroupManagerThread._create_one_group(
                thread, ["周安乐", "日乌", "科学-陈老师"]
            )

        self.assertEqual(checked_members, ["周安乐", "日乌", "科学-陈老师"])
        self.assertEqual(len(confirmed), 1)
        self.assertTrue(any("跳过成员 '周安乐'" in msg for msg in logs))

    def test_renames_group_after_successful_create_when_group_name_is_set(self):
        thread = service.GroupManagerThread(service.GroupManagerThread.MODE_CREATE, [])
        thread.lc = SimpleNamespace(
            quick_action="快捷操作",
            initiate_group="发起群聊",
            done="完成",
        )
        thread._log = lambda _msg: None
        thread._open_wechat = lambda: None
        thread._wait_picker_window = lambda _title: object()
        thread._picker_search_and_check = lambda _picker, _member: None
        thread._click_picker_confirm = lambda _picker, _button_name: None
        renamed = []
        thread._rename_current_group = renamed.append

        with (
            patch.object(service.auto, "ButtonControl", return_value=FakeControl(True)),
            patch.object(service, "_find_chat_more_menu_item", return_value=FakeControl(True)),
            patch.object(service, "_click", lambda _control: None),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            service.GroupManagerThread._create_one_group(
                thread, ["日乌", "科学-陈老师"], "项目讨论组"
            )

        self.assertEqual(renamed, ["项目讨论组"])


class GroupRenameTest(unittest.TestCase):
    def test_group_rename_uses_chat_info_name_entry_enter_and_modify(self):
        thread = service.GroupManagerThread(service.GroupManagerThread.MODE_CREATE, [])
        thread.lc = SimpleNamespace(chat_info="聊天信息")
        logs = []
        thread._log = logs.append
        thread._open_wechat = lambda: object()

        chat_info_btn = FakeControl(True)
        chat_info_btn.Name = "聊天信息"
        confirm_btn = FakeControl(True)
        confirm_btn.Name = "修改"

        label = FakePickerSearchControl(
            True,
            name="群聊名称",
            control_type="TextControl",
        )
        label.BoundingRectangle = FakeRect(200, 300, 360, 340)
        panel = FakePickerSearchControl(
            True,
            control_type="GroupControl",
            class_name=service.MEMBER_INFO_CLS,
            children=[label],
        )
        panel.BoundingRectangle = FakeRect(100, 100, 600, 900)

        clicked = []
        click_points = []
        copied = []
        sent_keys = []

        def button_control(**kwargs):
            if kwargs.get("Name") == "聊天信息":
                return chat_info_btn
            if kwargs.get("Name") == "修改":
                return confirm_btn
            return FakeControl(False)

        with (
            patch.object(service.auto, "ButtonControl", side_effect=button_control),
            patch.object(service.auto, "Control", return_value=panel),
            patch.object(service, "_click", lambda control: clicked.append(control)),
            patch.object(service, "_click_at", lambda x, y: click_points.append((x, y))),
            patch.object(service.auto, "SendKeys", lambda keys: sent_keys.append(keys)),
            patch.object(service.pyperclip, "copy", lambda text: copied.append(text)),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            service.GroupManagerThread._rename_current_group(thread, "项目讨论组")

        self.assertIn(chat_info_btn, clicked)
        self.assertIn(confirm_btn, clicked)
        self.assertEqual(copied, ["项目讨论组"])
        self.assertIn("{Ctrl}a", sent_keys)
        self.assertIn("{Ctrl}v", sent_keys)
        self.assertIn("{Enter}", sent_keys)
        self.assertEqual(click_points[0], (240, 450))


class GroupMemberRemoveButtonLocatorTest(unittest.TestCase):
    def test_finds_remove_button_by_named_descendant_inside_group_info_panel(self):
        remove_button = FakePickerSearchControl(
            True,
            name="移出",
            control_type="TextControl",
        )
        member_list = FakePickerSearchControl(
            True,
            name="成员列表",
            control_type="ListControl",
            automation_id="chat_member_list",
            children=[remove_button],
        )
        panel = FakePickerSearchControl(
            True,
            control_type="GroupControl",
            class_name=service.MEMBER_INFO_CLS,
            children=[member_list],
        )

        found = service._find_group_action_control(panel, ("移出", "-"), max_depth=6)

        self.assertIs(found, remove_button)

    def test_clicks_remove_slot_from_member_grid_when_button_is_not_exposed(self):
        thread = service.GroupManagerThread(service.GroupManagerThread.MODE_EXIT, [])
        thread.lc = SimpleNamespace(remove="移出")
        members = []
        for idx, name in enumerate(["周安乐", "日乌", "科学-陈老师"]):
            cell = FakePickerSearchControl(
                True,
                name=name,
                control_type="ListItemControl",
                class_name="mmui::ChatMemberCell",
            )
            cell.BoundingRectangle = FakeRect(
                left=2420 + idx * 106,
                top=254,
                right=2510 + idx * 106,
                bottom=380,
            )
            members.append(cell)
        member_list = FakePickerSearchControl(
            True,
            name="聊天成员",
            control_type="ListControl",
            automation_id="chat_member_list",
            class_name="QFReuseGridWidget",
            children=members,
        )
        member_list.BoundingRectangle = FakeRect(2374, 222, 2874, 1702)
        panel = FakePickerSearchControl(
            True,
            control_type="GroupControl",
            class_name=service.MEMBER_INFO_CLS,
            children=[member_list],
        )

        clicked_controls = []
        click_points = []
        with (
            patch.object(service, "_click", lambda control: clicked_controls.append(control)),
            patch.object(service, "_click_at", lambda x, y: click_points.append((x, y))),
        ):
            service.GroupManagerThread._click_remove_members_entry(thread, panel)

        self.assertEqual(clicked_controls, [])
        self.assertEqual(click_points, [(2465, 443)])


class GroupDeleteFlowTest(unittest.TestCase):
    def test_exit_group_removes_all_members_before_self_exit(self):
        thread = service.GroupManagerThread(service.GroupManagerThread.MODE_EXIT, [])
        first_panel = FakePickerSearchControl(
            True,
            control_type="GroupControl",
            class_name=service.MEMBER_INFO_CLS,
        )
        refreshed_panel = FakePickerSearchControl(
            True,
            control_type="GroupControl",
            class_name=service.MEMBER_INFO_CLS,
        )
        calls = []
        thread._search_contact = lambda group_name: calls.append(("search", group_name))
        panels = [first_panel, refreshed_panel]
        thread._open_group_info_panel = lambda: calls.append("open_panel") or panels.pop(0)
        thread._remove_all_members_from_group = lambda current_panel: calls.append(
            ("remove_all", current_panel)
        )
        thread._exit_current_group_from_panel = lambda current_panel: calls.append(
            ("exit_self", current_panel)
        )

        with patch.object(service.time, "sleep", lambda _seconds: None):
            service.GroupManagerThread._exit_one_group(thread, "项目讨论组")

        self.assertEqual(
            calls,
            [
                ("search", "项目讨论组"),
                "open_panel",
                ("remove_all", first_panel),
                "open_panel",
                ("exit_self", refreshed_panel),
            ],
        )

    def test_remove_all_members_clicks_remove_button_selects_all_and_confirms(self):
        thread = service.GroupManagerThread(service.GroupManagerThread.MODE_EXIT, [])
        thread.lc = SimpleNamespace(remove="移出")
        remove_button = FakePickerSearchControl(
            True,
            name="移出",
            control_type="TextControl",
        )
        panel = FakePickerSearchControl(
            True,
            control_type="GroupControl",
            class_name=service.MEMBER_INFO_CLS,
            children=[remove_button],
        )
        member_a = FakePickerSearchControl(
            True, name="日乌", control_type="CheckBoxControl"
        )
        member_b = FakePickerSearchControl(
            True, name="科学-陈老师", control_type="CheckBoxControl"
        )
        member_list = FakePickerSearchControl(
            True,
            name="请勾选需要移出的群成员",
            control_type="ListControl",
            automation_id="sp_to_select_contact_list",
            children=[member_a, member_b],
        )
        picker = FakePickerForVisibleResult(member_a, contact_list=member_list)
        empty_cancel = FakeControl(True)
        empty_picker = FakeCancelablePicker(
            FakePickerSearchControl(True, control_type="ListControl", children=[]),
            cancel_btn=empty_cancel,
        )
        picker_rounds = [picker, empty_picker]
        clicked = []
        confirmed = []
        logs = []
        thread._wait_picker_window = lambda title: picker_rounds.pop(0)
        thread._open_group_info_panel = lambda: panel
        thread._click_picker_confirm = lambda current_picker, button_name: confirmed.append(
            (current_picker, button_name)
        )
        thread._log = logs.append

        with (
            patch.object(service, "_click", lambda control: clicked.append(control)),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            service.GroupManagerThread._remove_all_members_from_group(thread, panel)

        self.assertEqual(clicked, [remove_button, member_a, member_b, remove_button, empty_cancel])
        self.assertEqual(confirmed, [(picker, "移出")])
        self.assertTrue(any("已选择 2 个成员" in msg for msg in logs))

    def test_remove_all_members_loops_until_no_removable_members_left(self):
        thread = service.GroupManagerThread(service.GroupManagerThread.MODE_EXIT, [])
        thread.lc = SimpleNamespace(remove="移出")
        remove_button = FakePickerSearchControl(
            True,
            name="移出",
            control_type="TextControl",
        )
        panel = FakePickerSearchControl(
            True,
            control_type="GroupControl",
            class_name=service.MEMBER_INFO_CLS,
            children=[remove_button],
        )
        member_a = FakePickerSearchControl(True, name="日乌", control_type="CheckBoxControl")
        member_b = FakePickerSearchControl(True, name="科学-陈老师", control_type="CheckBoxControl")
        member_c = FakePickerSearchControl(True, name="周安乐", control_type="CheckBoxControl")
        cancel_btn = FakeControl(True)
        picker_rounds = [
            FakeCancelablePicker(
                FakePickerSearchControl(True, control_type="ListControl", children=[member_a, member_b])
            ),
            FakeCancelablePicker(
                FakePickerSearchControl(True, control_type="ListControl", children=[member_c])
            ),
            FakeCancelablePicker(
                FakePickerSearchControl(True, control_type="ListControl", children=[]),
                cancel_btn=cancel_btn,
            ),
        ]
        wait_calls = []

        def wait_picker(title):
            wait_calls.append(title)
            return picker_rounds.pop(0)

        clicked = []
        confirmed = []
        logs = []
        thread._wait_picker_window = wait_picker
        thread._open_group_info_panel = lambda: panel
        thread._click_picker_confirm = lambda current_picker, button_name: confirmed.append(
            (current_picker, button_name)
        )
        thread._log = logs.append

        with (
            patch.object(service, "_click", lambda control: clicked.append(control)),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            service.GroupManagerThread._remove_all_members_from_group(thread, panel)

        self.assertEqual(
            clicked,
            [remove_button, member_a, member_b, remove_button, member_c, remove_button, cancel_btn],
        )
        self.assertEqual(len(confirmed), 2)
        self.assertEqual(wait_calls, ["移出群成员", "移出群成员", "移出群成员"])
        self.assertTrue(any("没有可继续移出的成员" in msg for msg in logs))

    def test_remove_all_members_stops_when_only_self_member_left(self):
        thread = service.GroupManagerThread(service.GroupManagerThread.MODE_EXIT, [])
        thread.lc = SimpleNamespace(remove="移出")
        self_member = FakePickerSearchControl(
            True,
            name="周安乐",
            control_type="ListItemControl",
            class_name="mmui::ChatMemberCell",
        )
        member_list = FakePickerSearchControl(
            True,
            name="聊天成员",
            control_type="ListControl",
            automation_id="chat_member_list",
            class_name="QFReuseGridWidget",
            children=[self_member],
        )
        panel = FakePickerSearchControl(
            True,
            control_type="GroupControl",
            class_name=service.MEMBER_INFO_CLS,
            children=[member_list],
        )
        clicked = []
        logs = []
        thread._log = logs.append
        thread._wait_picker_window = lambda _title: self.fail("不应再等待移出群成员窗口")

        with (
            patch.object(service, "_click", lambda control: clicked.append(control)),
            patch.object(service, "_click_at", lambda x, y: clicked.append((x, y))),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            service.GroupManagerThread._remove_all_members_from_group(thread, panel)

        self.assertEqual(clicked, [])
        self.assertTrue(any("只剩自己" in msg for msg in logs))

    def test_exit_current_group_clicks_exit_group_component_and_confirm(self):
        thread = service.GroupManagerThread(service.GroupManagerThread.MODE_EXIT, [])
        thread.lc = SimpleNamespace(exit_group="退出群聊")
        exit_button = FakePickerSearchControl(
            True,
            name="退出群聊",
            control_type="TextControl",
        )
        panel = FakePickerSearchControl(
            True,
            control_type="GroupControl",
            class_name=service.MEMBER_INFO_CLS,
            children=[exit_button],
        )
        confirm_btn = FakeControl(True)
        clicked = []
        logs = []
        thread._log = logs.append

        with (
            patch.object(service, "_click", lambda control: clicked.append(control)),
            patch.object(service.auto, "ButtonControl", return_value=confirm_btn),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            service.GroupManagerThread._exit_current_group_from_panel(thread, panel)

        self.assertEqual(clicked, [exit_button, confirm_btn])
        self.assertTrue(any("已点击退出确认" in msg for msg in logs))

    def test_exit_current_group_uses_sidebar_anchor_when_exit_component_is_hidden(self):
        thread = service.GroupManagerThread(service.GroupManagerThread.MODE_EXIT, [])
        thread.lc = SimpleNamespace(exit_group="退出群聊")
        panel = FakePickerSearchControl(
            True,
            control_type="GroupControl",
            class_name=service.MEMBER_INFO_CLS,
        )
        panel.BoundingRectangle = FakeRect(100, 200, 500, 1000)
        confirm_btn = FakeControl(False)
        moved = []
        wheel_down_calls = []
        click_points = []
        logs = []
        thread._log = logs.append

        with (
            patch.object(service, "_move", lambda control: moved.append(control)),
            patch.object(service, "_click_at", lambda x, y: click_points.append((x, y))),
            patch.object(service.auto, "WheelDown", lambda **_kwargs: wheel_down_calls.append(True)),
            patch.object(service.auto, "ButtonControl", return_value=confirm_btn),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            service.GroupManagerThread._exit_current_group_from_panel(thread, panel)

        self.assertEqual(click_points, [(300, 920)])
        self.assertEqual(len(wheel_down_calls), 12)
        self.assertTrue(any("侧栏底部位置" in msg for msg in logs))


class PickerSearchResultClickTest(unittest.TestCase):
    def test_deduplicates_picker_member_checkboxes_by_name_and_rect(self):
        controls = []
        for _ in range(3):
            member_a = FakePickerSearchControl(
                True, name="日乌", control_type="CheckBoxControl"
            )
            member_a.BoundingRectangle = FakeRect(1120, 110, 1180, 170)
            member_b = FakePickerSearchControl(
                True, name="科学-陈老师", control_type="CheckBoxControl"
            )
            member_b.BoundingRectangle = FakeRect(1120, 200, 1180, 260)
            controls.extend([member_a, member_b])

        member_list = FakePickerSearchControl(
            True,
            name="请勾选需要移出的群成员",
            control_type="ListControl",
            automation_id="sp_to_select_contact_list",
            children=controls,
        )
        picker = FakePickerForVisibleResult(controls[0], contact_list=member_list)

        members = service._collect_picker_contact_checkboxes(picker)

        self.assertEqual([member.Name for member in members], ["日乌", "科学-陈老师"])

    def test_clicks_search_result_checkbox_from_new_chat_result_list(self):
        checkbox_result = FakePickerSearchControl(
            True,
            name="科学-陈老师",
            control_type="CheckBoxControl",
            class_name="mmui::SearchContactCellView",
        )
        result_list = FakePickerSearchControl(
            True,
            name="请勾选需要添加的联系人",
            control_type="ListControl",
            automation_id="sp_search_new_chat_result_list",
            children=[checkbox_result],
        )
        picker = FakePickerForVisibleResult(checkbox_result, contact_list=result_list)
        clicked = []

        with (
            patch.object(service, "_click", lambda control: clicked.append(control)),
            patch.object(service.auto, "SendKeys", lambda _keys: None),
            patch.object(service.pyperclip, "copy", lambda _text: None),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            thread = SimpleNamespace(lc=SimpleNamespace(search="搜索"))
            service.GroupManagerThread._picker_search_and_check(
                thread, picker, "科学-陈老师"
            )

        self.assertIn(checkbox_result, clicked)

    def test_clicks_visible_search_result_when_contact_list_control_is_missing(self):
        visible_result = FakePickerSearchControl(
            True, name="科学-陈老师", control_type="ListItemControl"
        )
        picker = FakePickerForVisibleResult(visible_result)
        clicked = []

        with (
            patch.object(service, "_click", lambda control: clicked.append(control)),
            patch.object(service.auto, "SendKeys", lambda _keys: None),
            patch.object(service.pyperclip, "copy", lambda _text: None),
            patch.object(service.time, "sleep", lambda _seconds: None),
        ):
            thread = SimpleNamespace(lc=SimpleNamespace(search="搜索"))
            service.GroupManagerThread._picker_search_and_check(
                thread, picker, "科学-陈老师"
            )

        self.assertIn(visible_result, clicked)


if __name__ == "__main__":
    unittest.main()
