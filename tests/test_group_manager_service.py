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

    def Exists(self, *args, **kwargs):
        return self._exists


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

    def GetChildren(self):
        return self.children


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


class PickerSearchResultClickTest(unittest.TestCase):
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
