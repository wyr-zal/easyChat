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
    def __init__(self, exists=True, name="", control_type="", children=None):
        super().__init__(exists)
        self.Name = name
        self.ControlTypeName = control_type
        self.children = children or []

    def GetChildren(self):
        return self.children


class FakeSearchEdit(FakePickerSearchControl):
    pass


class FakePickerForVisibleResult(FakePickerSearchControl):
    def __init__(self, visible_result):
        super().__init__(True, name="微信发起群聊", control_type="WindowControl")
        self.search_edit = FakeSearchEdit(True, name="搜索", control_type="EditControl")
        self.visible_result = visible_result
        self.missing_contact_list = FakePickerSearchControl(False)
        self.children = [visible_result]

    def EditControl(self, **_kwargs):
        return self.search_edit

    def ListControl(self, **_kwargs):
        return self.missing_contact_list


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


class PickerSearchResultClickTest(unittest.TestCase):
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
