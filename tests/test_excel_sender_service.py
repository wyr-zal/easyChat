from __future__ import annotations

import sys
import types
import unittest
from contextlib import contextmanager
from unittest import mock

from excel_sender_service import PersonalizedSendThread


@contextmanager
def _fake_uiautomation_initializer():
    yield


class _FakeSenderService:
    def __init__(self, *, thread_ref: PersonalizedSendThread, **_kwargs) -> None:
        self.thread_ref = thread_ref
        self.prepared_targets: list[tuple[str, str, bool]] = []
        self.text_targets: list[str] = []
        self.file_targets: list[tuple[str, str]] = []
        self.report_targets: list[str] = []

    def prepare_text_message(self, wechat_id: str, message: str, search_user: bool = True) -> None:
        self.prepared_targets.append((wechat_id, message, search_user))

    def send_text_message(self, wechat_id: str, message: str, search_user: bool = True) -> None:
        _ = message, search_user
        self.text_targets.append(wechat_id)
        if wechat_id == "reporter":
            self.report_targets.append(wechat_id)

    def send_file(self, wechat_id: str, path: str, search_user: bool = True) -> None:
        _ = search_user
        self.file_targets.append((wechat_id, path))
        if len(self.file_targets) == 1:
            self.thread_ref.request_stop()


class _FailOnTargetSenderService(_FakeSenderService):
    def send_text_message(self, wechat_id: str, message: str, search_user: bool = True) -> None:
        super().send_text_message(wechat_id, message, search_user=search_user)
        if wechat_id == "李四":
            raise RuntimeError("模拟发送失败")


class ExcelSenderServiceStopTests(unittest.TestCase):
    def test_debug_mode_prefills_draft_but_skips_real_text_attachment_and_auto_report(self) -> None:
        summary_holder: dict = {}
        thread = PersonalizedSendThread(
            records=[
                {
                    "__target_value": "张三",
                    "message_mode": "custom",
                    "message": "您好",
                    "attachment_mode": "custom",
                    "attachments": [
                        {"file_path": "a.pdf", "file_type": "pdf"},
                        {"file_path": "b.pdf", "file_type": "pdf"},
                    ],
                }
            ],
            template="",
            interval_seconds=0,
            target_column="__target_value",
            auto_report=True,
            report_to="reporter",
            debug_mode=True,
            summary_callback=lambda summary: summary_holder.update(summary),
        )

        sender_holder: dict[str, _FakeSenderService] = {}

        def _sender_factory(*args, **kwargs):
            _ = args, kwargs
            sender = _FakeSenderService(thread_ref=thread)
            sender_holder["sender"] = sender
            return sender

        fake_auto = types.SimpleNamespace(
            UIAutomationInitializerInThread=_fake_uiautomation_initializer
        )

        with mock.patch("excel_sender_service.WeChatSenderService", side_effect=_sender_factory), \
             mock.patch.dict(sys.modules, {"uiautomation": fake_auto}):
            thread.run()

        sender = sender_holder["sender"]
        self.assertEqual(sender.prepared_targets, [("张三", "您好", True)])
        self.assertEqual(sender.text_targets, [])
        self.assertEqual(sender.file_targets, [])
        self.assertEqual(sender.report_targets, [])
        self.assertTrue(summary_holder.get("debug_mode"))
        self.assertEqual(summary_holder.get("sent"), 0)
        self.assertEqual(summary_holder.get("failed"), 0)
        self.assertEqual(summary_holder.get("skipped"), 1)
        self.assertEqual(summary_holder.get("attachments_sent"), 0)
        self.assertEqual(summary_holder.get("attachments_failed"), 0)
        target = summary_holder["targets"][0]
        self.assertTrue(target["debug_prepared"])
        self.assertEqual(target["send_status"], "skipped")
        self.assertEqual(target["text_status"], "skipped")
        self.assertEqual(target["attachment_status"], "skipped")
        self.assertTrue(all(item["attachment_status"] == "skipped" for item in target["attachments"]))
        self.assertIn("预填", target["error_msg"])

    def test_stop_request_interrupts_remaining_attachments_and_skips_auto_report(self) -> None:
        summary_holder: dict = {}
        thread = PersonalizedSendThread(
            records=[
                {
                    "__target_value": "张三",
                    "message_mode": "custom",
                    "message": "您好",
                    "attachment_mode": "custom",
                    "attachments": [
                        {"file_path": "a.pdf", "file_type": "pdf"},
                        {"file_path": "b.pdf", "file_type": "pdf"},
                    ],
                }
            ],
            template="",
            interval_seconds=0,
            target_column="__target_value",
            auto_report=True,
            report_to="reporter",
            summary_callback=lambda summary: summary_holder.update(summary),
        )

        sender_holder: dict[str, _FakeSenderService] = {}

        def _sender_factory(*args, **kwargs):
            _ = args, kwargs
            sender = _FakeSenderService(thread_ref=thread)
            sender_holder["sender"] = sender
            return sender

        fake_auto = types.SimpleNamespace(
            UIAutomationInitializerInThread=_fake_uiautomation_initializer
        )

        with mock.patch("excel_sender_service.WeChatSenderService", side_effect=_sender_factory), \
             mock.patch.dict(sys.modules, {"uiautomation": fake_auto}):
            thread.run()

        sender = sender_holder["sender"]
        self.assertEqual(sender.text_targets, ["张三"])
        self.assertEqual(sender.file_targets, [("张三", "a.pdf")])
        self.assertEqual(sender.report_targets, [])
        self.assertTrue(summary_holder.get("stopped"))
        self.assertEqual(summary_holder.get("attachments_sent"), 1)
        self.assertEqual(summary_holder.get("attachments_failed"), 1)

    def test_send_error_stops_following_targets_immediately(self) -> None:
        summary_holder: dict = {}
        thread = PersonalizedSendThread(
            records=[
                {"__target_value": "张三", "message_mode": "custom", "message": "您好"},
                {"__target_value": "李四", "message_mode": "custom", "message": "您好"},
                {"__target_value": "王五", "message_mode": "custom", "message": "您好"},
            ],
            template="",
            interval_seconds=0,
            target_column="__target_value",
            auto_report=False,
            summary_callback=lambda summary: summary_holder.update(summary),
        )

        sender_holder: dict[str, _FailOnTargetSenderService] = {}

        def _sender_factory(*args, **kwargs):
            _ = args, kwargs
            sender = _FailOnTargetSenderService(thread_ref=thread)
            sender_holder["sender"] = sender
            return sender

        fake_auto = types.SimpleNamespace(
            UIAutomationInitializerInThread=_fake_uiautomation_initializer
        )

        with mock.patch("excel_sender_service.WeChatSenderService", side_effect=_sender_factory), \
             mock.patch.dict(sys.modules, {"uiautomation": fake_auto}):
            thread.run()

        sender = sender_holder["sender"]
        self.assertEqual(sender.text_targets, ["张三", "李四"])
        self.assertTrue(summary_holder.get("stopped"))
        self.assertTrue(summary_holder.get("stopped_by_error"))
        self.assertEqual(summary_holder.get("failed"), 1)
        self.assertEqual(summary_holder.get("sent"), 1)
        self.assertEqual(summary_holder.get("interrupted_target"), "李四")
        self.assertEqual(summary_holder.get("interrupted_index"), 2)


if __name__ == "__main__":
    unittest.main()
