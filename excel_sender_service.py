import time

from PyQt5.QtCore import QThread, pyqtSignal

from excel_reader import DEFAULT_SEND_TARGET_COLUMN
from excel_template import render_template
from ui_auto_wechat import WeChat


CUSTOM_MESSAGE_OVERRIDE_KEY = "__custom_message_override"


class WeChatSenderService:
    def __init__(self, path: str = "", locale: str = "zh-CN") -> None:
        self.wechat = WeChat(path=path, locale=locale)

    def send_text_message(self, wechat_id: str, message: str) -> None:
        self.wechat.send_text_message(wechat_id, message)


class PersonalizedSendThread(QThread):
    progress = pyqtSignal(int, int, str)
    log = pyqtSignal(str)
    completed = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(
        self,
        records: list[dict[str, str]],
        template: str,
        interval_seconds: int,
        target_column: str = DEFAULT_SEND_TARGET_COLUMN,
        locale: str = "zh-CN",
        wechat_path: str = "",
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.records = records
        self.template = template
        self.interval_seconds = interval_seconds
        self.target_column = target_column.strip() or DEFAULT_SEND_TARGET_COLUMN
        self.locale = locale
        self.wechat_path = wechat_path
        self._stop_requested = False

    def request_stop(self) -> None:
        self._stop_requested = True

    def run(self) -> None:
        import uiautomation as auto

        total = len(self.records)
        summary = {
            "total": total,
            "sent": 0,
            "failed": 0,
            "skipped": 0,
            "stopped": False,
        }

        try:
            sender = WeChatSenderService(path=self.wechat_path, locale=self.locale)
            with auto.UIAutomationInitializerInThread():
                for index, row in enumerate(self.records, start=1):
                    if self._stop_requested:
                        summary["stopped"] = True
                        self.log.emit("用户已停止发送。")
                        break

                    target_value = (row.get(self.target_column) or "").strip()
                    if target_value == "":
                        summary["skipped"] += 1
                        self.log.emit(f"[{index}/{total}] 跳过：{self.target_column} 为空。")
                        continue

                    if CUSTOM_MESSAGE_OVERRIDE_KEY in row:
                        message = str(row.get(CUSTOM_MESSAGE_OVERRIDE_KEY, ""))
                    else:
                        message = render_template(self.template, row)
                    if message.strip() == "":
                        summary["skipped"] += 1
                        self.log.emit(f"[{index}/{total}] 跳过：{target_value} 的消息内容为空。")
                        continue

                    try:
                        sender.send_text_message(target_value, message)
                        summary["sent"] += 1
                        self.progress.emit(index, total, target_value)
                        self.log.emit(f"[{index}/{total}] 已发送：{target_value}")
                    except Exception as exc:
                        summary["failed"] += 1
                        self.log.emit(f"[{index}/{total}] 发送失败：{target_value}，错误：{exc}")

                    if index < total:
                        self._sleep_with_stop_check(self.interval_seconds)
                        if self._stop_requested:
                            summary["stopped"] = True
                            self.log.emit("用户已停止发送。")
                            break

        except Exception as exc:
            self.error.emit(str(exc))

        self.completed.emit(summary)

    def _sleep_with_stop_check(self, seconds: int) -> None:
        if seconds <= 0:
            return

        checks = seconds * 10
        for _ in range(checks):
            if self._stop_requested:
                return
            time.sleep(0.1)
