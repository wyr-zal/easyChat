import random
import time
from datetime import datetime

from PyQt5.QtCore import QThread, pyqtSignal

from excel_reader import DEFAULT_SEND_TARGET_COLUMN
from excel_template import render_template
from ui_auto_wechat import WeChat


CUSTOM_MESSAGE_OVERRIDE_KEY = "__custom_message_override"
DEFAULT_REPORT_TARGET = "C19881212"


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
        random_delay_min: int = 30,
        random_delay_max: int = 180,
        random_delay_count_min: int = 1,
        random_delay_count_max: int = 3,
        operator_name: str = "",
        report_to: str = DEFAULT_REPORT_TARGET,
        auto_report: bool = True,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.records = records
        self.template = template
        self.interval_seconds = interval_seconds
        self.target_column = target_column.strip() or DEFAULT_SEND_TARGET_COLUMN
        self.locale = locale
        self.wechat_path = wechat_path
        self.random_delay_min = max(0, int(random_delay_min))
        self.random_delay_max = max(0, int(random_delay_max))
        self.random_delay_count_min = max(0, int(random_delay_count_min))
        self.random_delay_count_max = max(0, int(random_delay_count_max))
        self.operator_name = operator_name.strip()
        self.report_to = report_to.strip() or DEFAULT_REPORT_TARGET
        self.auto_report = auto_report
        self._stop_requested = False

    def request_stop(self) -> None:
        self._stop_requested = True

    def run(self) -> None:
        import uiautomation as auto

        start_time = datetime.now()
        total = len(self.records)
        summary = {
            "total": total,
            "sent": 0,
            "failed": 0,
            "skipped": 0,
            "stopped": False,
            "error": "",
            "started_at": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": "",
            "elapsed": "",
            "message_preview": self._build_message_preview(),
            "report_sent": False,
            "report_error": "",
            "random_delay_count": 0,
        }

        sender: WeChatSenderService | None = None
        try:
            sender = WeChatSenderService(path=self.wechat_path, locale=self.locale)
            delay_plan = self._build_random_delay_plan(total)
            summary["random_delay_count"] = len(delay_plan)
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

                        extra_delay = delay_plan.get(index)
                        if extra_delay:
                            self.log.emit(f"[{index}/{total}] 随机延迟事务：等待 {extra_delay} 秒。")
                            self._sleep_with_stop_check(extra_delay)
                            if self._stop_requested:
                                summary["stopped"] = True
                                self.log.emit("用户已停止发送。")
                                break

        except Exception as exc:
            summary["error"] = str(exc)
            self.error.emit(str(exc))

        finished_at = datetime.now()
        summary["finished_at"] = finished_at.strftime("%Y-%m-%d %H:%M:%S")
        summary["elapsed"] = str(finished_at - start_time).split(".")[0]

        if self.auto_report and sender is not None and self.report_to:
            try:
                sender.send_text_message(self.report_to, self._build_report_text(summary))
                summary["report_sent"] = True
                self.log.emit(f"任务结束后已自动向 {self.report_to} 发送汇报。")
            except Exception as exc:
                summary["report_error"] = str(exc)
                self.log.emit(f"自动汇报失败：{exc}")

        self.completed.emit(summary)

    def _sleep_with_stop_check(self, seconds: int) -> None:
        if seconds <= 0:
            return

        checks = seconds * 10
        for _ in range(checks):
            if self._stop_requested:
                return
            time.sleep(0.1)

    def _build_random_delay_plan(self, total: int) -> dict[int, int]:
        if total <= 1:
            return {}
        if self.random_delay_max <= 0:
            return {}

        delay_count_min = min(self.random_delay_count_min, total - 1)
        delay_count_max = min(max(delay_count_min, self.random_delay_count_max), total - 1)
        if delay_count_max <= 0:
            return {}

        delay_count = random.randint(delay_count_min, delay_count_max)
        positions = random.sample(range(1, total), k=delay_count)
        delay_low = min(self.random_delay_min, self.random_delay_max)
        delay_high = max(self.random_delay_min, self.random_delay_max)
        return {
            position: random.randint(delay_low, delay_high)
            for position in positions
        }

    def _build_message_preview(self) -> str:
        for row in self.records:
            if CUSTOM_MESSAGE_OVERRIDE_KEY in row:
                preview = str(row.get(CUSTOM_MESSAGE_OVERRIDE_KEY, "")).strip()
            else:
                preview = render_template(self.template, row).strip()
            if preview:
                return preview[:50]
        return self.template.strip()[:50]

    def _build_report_text(self, summary: dict) -> str:
        preview = summary.get("message_preview", "")
        if preview:
            preview = preview[:30]
            if len(summary.get("message_preview", "")) > 30:
                preview += "..."
        else:
            preview = "无"

        operator_name = self.operator_name or "未知"
        return "\n".join(
            [
                "【群发汇报】",
                f"操作人：{operator_name}",
                f"发送总人数：{summary['total']}",
                f"成功：{summary['sent']} / 失败：{summary['failed']} / 跳过：{summary['skipped']}",
                f"内容摘要：{preview}",
                f"执行时间：{summary['started_at']} ~ {summary['finished_at']}（历时{summary['elapsed']}）",
            ]
        )
