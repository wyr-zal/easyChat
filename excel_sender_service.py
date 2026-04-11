import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

from PyQt5.QtCore import QThread, pyqtSignal

from excel_reader import DEFAULT_SEND_TARGET_COLUMN
from excel_template import render_template
from ui_auto_wechat import WeChat


CUSTOM_MESSAGE_OVERRIDE_KEY = "__custom_message_override"
DEFAULT_REPORT_TARGET = "C19881212"

MESSAGE_MODE_TEMPLATE = "template"
MESSAGE_MODE_CUSTOM = "custom"
ATTACHMENT_MODE_COMMON = "common"
ATTACHMENT_MODE_CUSTOM = "custom"

TEXT_STATUS_SUCCESS = "success"
TEXT_STATUS_FAILED = "failed"
TEXT_STATUS_SKIPPED = "skipped"

ATTACHMENT_STATUS_SUCCESS = "success"
ATTACHMENT_STATUS_FAILED = "failed"
ATTACHMENT_STATUS_SKIPPED = "skipped"

SEND_STATUS_SUCCESS = "success"
SEND_STATUS_PARTIAL_SUCCESS = "partial_success"
SEND_STATUS_FAILED = "failed"
SEND_STATUS_SKIPPED = "skipped"


class WeChatSenderService:
    def __init__(self, path: str = "", locale: str = "zh-CN") -> None:
        self.wechat = WeChat(path=path, locale=locale)

    def prepare_text_message(self, wechat_id: str, message: str, search_user: bool = True) -> None:
        self.wechat.prepare_text_message(wechat_id, message, search_user=search_user)

    def send_text_message(self, wechat_id: str, message: str, search_user: bool = True) -> None:
        self.wechat.send_text_message(wechat_id, message, search_user=search_user)

    def send_file(self, wechat_id: str, path: str, search_user: bool = True) -> None:
        self.wechat.send_file(wechat_id, path, search_user=search_user)

    def send_files(
        self,
        wechat_id: str,
        paths: list[str],
        search_user: bool = True,
        inter_file_delay: float = 0.2,
    ) -> None:
        self.wechat.send_files(
            wechat_id,
            paths=paths,
            search_user=search_user,
            inter_file_delay=inter_file_delay,
        )


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
        debug_mode: bool = False,
        common_attachments: list | None = None,
        target_result_callback: Callable[[dict], None] | None = None,
        target_log_callback: Callable[[str, dict], None] | None = None,
        summary_callback: Callable[[dict], None] | None = None,
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
        self.debug_mode = bool(debug_mode)
        self.common_attachments = self._normalize_attachment_items(common_attachments or [])
        self.target_result_callback = target_result_callback
        self.target_log_callback = target_log_callback
        self.summary_callback = summary_callback
        self._stop_requested = False

    def request_stop(self) -> None:
        self._stop_requested = True

    def run(self) -> None:
        start_time = datetime.now()
        total = len(self.records)
        summary = {
            "total": total,
            "sent": 0,
            "failed": 0,
            "skipped": 0,
            "stopped": False,
            "stopped_by_error": False,
            "error": "",
            "started_at": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": "",
            "elapsed": "",
            "message_preview": self._build_message_preview(),
            "report_sent": False,
            "report_error": "",
            "random_delay_count": 0,
            "attachments_sent": 0,
            "attachments_failed": 0,
            "debug_mode": self.debug_mode,
            "targets": [],
            "interrupted_target": "",
            "interrupted_index": 0,
        }

        sender: WeChatSenderService | None = None
        try:
            delay_plan = self._build_random_delay_plan(total)
            summary["random_delay_count"] = len(delay_plan)
            import uiautomation as auto

            sender = WeChatSenderService(path=self.wechat_path, locale=self.locale)
            if self.debug_mode:
                self._emit_log("调试模式已开启：本次会自动定位联系人并预填消息草稿，但不会按回车发送；附件与自动汇报也不会真实发送。")

            with auto.UIAutomationInitializerInThread():
                for index, row in enumerate(self.records, start=1):
                    if self._stop_requested:
                        summary["stopped"] = True
                        self._emit_log("用户已停止发送。")
                        break

                    row_result = self._execute_single_target(
                        sender=sender,
                        row=row,
                        index=index,
                        total=total,
                    )
                    stopped_after_target = bool(row_result.pop("_stop_requested", False))
                    abort_remaining = bool(row_result.pop("_abort_remaining", False))
                    summary["targets"].append(row_result)
                    summary["attachments_sent"] += int(row_result.get("attachment_sent_count", 0))
                    summary["attachments_failed"] += int(row_result.get("attachment_failed_count", 0))

                    send_status = str(row_result.get("send_status") or "")
                    if send_status == SEND_STATUS_SUCCESS:
                        summary["sent"] += 1
                    elif send_status == SEND_STATUS_SKIPPED:
                        summary["skipped"] += 1
                    else:
                        summary["failed"] += 1

                    self.progress.emit(index, total, str(row_result.get("target_value") or ""))
                    self._safe_target_result_callback(row_result)

                    if abort_remaining:
                        summary["stopped"] = True
                        summary["stopped_by_error"] = True
                        summary["interrupted_target"] = str(row_result.get("target_value") or "")
                        summary["interrupted_index"] = index
                        self._emit_log(f"检测到发送异常，任务已在 {summary['interrupted_target'] or f'第{index}项'} 后立即停止。")
                        break

                    if stopped_after_target:
                        summary["stopped"] = True
                        self._emit_log("用户已停止发送。")
                        break

                    if index < total:
                        self._sleep_with_stop_check(self.interval_seconds)
                        if self._stop_requested:
                            summary["stopped"] = True
                            self._emit_log("用户已停止发送。")
                            break

                        extra_delay = delay_plan.get(index)
                        if extra_delay:
                            self._emit_log(f"[{index}/{total}] 随机延迟事务：等待 {extra_delay} 秒。")
                            self._sleep_with_stop_check(extra_delay)
                            if self._stop_requested:
                                summary["stopped"] = True
                                self._emit_log("用户已停止发送。")
                                break

        except Exception as exc:
            summary["error"] = str(exc)
            self.error.emit(str(exc))

        finished_at = datetime.now()
        summary["finished_at"] = finished_at.strftime("%Y-%m-%d %H:%M:%S")
        summary["elapsed"] = str(finished_at - start_time).split(".")[0]

        if self.debug_mode and self.auto_report and (not summary["stopped"]) and self.report_to:
            self._emit_log(f"调试模式：已跳过向 {self.report_to} 的自动汇报发送。")
        elif self.auto_report and (not summary["stopped"]) and sender is not None and self.report_to:
            try:
                sender.send_text_message(
                    self.report_to,
                    self._build_report_text(summary),
                    search_user=True,
                )
                summary["report_sent"] = True
                self._emit_log(f"任务结束后已自动向 {self.report_to} 发送汇报。")
            except Exception as exc:
                summary["report_error"] = str(exc)
                self._emit_log(f"自动汇报失败：{exc}")

        self._safe_summary_callback(summary)
        self.completed.emit(summary)

    def _execute_single_target(
        self,
        *,
        sender: WeChatSenderService,
        row: dict[str, str],
        index: int,
        total: int,
    ) -> dict:
        started_at = datetime.now()
        target_value = (row.get(self.target_column) or "").strip()
        target_type = str(row.get("target_type") or row.get("__target_type") or "").strip()

        message, message_mode = self._resolve_message_and_mode(row)
        attachments, attachment_mode = self._resolve_attachments(row)

        row_result = {
            "index": index,
            "total": total,
            "target_value": target_value,
            "target_type": target_type,
            "message_mode": message_mode,
            "message": message,
            "attachment_mode": attachment_mode,
            "attachments": [],
            "attachment_status": ATTACHMENT_STATUS_SKIPPED,
            "attachment_sent_count": 0,
            "attachment_failed_count": 0,
            "text_status": TEXT_STATUS_SKIPPED,
            "send_status": SEND_STATUS_SKIPPED,
            "debug_prepared": False,
            "error_msg": "",
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "send_time": "",
            "elapsed_seconds": 0.0,
        }

        if target_value == "":
            row_result["error_msg"] = f"{self.target_column} 为空"
            self._emit_log(f"[{index}/{total}] 跳过：{self.target_column} 为空。", row_result)
            return self._finalize_row_result(row_result, started_at)

        has_message = bool(message.strip())
        has_attachments = bool(attachments)
        debug_skip_message = "调试模式：未实际发送"

        if not has_message and not has_attachments:
            row_result["error_msg"] = "消息与附件均为空"
            self._emit_log(f"[{index}/{total}] 跳过：{target_value} 的消息与附件均为空。", row_result)
            return self._finalize_row_result(row_result, started_at)

        text_error = ""
        if self.debug_mode:
            try:
                sender.prepare_text_message(
                    target_value,
                    message if has_message else "",
                    search_user=True,
                )
                row_result["text_status"] = TEXT_STATUS_SKIPPED
                row_result["debug_prepared"] = True
            except Exception as exc:
                text_error = str(exc)
                row_result["text_status"] = TEXT_STATUS_FAILED
                row_result["error_msg"] = text_error
                self._emit_log(f"[{index}/{total}] 调试预填失败：{target_value}，错误：{exc}", row_result)
        elif has_message:
            try:
                sender.send_text_message(target_value, message, search_user=True)
                row_result["text_status"] = TEXT_STATUS_SUCCESS
            except Exception as exc:
                text_error = str(exc)
                row_result["text_status"] = TEXT_STATUS_FAILED
                row_result["error_msg"] = text_error
                self._emit_log(f"[{index}/{total}] 文本发送失败：{target_value}，错误：{exc}", row_result)
        else:
            row_result["text_status"] = TEXT_STATUS_SKIPPED

        attachment_results: list[dict] = []
        attachment_sent_count = 0
        attachment_failed_count = 0
        attachment_error = ""
        stop_error = "用户手动停止，未继续发送剩余附件。"
        debug_detail_message = ""

        if attachments and row_result["text_status"] != TEXT_STATUS_FAILED:
            if self.debug_mode:
                for attachment in attachments:
                    attachment_results.append(
                        {
                            "file_path": str(attachment.get("file_path") or ""),
                            "file_type": str(attachment.get("file_type") or ""),
                            "attachment_status": ATTACHMENT_STATUS_SKIPPED,
                            "error_msg": debug_skip_message,
                        }
                    )
                attachment_error = debug_skip_message
                debug_detail_message = "调试模式：已定位联系人并预填文本草稿，附件未实际发送。"
            elif self._stop_requested:
                attachment_failed_count += self._append_interrupted_attachments(
                    attachment_results,
                    attachments,
                    error_message=stop_error,
                )
                attachment_error = stop_error
                row_result["_stop_requested"] = True
            else:
                for attachment_index, attachment in enumerate(attachments, start=1):
                    if self._stop_requested:
                        attachment_failed_count += self._append_interrupted_attachments(
                            attachment_results,
                            attachments[attachment_index - 1 :],
                            error_message=stop_error,
                        )
                        attachment_error = attachment_error or stop_error
                        row_result["_stop_requested"] = True
                        break

                    file_path = str(attachment.get("file_path") or "")
                    file_type = str(attachment.get("file_type") or "")
                    attachment_item = {
                        "file_path": file_path,
                        "file_type": file_type,
                        "attachment_status": ATTACHMENT_STATUS_SKIPPED,
                        "error_msg": "",
                    }
                    try:
                        sender.send_file(
                            target_value,
                            file_path,
                            search_user=(not has_message and attachment_index == 1),
                        )
                        attachment_item["attachment_status"] = ATTACHMENT_STATUS_SUCCESS
                        attachment_sent_count += 1
                    except Exception as exc:
                        attachment_item["attachment_status"] = ATTACHMENT_STATUS_FAILED
                        attachment_item["error_msg"] = str(exc)
                        attachment_failed_count += 1
                        if attachment_error == "":
                            attachment_error = str(exc)
                    attachment_results.append(attachment_item)

        elif attachments and row_result["text_status"] == TEXT_STATUS_FAILED:
            attachment_skip_reason = "调试预填失败，未执行附件发送。" if self.debug_mode else "文本发送失败，未执行附件发送。"
            for attachment in attachments:
                attachment_results.append(
                    {
                        "file_path": str(attachment.get("file_path") or ""),
                        "file_type": str(attachment.get("file_type") or ""),
                        "attachment_status": ATTACHMENT_STATUS_SKIPPED,
                        "error_msg": attachment_skip_reason,
                    }
                )

        row_result["attachments"] = attachment_results
        row_result["attachment_sent_count"] = attachment_sent_count
        row_result["attachment_failed_count"] = attachment_failed_count
        row_result["attachment_status"] = self._resolve_attachment_status(
            has_attachments=has_attachments,
            sent_count=attachment_sent_count,
            failed_count=attachment_failed_count,
        )

        row_result["send_status"] = self._resolve_send_status(
            text_status=str(row_result["text_status"]),
            attachment_status=str(row_result["attachment_status"]),
            has_message=has_message,
            has_attachments=has_attachments,
        )
        row_result["error_msg"] = row_result["error_msg"] or attachment_error or text_error
        if self.debug_mode and row_result["send_status"] == SEND_STATUS_SKIPPED:
            desired_debug_message = debug_skip_message
            if has_message and has_attachments:
                desired_debug_message = debug_detail_message
            elif has_message:
                desired_debug_message = "调试模式：已定位联系人并预填文本草稿，未按回车发送。"
            elif has_attachments and row_result.get("debug_prepared"):
                desired_debug_message = "调试模式：已定位联系人，但附件未实际发送。"
            if row_result["error_msg"] in {"", debug_skip_message}:
                row_result["error_msg"] = desired_debug_message
        if self._stop_requested:
            row_result["_stop_requested"] = True
        elif row_result["send_status"] in {SEND_STATUS_FAILED, SEND_STATUS_PARTIAL_SUCCESS}:
            row_result["_abort_remaining"] = True

        status_text = self._build_target_status_text(row_result)
        self._emit_log(f"[{index}/{total}] {status_text}：{target_value}", row_result)

        return self._finalize_row_result(row_result, started_at)

    def _resolve_message_and_mode(self, row: dict[str, str]) -> tuple[str, str]:
        explicit_mode = str(row.get("message_mode") or "").strip().lower()

        if CUSTOM_MESSAGE_OVERRIDE_KEY in row:
            return str(row.get(CUSTOM_MESSAGE_OVERRIDE_KEY, "")), MESSAGE_MODE_CUSTOM

        if explicit_mode == MESSAGE_MODE_CUSTOM:
            return str(row.get("message") or ""), MESSAGE_MODE_CUSTOM

        if explicit_mode == MESSAGE_MODE_TEMPLATE:
            return render_template(self.template, row), MESSAGE_MODE_TEMPLATE

        if self.template.strip():
            return render_template(self.template, row), MESSAGE_MODE_TEMPLATE

        fallback_message = str(row.get("message") or "")
        if fallback_message.strip():
            return fallback_message, MESSAGE_MODE_CUSTOM
        return "", MESSAGE_MODE_TEMPLATE

    def _resolve_attachments(self, row: dict[str, str]) -> tuple[list[dict], str]:
        explicit_mode = str(row.get("attachment_mode") or "").strip().lower()
        custom_attachments = self._normalize_attachment_items(self._extract_custom_attachment_source(row))

        if explicit_mode == ATTACHMENT_MODE_CUSTOM:
            return custom_attachments, ATTACHMENT_MODE_CUSTOM

        if explicit_mode == ATTACHMENT_MODE_COMMON:
            return [dict(item) for item in self.common_attachments], ATTACHMENT_MODE_COMMON

        if custom_attachments:
            return custom_attachments, ATTACHMENT_MODE_CUSTOM

        return [dict(item) for item in self.common_attachments], ATTACHMENT_MODE_COMMON

    def _extract_custom_attachment_source(self, row: dict[str, str]):
        if "attachments" in row:
            return row.get("attachments")
        if "custom_attachments" in row:
            return row.get("custom_attachments")
        if "__attachments" in row:
            return row.get("__attachments")
        return []

    def _normalize_attachment_items(self, raw_items) -> list[dict]:
        normalized: list[dict] = []
        items = self._load_attachment_items(raw_items)
        for item in items:
            normalized_item = self._normalize_attachment_item(item)
            if normalized_item is not None:
                normalized.append(normalized_item)
        return normalized

    def _load_attachment_items(self, raw_items):
        if raw_items is None:
            return []
        if isinstance(raw_items, (list, tuple)):
            return list(raw_items)
        if isinstance(raw_items, dict):
            return [raw_items]
        if isinstance(raw_items, str):
            raw_text = raw_items.strip()
            if raw_text == "":
                return []
            if raw_text.startswith("[") or raw_text.startswith("{"):
                try:
                    loaded = json.loads(raw_text)
                    if isinstance(loaded, list):
                        return loaded
                    if isinstance(loaded, dict):
                        return [loaded]
                except json.JSONDecodeError:
                    pass
            if ";" in raw_text:
                return [segment.strip() for segment in raw_text.split(";") if segment.strip()]
            return [raw_text]
        return []

    def _normalize_attachment_item(self, item) -> dict | None:
        if isinstance(item, str):
            file_path = item.strip()
            if file_path == "":
                return None
            return {
                "file_path": file_path,
                "file_type": self._infer_file_type(file_path),
            }

        if isinstance(item, dict):
            file_path = str(item.get("file_path") or item.get("path") or "").strip()
            if file_path == "":
                return None
            file_type = str(item.get("file_type") or "").strip().lower() or self._infer_file_type(file_path)
            return {
                "file_path": file_path,
                "file_type": file_type,
            }

        return None

    def _infer_file_type(self, file_path: str) -> str:
        suffix = Path(file_path).suffix.lower()
        if suffix == ".pdf":
            return "pdf"
        if suffix in {".jpg", ".jpeg", ".png", ".bmp", ".webp"}:
            return "image"
        return suffix.lstrip(".")

    def _resolve_attachment_status(
        self,
        *,
        has_attachments: bool,
        sent_count: int,
        failed_count: int,
    ) -> str:
        if not has_attachments:
            return ATTACHMENT_STATUS_SKIPPED
        if failed_count > 0:
            return ATTACHMENT_STATUS_FAILED
        if sent_count > 0:
            return ATTACHMENT_STATUS_SUCCESS
        return ATTACHMENT_STATUS_SKIPPED

    def _resolve_send_status(
        self,
        *,
        text_status: str,
        attachment_status: str,
        has_message: bool,
        has_attachments: bool,
    ) -> str:
        if text_status == TEXT_STATUS_FAILED:
            return SEND_STATUS_FAILED
        if attachment_status == ATTACHMENT_STATUS_FAILED:
            if text_status == TEXT_STATUS_SUCCESS:
                return SEND_STATUS_PARTIAL_SUCCESS
            return SEND_STATUS_FAILED
        if text_status == TEXT_STATUS_SUCCESS or attachment_status == ATTACHMENT_STATUS_SUCCESS:
            return SEND_STATUS_SUCCESS
        if not has_message and not has_attachments:
            return SEND_STATUS_SKIPPED
        return SEND_STATUS_SKIPPED

    def _build_target_status_text(self, row_result: dict) -> str:
        send_status = str(row_result.get("send_status") or "")
        text_status = str(row_result.get("text_status") or "")
        attachment_status = str(row_result.get("attachment_status") or "")
        if send_status == SEND_STATUS_SUCCESS:
            if attachment_status == ATTACHMENT_STATUS_SUCCESS and text_status == TEXT_STATUS_SUCCESS:
                return "文本+附件发送成功"
            if attachment_status == ATTACHMENT_STATUS_SUCCESS:
                return "附件发送成功"
            return "文本发送成功"
        if send_status == SEND_STATUS_PARTIAL_SUCCESS:
            return "文本成功，附件失败"
        if send_status == SEND_STATUS_SKIPPED:
            if self.debug_mode and bool(row_result.get("debug_prepared")):
                return "调试预填完成（未发送）"
            return "已跳过"
        return "发送失败"

    def _append_interrupted_attachments(
        self,
        attachment_results: list[dict],
        attachments: list[dict],
        *,
        error_message: str,
    ) -> int:
        count = 0
        for attachment in attachments:
            attachment_results.append(
                {
                    "file_path": str(attachment.get("file_path") or ""),
                    "file_type": str(attachment.get("file_type") or ""),
                    "attachment_status": ATTACHMENT_STATUS_FAILED,
                    "error_msg": error_message,
                }
            )
            count += 1
        return count

    def _finalize_row_result(self, row_result: dict, started_at: datetime) -> dict:
        finished_at = datetime.now()
        row_result["send_time"] = finished_at.strftime("%Y-%m-%d %H:%M:%S")
        row_result["elapsed_seconds"] = round((finished_at - started_at).total_seconds(), 3)
        return row_result

    def _emit_log(self, message: str, row_result: dict | None = None) -> None:
        self.log.emit(message)
        if self.target_log_callback is None:
            return
        try:
            payload = dict(row_result or {})
            self.target_log_callback(message, payload)
        except Exception as callback_exc:
            self.log.emit(f"日志回调执行失败：{callback_exc}")

    def _safe_target_result_callback(self, row_result: dict) -> None:
        if self.target_result_callback is None:
            return
        try:
            self.target_result_callback(dict(row_result))
        except Exception as callback_exc:
            self.log.emit(f"结果回调执行失败：{callback_exc}")

    def _safe_summary_callback(self, summary: dict) -> None:
        if self.summary_callback is None:
            return
        try:
            self.summary_callback(dict(summary))
        except Exception as callback_exc:
            self.log.emit(f"汇总回调执行失败：{callback_exc}")

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
                preview = render_template(self.template, row).strip() if self.template else str(row.get("message") or "").strip()
            if preview:
                return preview[:50]
        return self.template.strip()[:50]

    def _build_report_text(self, summary: dict) -> str:
        preview = summary.get("message_preview", "")
        if preview:
            preview = str(preview)[:30]
            if len(str(summary.get("message_preview", ""))) > 30:
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
                f"附件成功：{summary.get('attachments_sent', 0)} / 附件失败：{summary.get('attachments_failed', 0)}",
                f"内容摘要：{preview}",
                f"执行时间：{summary['started_at']} ~ {summary['finished_at']}（历时{summary['elapsed']}）",
            ]
        )
