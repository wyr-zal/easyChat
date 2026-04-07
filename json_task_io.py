from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any


JSON_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
SUPPORTED_ATTACHMENT_SUFFIXES = {".pdf", ".jpg", ".jpeg", ".png", ".bmp", ".webp"}
IMAGE_ATTACHMENT_SUFFIXES = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
PDF_ATTACHMENT_SUFFIXES = {".pdf"}

TARGET_TYPE_PERSON = "person"
TARGET_TYPE_GROUP = "group"
MESSAGE_MODE_TEMPLATE = "template"
MESSAGE_MODE_CUSTOM = "custom"
ATTACHMENT_MODE_COMMON = "common"
ATTACHMENT_MODE_CUSTOM = "custom"

SEND_STATUS_PENDING = "pending"
SEND_STATUS_SUCCESS = "success"
SEND_STATUS_FAILED = "failed"
SEND_STATUS_SKIPPED = "skipped"

ATTACHMENT_STATUS_NONE = "none"
ATTACHMENT_STATUS_SUCCESS = "success"
ATTACHMENT_STATUS_FAILED = "failed"
ATTACHMENT_STATUS_SKIPPED = "skipped"


def now_text() -> str:
    return datetime.now().strftime(JSON_TIME_FORMAT)


def normalize_path(path_value: str | Path, *, base_dir: str | Path | None = None) -> str:
    path_text = str(path_value or "").strip()
    if path_text == "":
        return ""

    path = Path(path_text)
    if not path.is_absolute() and base_dir is not None:
        path = Path(base_dir) / path
    return str(path.expanduser().resolve(strict=False))


def detect_attachment_type(path_value: str | Path) -> str:
    suffix = Path(str(path_value)).suffix.lower()
    if suffix in PDF_ATTACHMENT_SUFFIXES:
        return "pdf"
    if suffix in IMAGE_ATTACHMENT_SUFFIXES:
        return "image"
    raise ValueError(f"不支持的附件类型：{suffix or '无后缀'}")


def normalize_attachment_item(
    item: str | dict[str, Any],
    *,
    base_dir: str | Path | None = None,
    validate_exists: bool = True,
) -> dict[str, str]:
    if isinstance(item, dict):
        file_path = normalize_path(item.get("file_path", ""), base_dir=base_dir)
        file_type = str(item.get("file_type", "")).strip().lower()
    else:
        file_path = normalize_path(item, base_dir=base_dir)
        file_type = ""

    if file_path == "":
        raise ValueError("附件路径不能为空。")

    suffix = Path(file_path).suffix.lower()
    if suffix not in SUPPORTED_ATTACHMENT_SUFFIXES:
        raise ValueError(f"附件类型不合法：{suffix}")

    if validate_exists and not Path(file_path).exists():
        raise ValueError(f"附件不存在：{file_path}")

    normalized_type = detect_attachment_type(file_path)
    if file_type and file_type not in {normalized_type, "pdf", "image"}:
        raise ValueError(f"附件 file_type 与路径后缀不匹配：{file_path}")

    return {
        "file_type": normalized_type,
        "file_path": file_path,
    }


def normalize_attachment_list(
    attachments: Any,
    *,
    base_dir: str | Path | None = None,
    validate_exists: bool = True,
) -> list[dict[str, str]]:
    if attachments in (None, ""):
        return []
    if isinstance(attachments, str):
        raw_text = attachments.strip()
        if raw_text == "":
            return []
        if raw_text.startswith("[") or raw_text.startswith("{"):
            try:
                attachments = json.loads(raw_text)
            except json.JSONDecodeError as exc:
                raise ValueError("附件列表 JSON 解析失败。") from exc
        else:
            attachments = [segment.strip() for segment in raw_text.split(";") if segment.strip()]

    if isinstance(attachments, dict):
        attachments = [attachments]
    if not isinstance(attachments, list):
        raise ValueError("附件列表必须是数组。")

    return [
        normalize_attachment_item(item, base_dir=base_dir, validate_exists=validate_exists)
        for item in attachments
    ]


def normalize_target_payload(
    target: dict[str, Any],
    *,
    index: int,
    base_dir: str | Path | None = None,
    validate_exists: bool = True,
) -> dict[str, Any]:
    if not isinstance(target, dict):
        raise ValueError(f"targets[{index}] 必须是对象。")

    target_value = str(target.get("target_value", "")).strip()
    if target_value == "":
        raise ValueError(f"targets[{index}] 缺少 target_value。")

    target_type = str(target.get("target_type", TARGET_TYPE_PERSON)).strip().lower()
    if target_type not in {TARGET_TYPE_PERSON, TARGET_TYPE_GROUP}:
        raise ValueError(f"targets[{index}] 的 target_type 只能是 person 或 group。")

    message_mode = str(target.get("message_mode", MESSAGE_MODE_TEMPLATE)).strip().lower()
    if message_mode not in {MESSAGE_MODE_TEMPLATE, MESSAGE_MODE_CUSTOM}:
        raise ValueError(f"targets[{index}] 的 message_mode 只能是 template 或 custom。")

    attachment_mode = str(target.get("attachment_mode", ATTACHMENT_MODE_COMMON)).strip().lower()
    if attachment_mode not in {ATTACHMENT_MODE_COMMON, ATTACHMENT_MODE_CUSTOM}:
        raise ValueError(f"targets[{index}] 的 attachment_mode 只能是 common 或 custom。")

    attachments = normalize_attachment_list(
        target.get("attachments", []),
        base_dir=base_dir,
        validate_exists=validate_exists,
    )
    if attachment_mode == ATTACHMENT_MODE_COMMON:
        attachments = []

    message = str(target.get("message", "") or "")
    if message_mode == MESSAGE_MODE_CUSTOM and message.strip() == "":
        raise ValueError(f"targets[{index}] 在 custom 模式下必须提供 message。")

    return {
        "target_value": target_value,
        "target_type": target_type,
        "message_mode": message_mode,
        "message": message,
        "attachment_mode": attachment_mode,
        "attachments": attachments,
        "send_status": str(target.get("send_status", SEND_STATUS_PENDING) or SEND_STATUS_PENDING).strip().lower(),
        "attachment_status": str(
            target.get("attachment_status", ATTACHMENT_STATUS_NONE) or ATTACHMENT_STATUS_NONE
        ).strip().lower(),
        "error_msg": str(target.get("error_msg", "") or ""),
        "send_time": str(target.get("send_time", "") or ""),
        "source_json_index": int(target.get("source_json_index") or target.get("source_target_index") or index),
        "display_name": str(target.get("display_name", "") or ""),
    }


def validate_json_task_payload(
    payload: dict[str, Any],
    *,
    source_path: str | Path | None = None,
    validate_exists: bool = True,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("JSON 根节点必须是对象。")

    base_dir = Path(source_path).resolve(strict=False).parent if source_path else None
    start_time = str(payload.get("start_time", "")).strip()
    if start_time == "":
        raise ValueError("JSON 缺少 start_time。")
    try:
        datetime.strptime(start_time, JSON_TIME_FORMAT)
    except ValueError as exc:
        raise ValueError("start_time 格式必须为 yyyy-MM-dd HH:mm:ss。") from exc

    end_time = str(payload.get("end_time", "") or "")
    if end_time:
        try:
            datetime.strptime(end_time, JSON_TIME_FORMAT)
        except ValueError as exc:
            raise ValueError("end_time 格式必须为 yyyy-MM-dd HH:mm:ss。") from exc

    common_attachments = normalize_attachment_list(
        payload.get("common_attachments", []),
        base_dir=base_dir,
        validate_exists=validate_exists,
    )

    raw_targets = payload.get("targets", [])
    if not isinstance(raw_targets, list) or not raw_targets:
        raise ValueError("JSON 必须包含至少一个 targets 条目。")

    normalized_targets = [
        normalize_target_payload(
            target,
            index=index,
            base_dir=base_dir,
            validate_exists=validate_exists,
        )
        for index, target in enumerate(raw_targets, start=1)
    ]

    return {
        "start_time": start_time,
        "end_time": end_time,
        "total_count": int(payload.get("total_count") or len(normalized_targets)),
        "template_content": str(payload.get("template_content", "") or ""),
        "common_attachments": common_attachments,
        "targets": normalized_targets,
    }


def load_json_task_file(path_value: str | Path, *, validate_exists: bool = True) -> dict[str, Any]:
    path = Path(path_value)
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return validate_json_task_payload(payload, source_path=path, validate_exists=validate_exists)


def dump_json_task_file(
    path_value: str | Path,
    payload: dict[str, Any],
    *,
    create_backup: bool = False,
) -> None:
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)

    if create_backup and path.exists():
        backup_path = path.with_name(f"{path.name}.easychat.bak")
        if not backup_path.exists():
            backup_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")

    tmp_path = path.with_name(f"{path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def build_log_path(source_json_path: str | Path | None = None, *, task_id: int | None = None) -> str:
    if source_json_path:
        source_path = Path(source_json_path)
        return str(source_path.with_name(f"{source_path.stem}.easychat.log"))

    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    suffix = str(task_id) if task_id is not None else now_text().replace(":", "").replace(" ", "-")
    return str((logs_dir / f"easychat-task-{suffix}.log").resolve(strict=False))


def append_task_log(log_path: str | Path, entry: str | dict[str, Any]) -> None:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(entry, dict):
        timestamp = str(entry.get("timestamp") or now_text())
        target = str(entry.get("target") or "-")
        text_status = str(entry.get("text_status") or "-")
        attachment_status = str(entry.get("attachment_status") or "-")
        reason = str(entry.get("reason") or "-")
        line = (
            f"{timestamp} | target={target} | text={text_status} | "
            f"attachment={attachment_status} | reason={reason}"
        )
        attachments = entry.get("attachments", [])
        if attachments:
            attachment_bits = []
            for item in attachments:
                file_path = str(item.get("file_path") or "")
                item_status = str(item.get("attachment_status") or item.get("status") or "")
                attachment_bits.append(f"{Path(file_path).name}:{item_status or '-'}")
            line += " | files=" + ",".join(attachment_bits)
    else:
        line = str(entry)

    with path.open("a", encoding="utf-8") as handle:
        handle.write(line.rstrip() + "\n")


def filter_pending_targets(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    pending_targets: list[dict[str, Any]] = []
    skipped_success = 0
    for target in payload.get("targets", []):
        if str(target.get("send_status") or "").strip().lower() == SEND_STATUS_SUCCESS:
            skipped_success += 1
            continue
        pending_targets.append(dict(target))
    return pending_targets, skipped_success


def update_json_target_status(
    path_value: str | Path,
    *,
    target_index: int | None = None,
    source_json_index: int | None = None,
    send_status: str | None = None,
    error_msg: str | None = None,
    attachment_status: str | None = None,
    send_time: str | None = None,
    attachment_results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    path = Path(path_value)
    payload = load_json_task_file(path, validate_exists=False)
    targets = payload.get("targets", [])
    resolved_index = -1
    if source_json_index is not None:
        for idx, raw_target in enumerate(targets):
            current_index = int(raw_target.get("source_json_index") or raw_target.get("source_target_index") or idx + 1)
            if current_index == source_json_index:
                resolved_index = idx
                break
        if resolved_index < 0:
            raise IndexError(f"source_json_index 越界：{source_json_index}")
    elif target_index is not None:
        if target_index < 0 or target_index >= len(targets):
            raise IndexError(f"target_index 越界：{target_index}")
        resolved_index = target_index
    else:
        raise ValueError("target_index 和 source_json_index 至少要提供一个。")

    target = dict(targets[resolved_index])
    if send_status is not None:
        target["send_status"] = str(send_status)
    if error_msg is not None:
        target["error_msg"] = str(error_msg)
    if attachment_status is not None:
        target["attachment_status"] = str(attachment_status)
    if send_time is not None:
        target["send_time"] = str(send_time)
    if attachment_results is not None:
        target["attachment_results"] = list(attachment_results)
        target["attachment_details"] = list(attachment_results)
    targets[resolved_index] = target
    payload["targets"] = targets
    dump_json_task_file(path, payload, create_backup=True)
    return target


def update_json_task_end_time(path_value: str | Path, end_time: str | None = None) -> dict[str, Any]:
    path = Path(path_value)
    payload = load_json_task_file(path, validate_exists=False)
    payload["end_time"] = str(end_time or now_text())
    payload["total_count"] = len(payload.get("targets", []))
    dump_json_task_file(path, payload, create_backup=True)
    return payload


def load_json_task(path_value: str | Path, *, validate_exists: bool = True) -> dict[str, Any]:
    return load_json_task_file(path_value, validate_exists=validate_exists)


def write_json_task_atomic(
    path_value: str | Path,
    payload: dict[str, Any],
    *,
    create_backup: bool = False,
) -> None:
    dump_json_task_file(path_value, payload, create_backup=create_backup)


def normalize_json_task_payload(
    payload: dict[str, Any],
    *,
    source_path: str | Path | None = None,
    validate_exists: bool = True,
) -> dict[str, Any]:
    return validate_json_task_payload(payload, source_path=source_path, validate_exists=validate_exists)


def build_default_log_path(source_json_path: str | Path | None = None, *, task_id: int | None = None) -> str:
    return build_log_path(source_json_path, task_id=task_id)


def normalize_attachment_entry(
    item: str | dict[str, Any],
    *,
    base_dir: str | Path | None = None,
    require_exists: bool = True,
) -> dict[str, str]:
    return normalize_attachment_item(item, base_dir=base_dir, validate_exists=require_exists)


def normalize_attachments(
    attachments: Any,
    *,
    base_dir: str | Path | None = None,
    require_exists: bool = True,
) -> list[dict[str, str]]:
    return normalize_attachment_list(attachments, base_dir=base_dir, validate_exists=require_exists)


def infer_attachment_type(path_value: str | Path) -> str:
    return detect_attachment_type(path_value)


def update_target_runtime_fields(
    payload: dict[str, Any],
    source_json_index: int,
    *,
    send_status: str | None = None,
    error_msg: str | None = None,
    attachment_status: str | None = None,
    send_time: str | None = None,
    attachment_results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized = json.loads(json.dumps(payload, ensure_ascii=False))
    targets = normalized.get("targets", [])
    for idx, target in enumerate(targets):
        current_index = int(target.get("source_json_index") or target.get("source_target_index") or idx + 1)
        if current_index != source_json_index:
            continue
        if send_status is not None:
            target["send_status"] = str(send_status)
        if error_msg is not None:
            target["error_msg"] = str(error_msg)
        if attachment_status is not None:
            target["attachment_status"] = str(attachment_status)
        if send_time is not None:
            target["send_time"] = str(send_time)
        if attachment_results is not None:
            target["attachment_results"] = list(attachment_results)
            target["attachment_details"] = list(attachment_results)
        targets[idx] = target
        normalized["targets"] = targets
        normalized["total_count"] = len(targets)
        return normalized
    raise IndexError(f"source_json_index 越界：{source_json_index}")
