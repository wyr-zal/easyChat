from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from json_task_io import (
    build_log_path,
    filter_pending_targets,
    load_json_task_file,
    validate_json_task_payload,
)


DEFAULT_LOCAL_DB_PATH = Path("docs") / "easychat_local.sqlite3"
STANDARD_CONTACT_FIELDS = (
    "用户名",
    "显示名称",
    "备注",
    "昵称",
    "微信号",
    "标签",
    "详细描述",
    "类型",
)
FRIEND_SEARCH_PRIORITY = ("微信号", "备注", "显示名称", "昵称", "用户名")
GROUP_SEARCH_PRIORITY = ("显示名称", "昵称", "用户名", "备注", "微信号")

DATASET_FRIEND = "friend"
DATASET_GROUP = "group"
DATASET_ALL = "all"
SOURCE_MODE_FILE = "file"
SOURCE_MODE_LOCAL_DB = "local_db"
SOURCE_MODE_JSON = "json"

DATASET_LABELS = {
    DATASET_FRIEND: "好友",
    DATASET_GROUP: "群聊",
    DATASET_ALL: "好友+群聊",
}
DATASET_BATCH_TABLES = {
    DATASET_FRIEND: "friend_import_batches",
    DATASET_GROUP: "group_import_batches",
}
DATASET_CONTACT_TABLES = {
    DATASET_FRIEND: "friend_contacts",
    DATASET_GROUP: "group_contacts",
}
DATASET_SEQUENCE = (DATASET_FRIEND, DATASET_GROUP)

SCHEDULE_STATUS_PENDING = "pending"
SCHEDULE_STATUS_RUNNING = "running"
SCHEDULE_STATUS_COMPLETED = "completed"
SCHEDULE_STATUS_CANCELLED = "cancelled"
SCHEDULE_STATUS_FAILED = "failed"

SCHEDULE_MODE_ONCE = "once"
SCHEDULE_MODE_DAILY = "daily"
SCHEDULE_MODE_WEEKLY = "weekly"
SCHEDULE_MODE_CRON = "cron"

TASK_KIND_STANDARD = "standard"
TASK_KIND_JSON = "json"
CONFLICT_STATUS_WAITING = "waiting"


@dataclass(slots=True)
class ImportSummary:
    batch_id: int
    dataset_type: str
    source_path: str
    source_name: str
    imported_at: str
    row_count: int
    columns: list[str]

    @property
    def dataset_label(self) -> str:
        return DATASET_LABELS.get(self.dataset_type, self.dataset_type)


@dataclass(slots=True)
class ScheduledSendJob:
    job_id: int
    task_id: int
    created_at: str
    scheduled_at: str
    status: str
    interval_seconds: int
    random_delay_min: int
    random_delay_max: int
    operator_name: str
    report_to: str
    source_mode: str
    dataset_type: str
    template_preview: str
    total_count: int
    started_at: str
    completed_at: str
    last_error: str
    result_json: str
    task_kind: str = ""
    source_json_path: str = ""
    source_json_name: str = ""
    wait_reason: str = ""
    conflict_status: str = ""
    conflict_notified: int = 0
    wait_notified_at: str = ""
    log_path: str = ""
    json_writeback_enabled: int = 0
    schedule_mode: str = SCHEDULE_MODE_ONCE
    schedule_value: str = ""
    enabled: int = 1

    @property
    def dataset_label(self) -> str:
        return DATASET_LABELS.get(self.dataset_type, self.dataset_type or "未指定")


@dataclass(slots=True)
class SendTaskEvent:
    event_id: int
    task_id: int
    task_item_id: int | None
    scheduled_job_id: int | None
    created_at: str
    target_value: str
    target_type: str
    message_mode: str
    send_status: str
    send_time: str
    error_msg: str
    file_path: str
    file_type: str
    attachment_status: str
    source_json_path: str
    log_path: str
    event_json: str


class LocalContactStore:
    def __init__(self, db_path: str | Path = DEFAULT_LOCAL_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS friend_import_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_path TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    columns_json TEXT NOT NULL,
                    is_current INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS friend_contacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL,
                    source_index INTEGER NOT NULL,
                    username TEXT NOT NULL DEFAULT '',
                    display_name TEXT NOT NULL DEFAULT '',
                    note TEXT NOT NULL DEFAULT '',
                    nickname TEXT NOT NULL DEFAULT '',
                    wechat_id TEXT NOT NULL DEFAULT '',
                    tag TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    contact_type TEXT NOT NULL DEFAULT '',
                    raw_json TEXT NOT NULL,
                    FOREIGN KEY(batch_id) REFERENCES friend_import_batches(id)
                );

                CREATE TABLE IF NOT EXISTS group_import_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_path TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    columns_json TEXT NOT NULL,
                    is_current INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS group_contacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL,
                    source_index INTEGER NOT NULL,
                    username TEXT NOT NULL DEFAULT '',
                    display_name TEXT NOT NULL DEFAULT '',
                    note TEXT NOT NULL DEFAULT '',
                    nickname TEXT NOT NULL DEFAULT '',
                    wechat_id TEXT NOT NULL DEFAULT '',
                    tag TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    contact_type TEXT NOT NULL DEFAULT '',
                    raw_json TEXT NOT NULL,
                    FOREIGN KEY(batch_id) REFERENCES group_import_batches(id)
                );

                CREATE TABLE IF NOT EXISTS send_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_batch_id INTEGER,
                    created_at TEXT NOT NULL,
                    filter_fields TEXT NOT NULL DEFAULT '',
                    filter_pattern TEXT NOT NULL DEFAULT '',
                    target_column TEXT NOT NULL DEFAULT '',
                    template_text TEXT NOT NULL DEFAULT '',
                    total_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS send_task_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    source_contact_id INTEGER,
                    order_index INTEGER NOT NULL,
                    target_value TEXT NOT NULL DEFAULT '',
                    display_name_override TEXT NOT NULL DEFAULT '',
                    message_override TEXT NOT NULL DEFAULT '',
                    raw_json TEXT NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES send_tasks(id)
                );

                CREATE TABLE IF NOT EXISTS scheduled_send_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    scheduled_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    interval_seconds INTEGER NOT NULL DEFAULT 0,
                    random_delay_min INTEGER NOT NULL DEFAULT 30,
                    random_delay_max INTEGER NOT NULL DEFAULT 180,
                    operator_name TEXT NOT NULL DEFAULT '',
                    report_to TEXT NOT NULL DEFAULT 'C19881212',
                    source_mode TEXT NOT NULL DEFAULT '',
                    dataset_type TEXT NOT NULL DEFAULT '',
                    template_preview TEXT NOT NULL DEFAULT '',
                    total_count INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT NOT NULL DEFAULT '',
                    completed_at TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT '',
                    result_json TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(task_id) REFERENCES send_tasks(id)
                );

                CREATE TABLE IF NOT EXISTS send_task_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    task_item_id INTEGER,
                    scheduled_job_id INTEGER,
                    created_at TEXT NOT NULL,
                    target_value TEXT NOT NULL DEFAULT '',
                    target_type TEXT NOT NULL DEFAULT '',
                    message_mode TEXT NOT NULL DEFAULT '',
                    send_status TEXT NOT NULL DEFAULT '',
                    send_time TEXT NOT NULL DEFAULT '',
                    error_msg TEXT NOT NULL DEFAULT '',
                    file_path TEXT NOT NULL DEFAULT '',
                    file_type TEXT NOT NULL DEFAULT '',
                    attachment_status TEXT NOT NULL DEFAULT '',
                    source_json_path TEXT NOT NULL DEFAULT '',
                    log_path TEXT NOT NULL DEFAULT '',
                    event_json TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(task_id) REFERENCES send_tasks(id)
                );

                CREATE INDEX IF NOT EXISTS idx_friend_import_batches_is_current ON friend_import_batches(is_current);
                CREATE INDEX IF NOT EXISTS idx_group_import_batches_is_current ON group_import_batches(is_current);
                CREATE INDEX IF NOT EXISTS idx_friend_contacts_batch_id ON friend_contacts(batch_id);
                CREATE INDEX IF NOT EXISTS idx_group_contacts_batch_id ON group_contacts(batch_id);
                CREATE INDEX IF NOT EXISTS idx_send_task_items_task_id ON send_task_items(task_id);
                CREATE INDEX IF NOT EXISTS idx_scheduled_send_jobs_status_time ON scheduled_send_jobs(status, scheduled_at);
                CREATE INDEX IF NOT EXISTS idx_send_task_events_task_id ON send_task_events(task_id);
                CREATE INDEX IF NOT EXISTS idx_send_task_events_task_item_id ON send_task_events(task_item_id);
                CREATE INDEX IF NOT EXISTS idx_send_task_events_job_id ON send_task_events(scheduled_job_id);
                """
            )
            self._ensure_column(connection, "send_tasks", "source_mode", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_tasks", "dataset_type", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_tasks", "task_kind", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_tasks", "source_json_path", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_tasks", "source_json_name", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_tasks", "json_start_time", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_tasks", "json_end_time", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_tasks", "json_log_path", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_tasks", "common_attachments_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column(connection, "send_tasks", "json_payload", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_tasks", "last_sync_at", "TEXT NOT NULL DEFAULT ''")

            self._ensure_column(connection, "send_task_items", "target_type", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_task_items", "message_mode", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_task_items", "attachment_mode", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_task_items", "attachments_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column(connection, "send_task_items", "send_status", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_task_items", "send_time", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_task_items", "error_msg", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_task_items", "attachment_status", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_task_items", "source_json_index", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "send_task_items", "attachment_details_json", "TEXT NOT NULL DEFAULT '[]'")

            self._ensure_column(connection, "scheduled_send_jobs", "task_kind", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "scheduled_send_jobs", "source_json_path", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "scheduled_send_jobs", "source_json_name", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "scheduled_send_jobs", "enabled", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column(connection, "scheduled_send_jobs", "wait_reason", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "scheduled_send_jobs", "conflict_status", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "scheduled_send_jobs", "conflict_notified", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "scheduled_send_jobs", "wait_notified_at", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "scheduled_send_jobs", "log_path", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "scheduled_send_jobs", "json_writeback_enabled", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "scheduled_send_jobs", "schedule_mode", "TEXT NOT NULL DEFAULT 'once'")
            self._ensure_column(connection, "scheduled_send_jobs", "schedule_value", "TEXT NOT NULL DEFAULT ''")

        self._migrate_legacy_current_batch()

    def import_contacts(
        self,
        source_path: str | Path,
        records: list[dict[str, str]],
        columns: list[str],
    ) -> list[ImportSummary]:
        path = Path(source_path)
        imported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        normalized_records: list[dict[str, str]] = []
        for record in records:
            normalized = self._normalize_contact_record(record)
            if normalized:
                normalized_records.append(normalized)

        if not normalized_records:
            raise ValueError("Excel/CSV 中没有可导入的联系人或群聊数据。")

        dataset_rows = {
            DATASET_FRIEND: [row for row in normalized_records if self._resolve_dataset_type(row) == DATASET_FRIEND],
            DATASET_GROUP: [row for row in normalized_records if self._resolve_dataset_type(row) == DATASET_GROUP],
        }

        summaries: list[ImportSummary] = []
        with self.connect() as connection:
            for dataset_type in DATASET_SEQUENCE:
                rows = dataset_rows[dataset_type]
                if not rows:
                    continue
                summaries.append(
                    self._replace_dataset_current(
                        connection=connection,
                        dataset_type=dataset_type,
                        source_path=str(path),
                        source_name=path.name,
                        imported_at=imported_at,
                        columns=columns,
                        rows=rows,
                    )
                )

        return summaries

    def get_current_import_summary(self, dataset_type: str) -> ImportSummary | None:
        batch_table = self._get_batch_table(dataset_type)
        with self.connect() as connection:
            row = connection.execute(
                f"""
                SELECT id, source_path, source_name, imported_at, row_count, columns_json
                FROM {batch_table}
                WHERE is_current = 1
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()

        if row is None:
            return None

        return self._summary_from_row(dataset_type, row)

    def get_current_import_summaries(self) -> dict[str, ImportSummary]:
        summaries: dict[str, ImportSummary] = {}
        for dataset_type in DATASET_SEQUENCE:
            summary = self.get_current_import_summary(dataset_type)
            if summary is not None:
                summaries[dataset_type] = summary
        return summaries

    def load_current_contacts(self, dataset_type: str) -> tuple[list[dict[str, str]], list[str], int | None]:
        summary = self.get_current_import_summary(dataset_type)
        if summary is None:
            return [], [], None

        contact_table = self._get_contact_table(dataset_type)
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, raw_json
                FROM {contact_table}
                WHERE batch_id = ?
                ORDER BY source_index ASC, id ASC
                """,
                (summary.batch_id,),
            ).fetchall()

        records = [self._record_from_contact_row(row, dataset_type) for row in rows]
        return records, summary.columns, summary.batch_id

    def load_all_current_contacts(self) -> tuple[list[dict[str, str]], list[str], dict[str, int]]:
        all_records: list[dict[str, str]] = []
        merged_columns: list[str] = []
        batch_ids: dict[str, int] = {}

        for dataset_type in DATASET_SEQUENCE:
            records, columns, batch_id = self.load_current_contacts(dataset_type)
            if batch_id is not None:
                batch_ids[dataset_type] = batch_id
            merged_columns = self._merge_columns(merged_columns, columns)
            all_records.extend(records)

        return all_records, merged_columns, batch_ids

    def create_task_snapshot(
        self,
        rows: list[dict[str, Any]],
        filter_fields: str,
        filter_pattern: str,
        target_column: str,
        template_text: str,
        source_batch_id: int | None,
        source_mode: str = "",
        dataset_type: str = "",
        task_kind: str = TASK_KIND_STANDARD,
        source_json_path: str = "",
        source_json_name: str = "",
        json_start_time: str = "",
        json_end_time: str = "",
        common_attachments: list[dict[str, str]] | None = None,
        json_payload: dict[str, Any] | None = None,
        json_log_path: str = "",
    ) -> int:
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        normalized_kind = (task_kind or TASK_KIND_STANDARD).strip() or TASK_KIND_STANDARD
        normalized_source_json_path = str(source_json_path or "").strip()
        normalized_source_json_name = str(source_json_name or "").strip()
        if normalized_source_json_path and not normalized_source_json_name:
            normalized_source_json_name = Path(normalized_source_json_path).name
        common_attachments_json = self._normalize_json_list_text(common_attachments)
        payload_json = self._normalize_json_dict_text(json_payload)
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO send_tasks (
                    source_batch_id,
                    created_at,
                    filter_fields,
                    filter_pattern,
                    target_column,
                    template_text,
                    total_count,
                    source_mode,
                    dataset_type,
                    task_kind,
                    source_json_path,
                    source_json_name,
                    json_start_time,
                    json_end_time,
                    common_attachments_json,
                    json_payload,
                    json_log_path,
                    last_sync_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_batch_id,
                    created_at,
                    filter_fields,
                    filter_pattern,
                    target_column,
                    template_text,
                    len(rows),
                    source_mode,
                    dataset_type,
                    normalized_kind,
                    normalized_source_json_path,
                    normalized_source_json_name,
                    str(json_start_time or "").strip(),
                    str(json_end_time or "").strip(),
                    common_attachments_json,
                    payload_json,
                    str(json_log_path or "").strip(),
                    "",
                ),
            )
            task_id = int(cursor.lastrowid)
            connection.executemany(
                """
                INSERT INTO send_task_items (
                    task_id,
                    source_contact_id,
                    order_index,
                    target_value,
                    display_name_override,
                    message_override,
                    raw_json,
                    target_type,
                    message_mode,
                    attachment_mode,
                    attachments_json,
                    send_status,
                    send_time,
                    error_msg,
                    attachment_status,
                    source_json_index,
                    attachment_details_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        task_id,
                        self._to_optional_int(row.get("__contact_id")),
                        index,
                        str(row.get("__target_value") or ""),
                        str(row.get("__display_name_override") or ""),
                        str(row.get("__custom_message_override") or ""),
                        json.dumps(self._clean_record_for_storage(row), ensure_ascii=False),
                        str(row.get("target_type") or row.get("__target_type") or "").strip(),
                        str(row.get("message_mode") or row.get("__message_mode") or "").strip(),
                        str(row.get("attachment_mode") or row.get("__attachment_mode") or "").strip(),
                        self._normalize_json_list_text(row.get("attachments")),
                        str(row.get("send_status") or "").strip(),
                        str(row.get("send_time") or "").strip(),
                        str(row.get("error_msg") or "").strip(),
                        str(row.get("attachment_status") or "").strip(),
                        self._to_int(row.get("__source_json_index") or row.get("source_json_index"), default=0),
                        self._normalize_json_list_text(row.get("attachment_details")),
                    )
                    for index, row in enumerate(rows, start=1)
                ],
            )
        return task_id

    def get_task_details(self, task_id: int) -> dict[str, str] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT id, source_batch_id, created_at, filter_fields, filter_pattern,
                       target_column, template_text, total_count, source_mode, dataset_type,
                       task_kind, source_json_path, source_json_name, json_start_time, json_end_time,
                       json_log_path, common_attachments_json, json_payload, last_sync_at
                FROM send_tasks
                WHERE id = ?
                """,
                (task_id,),
            ).fetchone()

        if row is None:
            return None

        return {
            "id": str(row["id"]),
            "source_batch_id": "" if row["source_batch_id"] is None else str(row["source_batch_id"]),
            "created_at": str(row["created_at"]),
            "filter_fields": str(row["filter_fields"] or ""),
            "filter_pattern": str(row["filter_pattern"] or ""),
            "target_column": str(row["target_column"] or ""),
            "template_text": str(row["template_text"] or ""),
            "total_count": str(row["total_count"] or 0),
            "source_mode": str(row["source_mode"] or ""),
            "dataset_type": str(row["dataset_type"] or ""),
            "task_kind": str(row["task_kind"] or ""),
            "source_json_path": str(row["source_json_path"] or ""),
            "source_json_name": str(row["source_json_name"] or ""),
            "json_start_time": str(row["json_start_time"] or ""),
            "json_end_time": str(row["json_end_time"] or ""),
            "json_log_path": str(row["json_log_path"] or ""),
            "common_attachments_json": str(row["common_attachments_json"] or "[]"),
            "json_payload": str(row["json_payload"] or ""),
            "last_sync_at": str(row["last_sync_at"] or ""),
        }

    def load_task_records(self, task_id: int) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, source_contact_id, order_index, target_value, display_name_override, message_override, raw_json,
                       target_type, message_mode, attachment_mode, attachments_json,
                       send_status, send_time, error_msg, attachment_status,
                       source_json_index, attachment_details_json
                FROM send_task_items
                WHERE task_id = ?
                ORDER BY order_index ASC, id ASC
                """,
                (task_id,),
            ).fetchall()

        records: list[dict[str, Any]] = []
        for row in rows:
            record = json.loads(row["raw_json"])
            record["__task_item_id"] = str(row["id"])
            if row["source_contact_id"] is not None:
                record["__contact_id"] = str(row["source_contact_id"])
            record["__target_value"] = str(row["target_value"] or "")
            if row["display_name_override"]:
                record["__display_name_override"] = str(row["display_name_override"])
            if row["message_override"]:
                record["__custom_message_override"] = str(row["message_override"])
            record["target_type"] = str(row["target_type"] or record.get("target_type") or "")
            record["message_mode"] = str(row["message_mode"] or record.get("message_mode") or "")
            record["attachment_mode"] = str(row["attachment_mode"] or record.get("attachment_mode") or "")
            record["attachments"] = self._loads_json_value(
                row["attachments_json"],
                default=self._loads_json_value(record.get("attachments"), default=[]),
            )
            record["send_status"] = str(row["send_status"] or record.get("send_status") or "")
            record["send_time"] = str(row["send_time"] or record.get("send_time") or "")
            record["error_msg"] = str(row["error_msg"] or record.get("error_msg") or "")
            record["attachment_status"] = str(row["attachment_status"] or record.get("attachment_status") or "")
            record["source_json_index"] = int(row["source_json_index"] or record.get("source_json_index") or 0)
            record["attachment_details"] = self._loads_json_value(
                row["attachment_details_json"],
                default=self._loads_json_value(record.get("attachment_details"), default=[]),
            )
            records.append(record)
        return records

    def update_task_item(self, task_item_id: int, row: dict[str, Any]) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE send_task_items
                SET target_value = ?,
                    display_name_override = ?,
                    message_override = ?,
                    raw_json = ?,
                    target_type = ?,
                    message_mode = ?,
                    attachment_mode = ?,
                    attachments_json = ?,
                    send_status = ?,
                    send_time = ?,
                    error_msg = ?,
                    attachment_status = ?,
                    source_json_index = ?,
                    attachment_details_json = ?
                WHERE id = ?
                """,
                (
                    str(row.get("__target_value") or ""),
                    str(row.get("__display_name_override") or ""),
                    str(row.get("__custom_message_override") or ""),
                    json.dumps(self._clean_record_for_storage(row), ensure_ascii=False),
                    str(row.get("target_type") or row.get("__target_type") or "").strip(),
                    str(row.get("message_mode") or row.get("__message_mode") or "").strip(),
                    str(row.get("attachment_mode") or row.get("__attachment_mode") or "").strip(),
                    self._normalize_json_list_text(row.get("attachments")),
                    str(row.get("send_status") or "").strip(),
                    str(row.get("send_time") or "").strip(),
                    str(row.get("error_msg") or "").strip(),
                    str(row.get("attachment_status") or "").strip(),
                    self._to_int(row.get("__source_json_index") or row.get("source_json_index"), default=0),
                    self._normalize_json_list_text(row.get("attachment_details")),
                    task_item_id,
                ),
            )

    def delete_task_item(self, task_item_id: int) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM send_task_items WHERE id = ?", (task_item_id,))

    def create_scheduled_job(
        self,
        *,
        task_id: int,
        scheduled_at: str,
        interval_seconds: int,
        random_delay_min: int,
        random_delay_max: int,
        operator_name: str,
        report_to: str,
        source_mode: str,
        dataset_type: str,
        template_preview: str,
        total_count: int,
        task_kind: str = TASK_KIND_STANDARD,
        source_json_path: str = "",
        source_json_name: str = "",
        wait_reason: str = "",
        conflict_status: str = "",
        conflict_notified: int = 0,
        wait_notified_at: str = "",
        log_path: str = "",
        json_writeback_enabled: bool = False,
        schedule_mode: str = SCHEDULE_MODE_ONCE,
        schedule_value: str = "",
        enabled: bool = True,
    ) -> int:
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        normalized_schedule_mode = self.normalize_schedule_mode(schedule_mode)
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO scheduled_send_jobs (
                    task_id,
                    created_at,
                    scheduled_at,
                    status,
                    enabled,
                    interval_seconds,
                    random_delay_min,
                    random_delay_max,
                    operator_name,
                    report_to,
                    source_mode,
                    dataset_type,
                    template_preview,
                    total_count,
                    task_kind,
                    source_json_path,
                    source_json_name,
                    wait_reason,
                    conflict_status,
                    conflict_notified,
                    wait_notified_at,
                    log_path,
                    json_writeback_enabled,
                    schedule_mode,
                    schedule_value
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    created_at,
                    scheduled_at,
                    SCHEDULE_STATUS_PENDING,
                    1 if enabled else 0,
                    interval_seconds,
                    random_delay_min,
                    random_delay_max,
                    operator_name.strip(),
                    report_to.strip(),
                    source_mode.strip(),
                    dataset_type.strip(),
                    template_preview.strip(),
                    total_count,
                    (task_kind or TASK_KIND_STANDARD).strip() or TASK_KIND_STANDARD,
                    str(source_json_path or "").strip(),
                    str(source_json_name or "").strip(),
                    str(wait_reason or "").strip(),
                    str(conflict_status or "").strip(),
                    self._to_int(conflict_notified, default=0),
                    str(wait_notified_at or "").strip(),
                    str(log_path or "").strip(),
                    1 if json_writeback_enabled else 0,
                    normalized_schedule_mode,
                    str(schedule_value or "").strip(),
                ),
            )
        return int(cursor.lastrowid)

    def list_scheduled_jobs(self, limit: int = 50) -> list[ScheduledSendJob]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, task_id, created_at, scheduled_at, status, enabled, interval_seconds,
                       random_delay_min, random_delay_max, operator_name, report_to,
                       source_mode, dataset_type, template_preview, total_count,
                       started_at, completed_at, last_error, result_json,
                       task_kind, source_json_path, source_json_name, wait_reason,
                       conflict_status, conflict_notified, wait_notified_at, log_path,
                       json_writeback_enabled, schedule_mode, schedule_value
                FROM scheduled_send_jobs
                ORDER BY
                    CASE status
                        WHEN 'running' THEN 0
                        WHEN 'pending' THEN 1
                        ELSE 2
                    END,
                    scheduled_at ASC,
                    id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [self._scheduled_job_from_row(row) for row in rows]

    def get_due_scheduled_jobs(self, now_text: str, limit: int = 1) -> list[ScheduledSendJob]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, task_id, created_at, scheduled_at, status, enabled, interval_seconds,
                       random_delay_min, random_delay_max, operator_name, report_to,
                       source_mode, dataset_type, template_preview, total_count,
                       started_at, completed_at, last_error, result_json,
                       task_kind, source_json_path, source_json_name, wait_reason,
                       conflict_status, conflict_notified, wait_notified_at, log_path,
                       json_writeback_enabled, schedule_mode, schedule_value
                FROM scheduled_send_jobs
                WHERE status = ? AND enabled = 1 AND scheduled_at <= ?
                ORDER BY scheduled_at ASC, id ASC
                LIMIT ?
                """,
                (SCHEDULE_STATUS_PENDING, now_text, limit),
            ).fetchall()

        return [self._scheduled_job_from_row(row) for row in rows]

    def mark_scheduled_job_running(self, job_id: int) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE scheduled_send_jobs
                SET status = ?, started_at = ?, last_error = '',
                    conflict_status = '', wait_reason = ''
                WHERE id = ?
                """,
                (
                    SCHEDULE_STATUS_RUNNING,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    job_id,
                ),
            )

    def complete_scheduled_job(
        self,
        job_id: int,
        *,
        status: str,
        result: dict | None = None,
        last_error: str = "",
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE scheduled_send_jobs
                SET status = ?,
                    completed_at = ?,
                    last_error = ?,
                    result_json = ?,
                    conflict_status = '',
                    wait_reason = ''
                WHERE id = ?
                """,
                (
                    status,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    last_error.strip(),
                    json.dumps(result or {}, ensure_ascii=False),
                    job_id,
                ),
            )

    def cancel_scheduled_job(self, job_id: int) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE scheduled_send_jobs
                SET status = ?, completed_at = ?, last_error = '',
                    conflict_status = '', wait_reason = ''
                WHERE id = ? AND status = ?
                """,
                (
                    SCHEDULE_STATUS_CANCELLED,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    job_id,
                    SCHEDULE_STATUS_PENDING,
                ),
            )
        return cursor.rowcount > 0

    def set_scheduled_job_enabled(self, job_id: int, enabled: bool) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE scheduled_send_jobs
                SET enabled = ?,
                    conflict_status = '',
                    wait_reason = '',
                    conflict_notified = 0,
                    wait_notified_at = ''
                WHERE id = ? AND status != ?
                """,
                (
                    1 if enabled else 0,
                    job_id,
                    SCHEDULE_STATUS_RUNNING,
                ),
            )
        return cursor.rowcount > 0

    def delete_scheduled_job(self, job_id: int) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                DELETE FROM scheduled_send_jobs
                WHERE id = ? AND status != ?
                """,
                (job_id, SCHEDULE_STATUS_RUNNING),
            )
        return cursor.rowcount > 0

    def reschedule_scheduled_job(
        self,
        job_id: int,
        *,
        next_scheduled_at: str,
        result: dict | None = None,
        last_error: str = "",
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE scheduled_send_jobs
                SET status = ?,
                    scheduled_at = ?,
                    completed_at = '',
                    last_error = ?,
                    result_json = ?,
                    conflict_status = '',
                    wait_reason = '',
                    conflict_notified = 0,
                    wait_notified_at = ''
                WHERE id = ?
                """,
                (
                    SCHEDULE_STATUS_PENDING,
                    next_scheduled_at,
                    str(last_error or "").strip(),
                    json.dumps(result or {}, ensure_ascii=False),
                    job_id,
                ),
            )

    def create_json_task_from_payload(
        self,
        *,
        source_json_path: str | Path,
        payload: dict[str, Any],
        interval_seconds: int,
        random_delay_min: int,
        random_delay_max: int,
        operator_name: str,
        report_to: str,
        source_mode: str = SOURCE_MODE_JSON,
        dataset_type: str = "",
        template_preview: str = "",
        log_path: str = "",
        json_writeback_enabled: bool = True,
    ) -> tuple[int, int]:
        source_path = Path(source_json_path)
        if payload:
            normalized_payload = validate_json_task_payload(
                payload,
                source_path=source_path,
                validate_exists=True,
            )
        else:
            normalized_payload = load_json_task_file(source_path, validate_exists=True)
        targets, skipped_success = filter_pending_targets(normalized_payload)
        if not targets:
            raise ValueError("JSON 中的 targets 全部为已成功状态，无需重复导入。")

        task_rows: list[dict[str, Any]] = []
        for index, target in enumerate(targets, start=1):
            if not isinstance(target, dict):
                raise ValueError(f"targets[{index}] 不是对象。")
            row = dict(target)
            row["target_type"] = str(row.get("target_type") or "").strip()
            row["message_mode"] = str(row.get("message_mode") or "").strip()
            row["attachment_mode"] = str(row.get("attachment_mode") or "").strip()
            row["attachments"] = self._loads_json_value(row.get("attachments"), default=[])
            row["attachment_details"] = self._loads_json_value(row.get("attachment_details"), default=[])
            row["send_status"] = str(row.get("send_status") or "").strip()
            row["send_time"] = str(row.get("send_time") or "").strip()
            row["error_msg"] = str(row.get("error_msg") or "").strip()
            row["attachment_status"] = str(row.get("attachment_status") or "").strip()
            row["source_json_index"] = self._to_int(
                row.get("source_json_index") or row.get("source_target_index") or row.get("__source_json_index") or index,
                default=index,
            )
            row["__source_json_index"] = row["source_json_index"]
            row["__target_value"] = str(row.get("target_value") or "").strip()
            row["__display_name_override"] = str(row.get("display_name") or "").strip()
            if row["message_mode"] == "custom":
                row["__custom_message_override"] = str(row.get("message") or "")
            task_rows.append(row)

        if not task_rows:
            raise ValueError("JSON 任务经过过滤后没有可执行目标。")

        common_attachments = self._loads_json_value(normalized_payload.get("common_attachments"), default=[])
        start_time = str(normalized_payload.get("start_time") or "").strip()
        end_time = str(normalized_payload.get("end_time") or "").strip()
        resolved_log_path = str(log_path or "").strip() or build_log_path(source_path)
        task_id = self.create_task_snapshot(
            rows=task_rows,
            filter_fields="",
            filter_pattern="",
            target_column="target_value",
            template_text=str(normalized_payload.get("template_content") or ""),
            source_batch_id=None,
            source_mode=source_mode,
            dataset_type=dataset_type,
            task_kind=TASK_KIND_JSON,
            source_json_path=str(source_path),
            source_json_name=source_path.name,
            json_start_time=start_time,
            json_end_time=end_time,
            common_attachments=common_attachments,
            json_payload=normalized_payload,
            json_log_path=resolved_log_path,
        )
        job_id = self.create_scheduled_job(
            task_id=task_id,
            scheduled_at=start_time,
            interval_seconds=interval_seconds,
            random_delay_min=random_delay_min,
            random_delay_max=random_delay_max,
            operator_name=operator_name,
            report_to=report_to,
            source_mode=source_mode,
            dataset_type=dataset_type,
            template_preview=template_preview.strip() or str(normalized_payload.get("template_content") or "")[:50],
            total_count=len(task_rows),
            task_kind=TASK_KIND_JSON,
            source_json_path=str(source_path),
            source_json_name=source_path.name,
            wait_reason=f"导入时已跳过 {skipped_success} 个 success 目标。" if skipped_success else "",
            log_path=resolved_log_path,
            json_writeback_enabled=json_writeback_enabled,
            schedule_mode=str(normalized_payload.get("schedule_mode") or SCHEDULE_MODE_ONCE),
            schedule_value=str(normalized_payload.get("schedule_value") or ""),
        )
        return task_id, job_id

    def append_send_event(
        self,
        *,
        task_id: int,
        task_item_id: int | None = None,
        scheduled_job_id: int | None = None,
        target_value: str = "",
        target_type: str = "",
        message_mode: str = "",
        send_status: str = "",
        send_time: str = "",
        error_msg: str = "",
        file_path: str = "",
        file_type: str = "",
        attachment_status: str = "",
        source_json_path: str = "",
        log_path: str = "",
        event_data: dict[str, Any] | None = None,
    ) -> int:
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO send_task_events (
                    task_id, task_item_id, scheduled_job_id, created_at, target_value, target_type,
                    message_mode, send_status, send_time, error_msg, file_path, file_type,
                    attachment_status, source_json_path, log_path, event_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    task_item_id,
                    scheduled_job_id,
                    created_at,
                    target_value.strip(),
                    target_type.strip(),
                    message_mode.strip(),
                    send_status.strip(),
                    send_time.strip(),
                    error_msg.strip(),
                    file_path.strip(),
                    file_type.strip(),
                    attachment_status.strip(),
                    source_json_path.strip(),
                    log_path.strip(),
                    self._normalize_json_dict_text(event_data),
                ),
            )
        return int(cursor.lastrowid)

    def list_task_events(self, task_id: int, limit: int = 200) -> list[SendTaskEvent]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, task_id, task_item_id, scheduled_job_id, created_at, target_value, target_type,
                       message_mode, send_status, send_time, error_msg, file_path, file_type,
                       attachment_status, source_json_path, log_path, event_json
                FROM send_task_events
                WHERE task_id = ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (task_id, limit),
            ).fetchall()
        return [
            SendTaskEvent(
                event_id=int(row["id"]),
                task_id=int(row["task_id"]),
                task_item_id=self._to_optional_int(row["task_item_id"]),
                scheduled_job_id=self._to_optional_int(row["scheduled_job_id"]),
                created_at=str(row["created_at"] or ""),
                target_value=str(row["target_value"] or ""),
                target_type=str(row["target_type"] or ""),
                message_mode=str(row["message_mode"] or ""),
                send_status=str(row["send_status"] or ""),
                send_time=str(row["send_time"] or ""),
                error_msg=str(row["error_msg"] or ""),
                file_path=str(row["file_path"] or ""),
                file_type=str(row["file_type"] or ""),
                attachment_status=str(row["attachment_status"] or ""),
                source_json_path=str(row["source_json_path"] or ""),
                log_path=str(row["log_path"] or ""),
                event_json=str(row["event_json"] or ""),
            )
            for row in rows
        ]

    def update_task_item_result(
        self,
        task_item_id: int,
        *,
        send_status: str | None = None,
        send_time: str | None = None,
        error_msg: str | None = None,
        attachment_status: str | None = None,
        attachments: list[dict[str, Any]] | None = None,
        attachment_details: list[dict[str, Any]] | None = None,
        raw_updates: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT target_value, display_name_override, message_override, raw_json, target_type, message_mode,
                       attachment_mode, attachments_json, send_status, send_time, error_msg,
                       attachment_status, source_json_index, attachment_details_json
                FROM send_task_items
                WHERE id = ?
                """,
                (task_item_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"任务项不存在：{task_item_id}")

            record = json.loads(str(row["raw_json"] or "{}"))
            if raw_updates:
                record.update(raw_updates)
            if send_status is not None:
                record["send_status"] = send_status
            if send_time is not None:
                record["send_time"] = send_time
            if error_msg is not None:
                record["error_msg"] = error_msg
            if attachment_status is not None:
                record["attachment_status"] = attachment_status
            if attachments is not None:
                record["attachments"] = attachments
            if attachment_details is not None:
                record["attachment_details"] = attachment_details

            connection.execute(
                """
                UPDATE send_task_items
                SET raw_json = ?,
                    send_status = ?,
                    send_time = ?,
                    error_msg = ?,
                    attachment_status = ?,
                    attachments_json = ?,
                    attachment_details_json = ?
                WHERE id = ?
                """,
                (
                    json.dumps(self._clean_record_for_storage(record), ensure_ascii=False),
                    str(record.get("send_status") or row["send_status"] or "").strip(),
                    str(record.get("send_time") or row["send_time"] or "").strip(),
                    str(record.get("error_msg") or row["error_msg"] or "").strip(),
                    str(record.get("attachment_status") or row["attachment_status"] or "").strip(),
                    self._normalize_json_list_text(record.get("attachments")),
                    self._normalize_json_list_text(record.get("attachment_details")),
                    task_item_id,
                ),
            )

        record["__task_item_id"] = str(task_item_id)
        record["__target_value"] = str(row["target_value"] or "")
        if row["display_name_override"]:
            record["__display_name_override"] = str(row["display_name_override"])
        if row["message_override"]:
            record["__custom_message_override"] = str(row["message_override"])
        record["target_type"] = str(record.get("target_type") or row["target_type"] or "")
        record["message_mode"] = str(record.get("message_mode") or row["message_mode"] or "")
        record["attachment_mode"] = str(record.get("attachment_mode") or row["attachment_mode"] or "")
        record["attachments"] = self._loads_json_value(record.get("attachments"), default=self._loads_json_value(row["attachments_json"], default=[]))
        record["attachment_details"] = self._loads_json_value(record.get("attachment_details"), default=self._loads_json_value(row["attachment_details_json"], default=[]))
        record["source_json_index"] = self._to_int(record.get("source_json_index") or row["source_json_index"], default=0)
        return record

    def sync_json_task_payload(
        self,
        task_id: int,
        payload: dict[str, Any],
        *,
        json_end_time: str | None = None,
        log_path: str | None = None,
        common_attachments: list[dict[str, Any]] | None = None,
    ) -> None:
        last_sync_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        normalized_payload = self._normalize_json_dict_text(payload)
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE send_tasks
                SET json_payload = ?,
                    json_end_time = COALESCE(?, json_end_time),
                    json_log_path = COALESCE(?, json_log_path),
                    common_attachments_json = COALESCE(?, common_attachments_json),
                    total_count = ?,
                    last_sync_at = ?
                WHERE id = ?
                """,
                (
                    normalized_payload,
                    json_end_time,
                    log_path,
                    self._normalize_json_list_text(common_attachments) if common_attachments is not None else None,
                    len(self._loads_json_value(payload.get("targets"), default=[])),
                    last_sync_at,
                    task_id,
                ),
            )

    def mark_job_waiting_conflict(self, job_id: int, wait_reason: str, *, notify: bool = True) -> None:
        notified_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if notify else ""
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE scheduled_send_jobs
                SET conflict_status = ?,
                    wait_reason = ?,
                    conflict_notified = ?,
                    wait_notified_at = ?
                WHERE id = ?
                """,
                (
                    CONFLICT_STATUS_WAITING,
                    wait_reason.strip(),
                    1 if notify else 0,
                    notified_at,
                    job_id,
                ),
            )

    def clear_job_waiting_conflict(self, job_id: int) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE scheduled_send_jobs
                SET conflict_status = '',
                    wait_reason = '',
                    conflict_notified = 0,
                    wait_notified_at = ''
                WHERE id = ?
                """,
                (job_id,),
            )

    def get_job_source_path(self, job_id: int) -> str:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT source_json_path FROM scheduled_send_jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
        return "" if row is None else str(row["source_json_path"] or "")

    def list_json_jobs(self, limit: int = 50) -> list[ScheduledSendJob]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, task_id, created_at, scheduled_at, status, enabled, interval_seconds,
                       random_delay_min, random_delay_max, operator_name, report_to,
                       source_mode, dataset_type, template_preview, total_count,
                       started_at, completed_at, last_error, result_json,
                       task_kind, source_json_path, source_json_name, wait_reason,
                       conflict_status, conflict_notified, wait_notified_at, log_path,
                       json_writeback_enabled, schedule_mode, schedule_value
                FROM scheduled_send_jobs
                WHERE task_kind = ?
                ORDER BY
                    CASE status
                        WHEN 'running' THEN 0
                        WHEN 'pending' THEN 1
                        ELSE 2
                    END,
                    scheduled_at ASC,
                    id DESC
                LIMIT ?
                """,
                (TASK_KIND_JSON, limit),
            ).fetchall()
        return [self._scheduled_job_from_row(row) for row in rows]

    def _migrate_legacy_current_batch(self) -> None:
        with self.connect() as connection:
            if any(self._has_current_batch(connection, dataset_type) for dataset_type in DATASET_SEQUENCE):
                return
            if not self._table_exists(connection, "import_batches") or not self._table_exists(connection, "contacts"):
                return

            legacy_batch = connection.execute(
                """
                SELECT id, source_path, source_name, imported_at, columns_json
                FROM import_batches
                WHERE is_current = 1
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            if legacy_batch is None:
                return

            rows = connection.execute(
                """
                SELECT raw_json
                FROM contacts
                WHERE batch_id = ?
                ORDER BY source_index ASC, id ASC
                """,
                (legacy_batch["id"],),
            ).fetchall()
            records = [self._normalize_contact_record(json.loads(row["raw_json"])) for row in rows]
            records = [row for row in records if row]
            if not records:
                return

            columns = self._loads_json_list(legacy_batch["columns_json"])
            imported_at = str(legacy_batch["imported_at"])
            for dataset_type in DATASET_SEQUENCE:
                dataset_rows = [row for row in records if self._resolve_dataset_type(row) == dataset_type]
                if dataset_rows:
                    self._replace_dataset_current(
                        connection=connection,
                        dataset_type=dataset_type,
                        source_path=str(legacy_batch["source_path"]),
                        source_name=str(legacy_batch["source_name"]),
                        imported_at=imported_at,
                        columns=columns,
                        rows=dataset_rows,
                    )

    def _replace_dataset_current(
        self,
        *,
        connection: sqlite3.Connection,
        dataset_type: str,
        source_path: str,
        source_name: str,
        imported_at: str,
        columns: list[str],
        rows: list[dict[str, str]],
    ) -> ImportSummary:
        batch_table = self._get_batch_table(dataset_type)
        contact_table = self._get_contact_table(dataset_type)

        connection.execute(f"UPDATE {batch_table} SET is_current = 0 WHERE is_current = 1")
        cursor = connection.execute(
            f"""
            INSERT INTO {batch_table} (source_path, source_name, imported_at, row_count, columns_json, is_current)
            VALUES (?, ?, ?, ?, ?, 1)
            """,
            (
                source_path,
                source_name,
                imported_at,
                len(rows),
                json.dumps(columns, ensure_ascii=False),
            ),
        )
        batch_id = int(cursor.lastrowid)

        connection.executemany(
            f"""
            INSERT INTO {contact_table} (
                batch_id,
                source_index,
                username,
                display_name,
                note,
                nickname,
                wechat_id,
                tag,
                description,
                contact_type,
                raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    batch_id,
                    index,
                    row["用户名"],
                    row["显示名称"],
                    row["备注"],
                    row["昵称"],
                    row["微信号"],
                    row["标签"],
                    row["详细描述"],
                    row["类型"],
                    json.dumps(row, ensure_ascii=False),
                )
                for index, row in enumerate(rows, start=1)
            ],
        )

        return ImportSummary(
            batch_id=batch_id,
            dataset_type=dataset_type,
            source_path=source_path,
            source_name=source_name,
            imported_at=imported_at,
            row_count=len(rows),
            columns=list(columns),
        )

    def _normalize_contact_record(self, record: dict[str, str]) -> dict[str, str]:
        normalized = {field: self._normalize_value(record.get(field, "")) for field in STANDARD_CONTACT_FIELDS}
        if normalized["类型"] == "":
            if normalized["用户名"].endswith("@chatroom"):
                normalized["类型"] = "群聊"
            else:
                normalized["类型"] = "好友"

        if not any(value.strip() for value in normalized.values()):
            return {}

        for key, value in record.items():
            key_text = str(key).strip()
            if key_text and key_text not in normalized:
                normalized[key_text] = self._normalize_value(value)
        return normalized

    def _resolve_dataset_type(self, row: dict[str, str]) -> str:
        contact_type = (row.get("类型") or "").strip()
        return DATASET_GROUP if contact_type == "群聊" else DATASET_FRIEND

    def _summary_from_row(self, dataset_type: str, row: sqlite3.Row) -> ImportSummary:
        return ImportSummary(
            batch_id=int(row["id"]),
            dataset_type=dataset_type,
            source_path=str(row["source_path"]),
            source_name=str(row["source_name"]),
            imported_at=str(row["imported_at"]),
            row_count=int(row["row_count"]),
            columns=self._loads_json_list(row["columns_json"]),
        )

    def _record_from_contact_row(self, row: sqlite3.Row, dataset_type: str) -> dict[str, str]:
        record = json.loads(row["raw_json"])
        record["__contact_id"] = str(row["id"])
        record["__dataset_type"] = dataset_type
        return record

    def _scheduled_job_from_row(self, row: sqlite3.Row) -> ScheduledSendJob:
        return ScheduledSendJob(
            job_id=int(row["id"]),
            task_id=int(row["task_id"]),
            created_at=str(row["created_at"]),
            scheduled_at=str(row["scheduled_at"]),
            status=str(row["status"]),
            enabled=int(row["enabled"] or 0),
            interval_seconds=int(row["interval_seconds"]),
            random_delay_min=int(row["random_delay_min"]),
            random_delay_max=int(row["random_delay_max"]),
            operator_name=str(row["operator_name"] or ""),
            report_to=str(row["report_to"] or ""),
            source_mode=str(row["source_mode"] or ""),
            dataset_type=str(row["dataset_type"] or ""),
            template_preview=str(row["template_preview"] or ""),
            total_count=int(row["total_count"] or 0),
            started_at=str(row["started_at"] or ""),
            completed_at=str(row["completed_at"] or ""),
            last_error=str(row["last_error"] or ""),
            result_json=str(row["result_json"] or ""),
            task_kind=str(row["task_kind"] or ""),
            source_json_path=str(row["source_json_path"] or ""),
            source_json_name=str(row["source_json_name"] or ""),
            wait_reason=str(row["wait_reason"] or ""),
            conflict_status=str(row["conflict_status"] or ""),
            conflict_notified=int(row["conflict_notified"] or 0),
            wait_notified_at=str(row["wait_notified_at"] or ""),
            log_path=str(row["log_path"] or ""),
            json_writeback_enabled=int(row["json_writeback_enabled"] or 0),
            schedule_mode=self.normalize_schedule_mode(row["schedule_mode"]),
            schedule_value=str(row["schedule_value"] or ""),
        )

    def normalize_schedule_mode(self, value: object) -> str:
        normalized = str(value or SCHEDULE_MODE_ONCE).strip().lower()
        if normalized in {
            SCHEDULE_MODE_ONCE,
            SCHEDULE_MODE_DAILY,
            SCHEDULE_MODE_WEEKLY,
            SCHEDULE_MODE_CRON,
        }:
            return normalized
        return SCHEDULE_MODE_ONCE

    def _clean_record_for_storage(self, row: dict[str, str]) -> dict[str, str]:
        return {
            key: value
            for key, value in row.items()
            if not key.startswith("__") or key in {"__display_name_override", "__custom_message_override"}
        }

    def _merge_columns(self, base: list[str], extra: list[str]) -> list[str]:
        merged = list(base)
        for field in STANDARD_CONTACT_FIELDS:
            if field in extra and field not in merged:
                merged.append(field)
        for field in extra:
            if field not in merged:
                merged.append(field)
        return merged

    def _normalize_value(self, value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _loads_json_list(self, raw_value: object) -> list[str]:
        if raw_value is None:
            return []
        loaded = json.loads(str(raw_value))
        return [str(item) for item in loaded]

    def _loads_json_value(self, raw_value: object, default: Any) -> Any:
        if raw_value in (None, ""):
            return json.loads(json.dumps(default, ensure_ascii=False))
        if isinstance(raw_value, (list, dict)):
            return json.loads(json.dumps(raw_value, ensure_ascii=False))
        try:
            return json.loads(str(raw_value))
        except (TypeError, json.JSONDecodeError):
            return json.loads(json.dumps(default, ensure_ascii=False))

    def _normalize_json_list_text(self, raw_value: object) -> str:
        if raw_value in (None, ""):
            return "[]"
        normalized = self._loads_json_value(raw_value, default=[])
        if not isinstance(normalized, list):
            normalized = [normalized]
        return json.dumps(normalized, ensure_ascii=False)

    def _normalize_json_dict_text(self, raw_value: object) -> str:
        if raw_value in (None, ""):
            return ""
        normalized = self._loads_json_value(raw_value, default={})
        if not isinstance(normalized, dict):
            raise ValueError("JSON payload 必须是对象。")
        return json.dumps(normalized, ensure_ascii=False)

    def _to_int(self, value: object, default: int = 0) -> int:
        if value in (None, ""):
            return default
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return default

    def _to_optional_int(self, value: object) -> int | None:
        if value in (None, ""):
            return None
        return int(str(value))

    def _get_batch_table(self, dataset_type: str) -> str:
        if dataset_type not in DATASET_BATCH_TABLES:
            raise ValueError(f"不支持的数据集类型：{dataset_type}")
        return DATASET_BATCH_TABLES[dataset_type]

    def _get_contact_table(self, dataset_type: str) -> str:
        if dataset_type not in DATASET_CONTACT_TABLES:
            raise ValueError(f"不支持的数据集类型：{dataset_type}")
        return DATASET_CONTACT_TABLES[dataset_type]

    def _table_exists(self, connection: sqlite3.Connection, table_name: str) -> bool:
        row = connection.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            """,
            (table_name,),
        ).fetchone()
        return row is not None

    def _has_current_batch(self, connection: sqlite3.Connection, dataset_type: str) -> bool:
        batch_table = self._get_batch_table(dataset_type)
        row = connection.execute(
            f"SELECT 1 FROM {batch_table} WHERE is_current = 1 LIMIT 1"
        ).fetchone()
        return row is not None

    def _ensure_column(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        column_name: str,
        column_definition: str,
    ) -> None:
        columns = {
            str(row["name"])
            for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name in columns:
            return
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")
