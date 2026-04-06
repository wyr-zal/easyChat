from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


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

    @property
    def dataset_label(self) -> str:
        return DATASET_LABELS.get(self.dataset_type, self.dataset_type or "未指定")


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

                CREATE INDEX IF NOT EXISTS idx_friend_import_batches_is_current ON friend_import_batches(is_current);
                CREATE INDEX IF NOT EXISTS idx_group_import_batches_is_current ON group_import_batches(is_current);
                CREATE INDEX IF NOT EXISTS idx_friend_contacts_batch_id ON friend_contacts(batch_id);
                CREATE INDEX IF NOT EXISTS idx_group_contacts_batch_id ON group_contacts(batch_id);
                CREATE INDEX IF NOT EXISTS idx_send_task_items_task_id ON send_task_items(task_id);
                CREATE INDEX IF NOT EXISTS idx_scheduled_send_jobs_status_time ON scheduled_send_jobs(status, scheduled_at);
                """
            )
            self._ensure_column(connection, "send_tasks", "source_mode", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "send_tasks", "dataset_type", "TEXT NOT NULL DEFAULT ''")

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
        rows: list[dict[str, str]],
        filter_fields: str,
        filter_pattern: str,
        target_column: str,
        template_text: str,
        source_batch_id: int | None,
        source_mode: str = "",
        dataset_type: str = "",
    ) -> int:
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
                    dataset_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
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
                       target_column, template_text, total_count, source_mode, dataset_type
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
        }

    def load_task_records(self, task_id: int) -> list[dict[str, str]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, source_contact_id, order_index, target_value, display_name_override, message_override, raw_json
                FROM send_task_items
                WHERE task_id = ?
                ORDER BY order_index ASC, id ASC
                """,
                (task_id,),
            ).fetchall()

        records: list[dict[str, str]] = []
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
            records.append(record)
        return records

    def update_task_item(self, task_item_id: int, row: dict[str, str]) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE send_task_items
                SET target_value = ?,
                    display_name_override = ?,
                    message_override = ?,
                    raw_json = ?
                WHERE id = ?
                """,
                (
                    str(row.get("__target_value") or ""),
                    str(row.get("__display_name_override") or ""),
                    str(row.get("__custom_message_override") or ""),
                    json.dumps(self._clean_record_for_storage(row), ensure_ascii=False),
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
    ) -> int:
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO scheduled_send_jobs (
                    task_id,
                    created_at,
                    scheduled_at,
                    status,
                    interval_seconds,
                    random_delay_min,
                    random_delay_max,
                    operator_name,
                    report_to,
                    source_mode,
                    dataset_type,
                    template_preview,
                    total_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    created_at,
                    scheduled_at,
                    SCHEDULE_STATUS_PENDING,
                    interval_seconds,
                    random_delay_min,
                    random_delay_max,
                    operator_name.strip(),
                    report_to.strip(),
                    source_mode.strip(),
                    dataset_type.strip(),
                    template_preview.strip(),
                    total_count,
                ),
            )
        return int(cursor.lastrowid)

    def list_scheduled_jobs(self, limit: int = 50) -> list[ScheduledSendJob]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, task_id, created_at, scheduled_at, status, interval_seconds,
                       random_delay_min, random_delay_max, operator_name, report_to,
                       source_mode, dataset_type, template_preview, total_count,
                       started_at, completed_at, last_error, result_json
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
                SELECT id, task_id, created_at, scheduled_at, status, interval_seconds,
                       random_delay_min, random_delay_max, operator_name, report_to,
                       source_mode, dataset_type, template_preview, total_count,
                       started_at, completed_at, last_error, result_json
                FROM scheduled_send_jobs
                WHERE status = ? AND scheduled_at <= ?
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
                SET status = ?, started_at = ?, last_error = ''
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
                    result_json = ?
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
                SET status = ?, completed_at = ?, last_error = ''
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
        )

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
