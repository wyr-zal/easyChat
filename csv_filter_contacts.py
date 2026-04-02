import argparse
import csv
import re
from pathlib import Path
from typing import Iterable


DEFAULT_FIELDS = ("显示名称", "备注", "昵称", "标签", "详细描述", "微信号")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="按正则筛选联系人 CSV，并导出匹配到的微信号列表。"
    )
    parser.add_argument(
        "--csv",
        required=True,
        dest="csv_path",
        help="联系人 CSV 文件路径。",
    )
    parser.add_argument(
        "--pattern",
        required=True,
        help="用于匹配联系人的正则表达式。",
    )
    parser.add_argument(
        "--output",
        default="filtered_wechat_ids.txt",
        help="导出的微信号 txt 文件路径，默认 filtered_wechat_ids.txt。",
    )
    parser.add_argument(
        "--fields",
        default=",".join(DEFAULT_FIELDS),
        help="参与匹配的字段，使用英文逗号分隔。",
    )
    parser.add_argument(
        "--contact-type",
        default="好友",
        help="按联系人类型过滤，默认只保留“好友”；传空字符串可关闭过滤。",
    )
    parser.add_argument(
        "--ignore-case",
        action="store_true",
        help="启用正则忽略大小写匹配。",
    )
    parser.add_argument(
        "--preview",
        type=int,
        default=10,
        help="终端预览前几条匹配结果，默认 10。",
    )
    return parser.parse_args()


def normalize_fields(raw_fields: str) -> list[str]:
    fields = [field.strip() for field in raw_fields.split(",") if field.strip()]
    if not fields:
        raise ValueError("至少需要指定一个匹配字段。")
    return fields


def compile_pattern(pattern_text: str, ignore_case: bool = False) -> re.Pattern[str]:
    flags = re.IGNORECASE if ignore_case else 0
    return re.compile(pattern_text, flags)


def read_contacts(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError("CSV 文件缺少表头。")
        return list(reader)


def validate_columns(rows: list[dict[str, str]], fields: Iterable[str], contact_type: str) -> None:
    if not rows:
        raise ValueError("CSV 文件没有联系人数据。")

    available_columns = set(rows[0].keys())
    missing_columns = [field for field in fields if field not in available_columns]
    if "微信号" not in available_columns:
        missing_columns.append("微信号")
    if contact_type and "类型" not in available_columns:
        missing_columns.append("类型")

    if missing_columns:
        unique_missing = sorted(set(missing_columns))
        raise ValueError(f"CSV 文件缺少必要列: {', '.join(unique_missing)}")


def build_search_text(row: dict[str, str], fields: Iterable[str]) -> str:
    return " | ".join((row.get(field) or "").strip() for field in fields)


def filter_contacts(
    rows: list[dict[str, str]],
    pattern: re.Pattern[str],
    fields: Iterable[str],
    contact_type: str,
) -> list[dict[str, str]]:
    matched_rows: list[dict[str, str]] = []
    seen_wechat_ids: set[str] = set()

    for row in rows:
        if contact_type and (row.get("类型") or "").strip() != contact_type:
            continue

        wechat_id = (row.get("微信号") or "").strip()
        if not wechat_id or wechat_id in seen_wechat_ids:
            continue

        search_text = build_search_text(row, fields)
        if not pattern.search(search_text):
            continue

        matched_rows.append(row)
        seen_wechat_ids.add(wechat_id)

    return matched_rows


def filter_contacts_from_csv(
    csv_path: Path,
    pattern_text: str,
    fields: Iterable[str],
    contact_type: str = "好友",
    ignore_case: bool = False,
) -> list[dict[str, str]]:
    rows = read_contacts(csv_path)
    validate_columns(rows, fields, contact_type)
    pattern = compile_pattern(pattern_text, ignore_case)
    return filter_contacts(rows, pattern, fields, contact_type)


def export_wechat_ids(output_path: Path, matched_rows: list[dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file:
        for row in matched_rows:
            file.write(f"{row['微信号'].strip()}\n")


def format_preview_rows(matched_rows: list[dict[str, str]], preview: int) -> list[str]:
    preview_rows: list[str] = []
    for index, row in enumerate(matched_rows[:preview], start=1):
        display_name = (row.get("显示名称") or "").strip()
        note = (row.get("备注") or "").strip()
        nickname = (row.get("昵称") or "").strip()
        wechat_id = (row.get("微信号") or "").strip()
        preview_rows.append(
            f"{index}. 显示名称={display_name} | 备注={note} | 昵称={nickname} | 微信号={wechat_id}"
        )
    return preview_rows


def print_preview(matched_rows: list[dict[str, str]], preview: int) -> None:
    if not matched_rows:
        print("未匹配到任何联系人。")
        return

    print(f"共匹配到 {len(matched_rows)} 个联系人，预览前 {min(preview, len(matched_rows))} 条：")
    for line in format_preview_rows(matched_rows, preview):
        print(line)


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv_path)
    output_path = Path(args.output)

    if not csv_path.exists():
        raise FileNotFoundError(f"找不到 CSV 文件: {csv_path}")

    fields = normalize_fields(args.fields)
    matched_rows = filter_contacts_from_csv(
        csv_path=csv_path,
        pattern_text=args.pattern,
        fields=fields,
        contact_type=args.contact_type,
        ignore_case=args.ignore_case,
    )
    export_wechat_ids(output_path, matched_rows)
    print_preview(matched_rows, args.preview)
    print(f"已导出微信号列表: {output_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
