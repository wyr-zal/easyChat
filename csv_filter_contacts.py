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


def wildcard_to_regex(pattern_text: str) -> str:
    """将通配符模式转换为正则表达式。* 匹配任意字符，? 匹配单个字符。
    如果字符串已经包含正则特殊语法（如 ^、$、[、(、{），则视为纯正则直接返回。"""
    regex_only_chars = set("^$[](){}+\\|")
    if any(c in pattern_text for c in regex_only_chars):
        return pattern_text
    # 通配符转义：先转义正则元字符，再还原 * 和 ?
    escaped = re.escape(pattern_text)
    escaped = escaped.replace(r"\*", ".*").replace(r"\?", ".")
    return escaped


def compile_pattern(pattern_text: str, ignore_case: bool = False) -> re.Pattern[str]:
    flags = re.IGNORECASE if ignore_case else 0
    regex_text = wildcard_to_regex(pattern_text)
    return re.compile(regex_text, flags)


def read_contacts(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError("CSV 文件缺少表头。")
        return list(reader)


def validate_columns(rows: list[dict[str, str]], fields: Iterable[str], contact_type: str, require_wechat_id: bool = True) -> None:
    if not rows:
        raise ValueError("CSV 文件没有联系人数据。")

    available_columns = set(rows[0].keys())
    missing_columns = [field for field in fields if field not in available_columns]
    if require_wechat_id and "微信号" not in available_columns:
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
    search_key_field: str = "",
) -> list[dict[str, str]]:
    """筛选联系人。
    search_key_field: 最终发送时用来在微信搜索框搜索的字段名，结果会注入到 _search_key 字段中。
    """
    matched_rows: list[dict[str, str]] = []
    seen_keys: set[str] = set()

    for row in rows:
        if contact_type and (row.get("类型") or "").strip() != contact_type:
            continue

        # 用显示名称作为去重 key（群聊没有微信号）
        display_name = (row.get("显示名称") or "").strip()
        wechat_id = (row.get("微信号") or "").strip()
        dedup_key = wechat_id or display_name
        if not dedup_key or dedup_key in seen_keys:
            continue

        search_text = build_search_text(row, fields)
        if not pattern.search(search_text):
            continue

        # 注入搜索识别字段
        result_row = dict(row)
        if search_key_field:
            result_row["_search_key"] = (row.get(search_key_field) or "").strip()
        else:
            result_row["_search_key"] = wechat_id or display_name

        matched_rows.append(result_row)
        seen_keys.add(dedup_key)

    return matched_rows


def filter_contacts_from_csv(
    csv_path: Path,
    pattern_text: str,
    fields: Iterable[str],
    contact_type: str = "好友",
    ignore_case: bool = False,
    search_key_field: str = "",
) -> list[dict[str, str]]:
    rows = read_contacts(csv_path)
    require_wechat_id = (contact_type == "好友")
    validate_columns(rows, fields, contact_type, require_wechat_id=require_wechat_id)
    pattern = compile_pattern(pattern_text, ignore_case)
    return filter_contacts(rows, pattern, fields, contact_type, search_key_field)


def load_and_merge_csvs(csv_paths: list[Path]) -> list[dict[str, str]]:
    """合并多个CSV文件，保留所有行（去重由后续filter负责）。"""
    all_rows: list[dict[str, str]] = []
    for path in csv_paths:
        if path.exists():
            all_rows.extend(read_contacts(path))
    return all_rows


def filter_contacts_from_multiple_csvs(
    csv_paths: list[Path],
    pattern_text: str,
    fields: Iterable[str],
    contact_type: str = "",
    ignore_case: bool = False,
    search_key_field: str = "",
) -> list[dict[str, str]]:
    """从多个CSV中合并筛选联系人。contact_type为空则不过滤类型。"""
    all_rows = load_and_merge_csvs(csv_paths)
    if not all_rows:
        raise ValueError("所有CSV文件均为空或不存在。")
    validate_columns(all_rows, fields, contact_type, require_wechat_id=False)
    pattern = compile_pattern(pattern_text, ignore_case)
    return filter_contacts(all_rows, pattern, fields, contact_type, search_key_field)


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
