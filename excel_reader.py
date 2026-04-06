from pathlib import Path

import pandas as pd


SUPPORTED_SPREADSHEET_SUFFIXES = (".xlsx", ".xls", ".csv")
DEFAULT_SEND_TARGET_COLUMN = "微信号"
WECHAT_ID_COLUMN = DEFAULT_SEND_TARGET_COLUMN


def load_spreadsheet(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix not in SUPPORTED_SPREADSHEET_SUFFIXES:
        raise ValueError(f"暂不支持的文件类型：{suffix}")

    if suffix == ".csv":
        dataframe = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    else:
        dataframe = pd.read_excel(file_path, dtype=str, keep_default_na=False)

    dataframe = dataframe.fillna("")
    dataframe.columns = [str(column).strip() for column in dataframe.columns]
    return dataframe


def dataframe_to_records(dataframe: pd.DataFrame) -> tuple[list[dict[str, str]], list[str]]:
    columns = list(dataframe.columns)
    records: list[dict[str, str]] = []

    for row in dataframe.to_dict(orient="records"):
        normalized_row = {
            str(column).strip(): "" if value is None else str(value)
            for column, value in row.items()
        }
        if any(value.strip() for value in normalized_row.values()):
            records.append(normalized_row)

    return records, columns


def load_contact_records(file_path: str | Path) -> tuple[list[dict[str, str]], list[str]]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"找不到文件：{path}")

    dataframe = load_spreadsheet(path)
    records, columns = dataframe_to_records(dataframe)
    return records, columns


def validate_contact_records(
    records: list[dict[str, str]],
    columns: list[str],
    required_column: str | None = DEFAULT_SEND_TARGET_COLUMN,
) -> None:
    if not columns:
        raise ValueError("Excel 文件没有表头。")

    if required_column is not None:
        normalized_required_column = required_column.strip() or DEFAULT_SEND_TARGET_COLUMN
        if normalized_required_column not in columns:
            raise ValueError(f"Excel 中缺少必填列：{normalized_required_column}")

    if not records:
        raise ValueError("Excel 中没有可用数据。")
