import re


PLACEHOLDER_PATTERN = re.compile(r"{{\s*([^{}]+?)\s*}}")


def extract_placeholders(template: str) -> list[str]:
    placeholders: list[str] = []
    seen: set[str] = set()

    for match in PLACEHOLDER_PATTERN.finditer(template):
        field_name = match.group(1).strip()
        if field_name and field_name not in seen:
            placeholders.append(field_name)
            seen.add(field_name)

    return placeholders


def find_missing_fields(placeholders: list[str], available_fields: list[str]) -> list[str]:
    available = set(available_fields)
    return [field for field in placeholders if field not in available]


def render_template(template: str, row: dict[str, str]) -> str:
    def replace(match: re.Match[str]) -> str:
        field_name = match.group(1).strip()
        value = row.get(field_name, "")
        return "" if value is None else str(value)

    return PLACEHOLDER_PATTERN.sub(replace, template)
