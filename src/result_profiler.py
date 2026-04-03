import json
import math
import re
from datetime import datetime
from typing import Any


MAX_SAMPLE_ROWS = 5


def _json_safe(value: Any):
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 4)
    return str(value)


def _looks_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _looks_temporal(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, datetime):
        return True
    if not isinstance(value, str):
        return False

    value = value.strip()
    if not value:
        return False

    patterns = (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m",
        "%Y/%m/%d",
        "%Y/%m",
    )
    for pattern in patterns:
        try:
            datetime.strptime(value, pattern)
            return True
        except ValueError:
            continue
    return False


def _column_kind(values: list[Any]) -> str:
    non_null = [value for value in values if value is not None]
    if not non_null:
        return "unknown"
    if all(_looks_numeric(value) for value in non_null):
        return "numeric"
    if all(_looks_temporal(value) for value in non_null):
        return "temporal"
    return "text"


def _normalize_row(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    return {column: _json_safe(value) for column, value in zip(columns, row)}


def _sql_traits(sql: str) -> dict[str, bool]:
    normalized = re.sub(r"\s+", " ", sql.lower()).strip()
    return {
        "has_group_by": " group by " in f" {normalized} ",
        "has_order_by": " order by " in f" {normalized} ",
        "has_limit": " limit " in f" {normalized} ",
        "has_cte": normalized.startswith("with "),
    }


def _series_highlights(columns: list[str], rows: list[tuple[Any, ...]], dimension_columns: list[str], measure_columns: list[str]) -> list[str]:
    if not rows or not dimension_columns or not measure_columns:
        return []

    first_dimension = columns.index(dimension_columns[0])
    first_measure = columns.index(measure_columns[0])
    highlights = []

    top_row = max(rows, key=lambda row: row[first_measure] if _looks_numeric(row[first_measure]) else float("-inf"))
    bottom_row = min(rows, key=lambda row: row[first_measure] if _looks_numeric(row[first_measure]) else float("inf"))
    if _looks_numeric(top_row[first_measure]):
        highlights.append(
            f"Highest {measure_columns[0]} is {round(float(top_row[first_measure]), 2)} for {top_row[first_dimension]}."
        )
    if len(rows) > 1 and _looks_numeric(bottom_row[first_measure]):
        highlights.append(
            f"Lowest {measure_columns[0]} is {round(float(bottom_row[first_measure]), 2)} for {bottom_row[first_dimension]}."
        )

    if _looks_temporal(rows[0][first_dimension]) and len(rows) > 2:
        deltas = []
        previous_value = None
        for row in rows:
            current_value = row[first_measure]
            if previous_value is None or not _looks_numeric(current_value) or not _looks_numeric(previous_value):
                previous_value = current_value
                continue
            deltas.append((row[first_dimension], float(current_value) - float(previous_value)))
            previous_value = current_value
        if deltas:
            largest_change_period, largest_change_value = max(deltas, key=lambda item: abs(item[1]))
            direction = "increase" if largest_change_value >= 0 else "decrease"
            highlights.append(
                f"Largest sequential change is a {direction} of {round(abs(largest_change_value), 2)} at {largest_change_period}."
            )

    return highlights[:3]


def profile_result(columns: list[str], rows: list[tuple[Any, ...]], sql: str) -> dict[str, Any]:
    column_values = {column: [row[idx] for row in rows] for idx, column in enumerate(columns)}
    columns_meta = [
        {"name": column, "kind": _column_kind(column_values.get(column, []))}
        for column in columns
    ]
    measure_columns = [item["name"] for item in columns_meta if item["kind"] == "numeric"]
    dimension_columns = [item["name"] for item in columns_meta if item["kind"] in ("text", "temporal")]
    sample_rows = [_normalize_row(columns, row) for row in rows[:MAX_SAMPLE_ROWS]]
    traits = _sql_traits(sql)

    if not rows:
        shape = "empty"
    elif len(rows) == 1 and len(columns) <= 3:
        shape = "scalar"
    elif dimension_columns and measure_columns and any(item["kind"] == "temporal" for item in columns_meta):
        shape = "time_series"
    elif traits["has_group_by"] and measure_columns:
        shape = "breakdown"
    elif len(columns) <= 3 and measure_columns:
        shape = "ranking"
    else:
        shape = "detail"

    numeric_summaries = []
    for column in measure_columns[:3]:
        values = [float(value) for value in column_values[column] if _looks_numeric(value)]
        if not values:
            continue
        numeric_summaries.append(
            {
                "column": column,
                "min": round(min(values), 2),
                "max": round(max(values), 2),
                "avg": round(sum(values) / len(values), 2),
            }
        )

    highlights = _series_highlights(columns, rows, dimension_columns, measure_columns)
    if shape == "scalar" and rows and columns:
        highlights.append(f"Primary result: {columns[0]} = {_json_safe(rows[0][0])}.")

    return {
        "row_count": len(rows),
        "column_count": len(columns),
        "shape": shape,
        "columns": columns_meta,
        "dimension_columns": dimension_columns,
        "measure_columns": measure_columns,
        "sample_rows": sample_rows,
        "numeric_summaries": numeric_summaries,
        "highlights": highlights[:4],
        "sql_traits": traits,
    }


def profile_to_prompt_text(profile: dict[str, Any]) -> str:
    return json.dumps(profile, ensure_ascii=False, indent=2)
