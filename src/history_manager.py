import json
import os
from datetime import datetime
from uuid import uuid4

from src.config import HISTORY_PATH


def _normalize_entry(entry: dict, idx: int | None = None) -> dict:
    normalized = dict(entry)
    normalized["id"] = (
        normalized.get("id") or f"legacy-{idx if idx is not None else uuid4().hex}"
    )
    normalized["timestamp"] = normalized.get("timestamp") or datetime.now().isoformat(
        timespec="seconds"
    )
    normalized["question"] = normalized.get("question", "")
    normalized["sql"] = normalized.get("sql", "")
    normalized["answer"] = normalized.get("answer", "")
    normalized["row_count"] = normalized.get("row_count", 0)
    normalized["suggestions"] = normalized.get("suggestions") or []
    return normalized


def _persist(history: list[dict]) -> None:
    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
    with open(HISTORY_PATH, "w", encoding="utf-8") as file:
        json.dump(history, file, indent=2, ensure_ascii=False)


def save_history(
    question: str,
    sql: str,
    answer: str,
    row_count: int = 0,
    suggestions: list | None = None,
) -> str:
    """Append one entry to the query history JSON file and return its id."""
    history = load_history()
    entry = {
        "id": str(uuid4()),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "question": question,
        "sql": sql,
        "answer": answer,
        "row_count": row_count,
        "suggestions": suggestions or [],
    }
    history.append(entry)
    _persist(history)
    return entry["id"]


def load_history() -> list[dict]:
    """Load and return all history entries, or [] if the file doesn't exist."""
    if not os.path.exists(HISTORY_PATH):
        return []
    with open(HISTORY_PATH, "r", encoding="utf-8") as file:
        payload = json.load(file)
    return [_normalize_entry(entry, idx) for idx, entry in enumerate(payload)]


def update_entry(entry_id: str, *, suggestions: list | None = None) -> bool:
    history = load_history()
    updated = False
    for entry in history:
        if entry.get("id") != entry_id:
            continue
        if suggestions is not None:
            entry["suggestions"] = suggestions
        updated = True
        break
    if updated:
        _persist(history)
    return updated


def delete_entry(idx: int) -> bool:
    """Delete the entry at position idx and persist. Returns True on success."""
    history = load_history()
    if 0 <= idx < len(history):
        history.pop(idx)
        _persist(history)
        return True
    return False
