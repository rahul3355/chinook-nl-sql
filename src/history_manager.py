import json
import os
from datetime import datetime
from src.config import HISTORY_PATH


def save_history(question: str, sql: str, answer: str) -> None:
    """Append one entry to the query history JSON file."""
    history = load_history()
    entry = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "question": question,
        "sql": sql,
        "answer": answer,
    }
    history.append(entry)

    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)


def load_history() -> list:
    """Load and return all history entries, or [] if the file doesn't exist."""
    if not os.path.exists(HISTORY_PATH):
        return []
    with open(HISTORY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def delete_entry(idx: int) -> bool:
    """Delete the entry at position idx and persist. Returns True on success."""
    history = load_history()
    if 0 <= idx < len(history):
        history.pop(idx)
        os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
        with open(HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
        return True
    return False
