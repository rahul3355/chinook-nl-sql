import json
import re
from typing import Any


def extract_json_payload(text: str, default: Any):
    """Best-effort JSON extraction for LLM responses."""
    if not text:
        return default

    candidates = []
    stripped = text.strip()
    if stripped:
        candidates.append(stripped)

    fenced_blocks = re.findall(r"```(?:json)?\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
    candidates.extend(block.strip() for block in fenced_blocks if block.strip())

    for opener, closer in (("[", "]"), ("{", "}")):
        start = text.find(opener)
        end = text.rfind(closer)
        if start != -1 and end != -1 and end > start:
            candidates.append(text[start:end + 1].strip())

    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    return default


def compact_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return re.sub(r"\s+", " ", text).strip()
