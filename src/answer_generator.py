import os
import re
from src.config import PROMPTS_DIR
from src.llm import call_llm


def _strip_markdown(text: str) -> str:
    """Remove common markdown formatting so answers are plain text."""
    # Bold: **text** or __text__
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text, flags=re.DOTALL)
    text = re.sub(r'__(.+?)__',     r'\1', text, flags=re.DOTALL)
    # Italic: *text* or _text_
    text = re.sub(r'\*(.+?)\*', r'\1', text, flags=re.DOTALL)
    text = re.sub(r'_(.+?)_',   r'\1', text, flags=re.DOTALL)
    # Inline code: `text`
    text = re.sub(r'`(.+?)`', r'\1', text)
    return text.strip()


def _load_prompt(filename: str) -> str:
    path = os.path.join(PROMPTS_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def generate_answer(question: str, sql: str, rows: list) -> str:
    """
    Given the original question, the SQL used, and the result rows,
    ask the LLM to produce a plain-English answer.
    """
    system_prompt = _load_prompt("answer_system_prompt.txt")

    rows_text = "\n".join(str(row) for row in rows) if rows else "(no results)"

    user_prompt = (
        f"Question: {question}\n\n"
        f"SQL used:\n{sql}\n\n"
        f"Results:\n{rows_text}"
    )

    return _strip_markdown(call_llm(system_prompt, user_prompt))
