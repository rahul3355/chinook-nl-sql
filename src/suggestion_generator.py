import os
from src.config import PROMPTS_DIR
from src.llm import call_llm


def _load_prompt(filename: str) -> str:
    path = os.path.join(PROMPTS_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def generate_suggestions(question: str, sql: str, rows: list) -> list[str]:
    """
    Given the original question, the SQL used, and the result rows,
    ask the LLM to produce 3 contextual next questions.
    """
    system_prompt = _load_prompt("suggestion_system_prompt.txt")

    rows_text = "\n".join(str(row) for row in rows[:10]) if rows else "(no results)"
    if len(rows) > 10:
        rows_text += f"\n... (and {len(rows) - 10} more rows)"

    user_prompt = (
        f"Original User Question: {question}\n\n"
        f"SQL Query Executed:\n{sql}\n\n"
        f"Sample Results (up to 10 rows):\n{rows_text}\n\n"
        f"Based on this analysis, what are 3 contextual follow-up questions the user might want to explore?"
    )

    try:
        response = call_llm(system_prompt, user_prompt)
        # Parse the response: one question per line, exactly 3.
        suggestions = [line.strip() for line in response.split('\n') if line.strip()]
        # Take the first 3 lines just in case
        return suggestions[:3]
    except Exception as e:
        print(f"Error generating suggestions: {e}")
        return []
