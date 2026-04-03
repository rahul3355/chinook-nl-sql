import os
from src.config import PROMPTS_DIR
from src.llm import call_llm


def _load_prompt(filename: str) -> str:
    path = os.path.join(PROMPTS_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def generate_insights(question: str, sql: str, rows: list) -> list[str]:
    """
    Given the original question, the SQL used, and the result rows,
    ask the LLM to identify 2-3 non-obvious insights.
    """
    if not rows or len(rows) < 2:
        return []

    system_prompt = _load_prompt("insight_system_prompt.txt")

    rows_text = "\n".join(str(row) for row in rows[:15]) if rows else "(no results)"
    if len(rows) > 15:
        rows_text += f"\n... (and {len(rows) - 15} more rows)"

    user_prompt = (
        f"Original User Question: {question}\n\n"
        f"SQL Query Executed:\n{sql}\n\n"
        f"Sample Results (up to 15 rows):\n{rows_text}\n\n"
        f"Identify 2-3 non-obvious insights, anomalies, or trends from this analysis."
    )

    try:
        response = call_llm(system_prompt, user_prompt)
        # Parse the response: one insight per line.
        insights = [line.strip() for line in response.split('\n') if line.strip()]
        # Take the first 3 lines just in case
        return insights[:3]
    except Exception as e:
        print(f"Error generating insights: {e}")
        return []
