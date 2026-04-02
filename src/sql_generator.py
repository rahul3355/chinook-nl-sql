import os
from src.config import PROMPTS_DIR
from src.llm import call_llm


def _load_prompt(filename: str) -> str:
    path = os.path.join(PROMPTS_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def generate_sql(user_question: str) -> str:
    """
    Convert a natural-language question into a SQL query.
    Loads the schema and system prompt from the prompts/ directory.
    """
    schema = _load_prompt("schema.txt")
    system_prompt = _load_prompt("sql_system_prompt.txt")

    user_prompt = (
        f"Database schema:\n{schema}\n\n"
        f"Question: {user_question}"
    )

    sql = call_llm(system_prompt, user_prompt)

    # Strip any accidental markdown fences the model might add
    sql = sql.strip().strip("`").strip()

    return sql
