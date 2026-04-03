import re

from src.analytical_agent import AnalyticalAgent
from src.llm import call_reasoning_llm
from src.vanna_logic import vn_engine


REASONING_SYSTEM_PROMPT = """You are an expert SQLite data analyst. Your job is to convert natural language questions into correct SQLite queries.

DATABASE SCHEMA:
{ddl}

DOCUMENTATION:
{documentation}

RULES:
1. Think step by step about what tables, columns, joins, and filters are needed
2. Always use the correct table and column names from the schema
3. Use aliases for readability (o for orders, oi for order_items, c for customers, etc.)
4. For time-based grouping, use: strftime('%Y-%m', column_name)
5. For counting unique items, use: COUNT(DISTINCT column_name)
6. For revenue/monetary values, use: SUM(column_name) or AVG(column_name)
7. Filter by order_status when the question mentions delivered, cancelled, shipped, etc.
8. Order results logically (DESC for "top", "most", "highest"; ASC for "lowest")
9. Use LIMIT when asking for "top N" or "first N"
10. Return ONLY the SQL query at the very end, with no markdown formatting, no backticks, no explanation after it

If the question is ambiguous, make reasonable assumptions based on the schema and documentation.
If the question cannot be answered with the available tables, explain why and suggest what data would be needed."""


def generate_sql(
    user_question: str, conversation_history: list = None
) -> tuple[str, list[dict]]:
    """Convert a natural-language question into a SQL query with reasoning.
    For causal/analytical questions, uses the multi-step analytical agent.
    Returns (sql_or_answer, reasoning_details)."""

    print(f"[SQL_GEN] === GENERATING SQL ===")
    print(f"[SQL_GEN] Question: {user_question}")
    print(
        f"[SQL_GEN] Conversation history: {len(conversation_history) if conversation_history else 0} turns"
    )

    # For causal/analytical questions, use the agent
    if _is_analytical_question(user_question):
        print(f"[SQL_GEN] Using analytical agent...")
        agent = AnalyticalAgent()
        result = agent.answer(user_question)

        # Return the final answer as the "SQL" (it's actually the full answer)
        # The reasoning steps become the reasoning_details
        return result["answer"], result["reasoning"]

    # For simple questions, use the reasoning LLM directly
    messages = _build_messages(user_question, conversation_history)
    reasoning_details, content = call_reasoning_llm(messages, max_tokens=35000)

    print(f"[SQL_GEN] LLM response received")
    print(f"[SQL_GEN] Reasoning steps: {len(reasoning_details)}")
    print(f"[SQL_GEN] Content length: {len(content)} chars")
    print(f"[SQL_GEN] Content: {content[:500]}...")

    sql = _extract_sql(content)
    print(f"[SQL_GEN] Extracted SQL: '{sql[:200] if sql else 'EMPTY'}'")

    if not sql:
        print(f"[SQL_GEN] SQL extraction FAILED, falling back to vn_engine")
        try:
            sql = vn_engine.generate_sql(user_question)
            print(f"[SQL_GEN] Fallback SQL: '{sql[:200]}'")
        except Exception as e:
            print(f"[SQL_GEN] Fallback also failed: {e}")
            sql = ""

    return sql, reasoning_details


def _is_analytical_question(question: str) -> bool:
    """Check if the question requires multi-step analytical reasoning."""
    lowered = question.lower()
    analytical_keywords = [
        "reason",
        "cause",
        "why",
        "what drove",
        "what driving",
        "spike",
        "dip",
        "surge",
        "drop",
        "trend",
        "pattern",
        "main reason",
        "root cause",
        "explain",
        "analyze",
        "throughout the year",
        "over time",
        "monthly",
        "yearly",
        "what is driving",
        "what's driving",
        "what caused",
    ]
    return any(kw in lowered for kw in analytical_keywords)


def _build_messages(question: str, conversation_history: list = None) -> list[dict]:
    """Build the messages list for the reasoning LLM."""

    schema_context = vn_engine.ddl
    doc_context = "\n".join(vn_engine.documentation)

    system_prompt = REASONING_SYSTEM_PROMPT.format(
        ddl=schema_context,
        documentation=doc_context,
    )

    messages = [{"role": "system", "content": system_prompt}]

    if conversation_history:
        for turn in conversation_history[-3:]:
            sql = turn.get("sql", "")
            # Only include turns that have actual SQL, skip analytical answers
            if sql and sql.strip().upper().startswith(("SELECT", "WITH")):
                messages.append({"role": "user", "content": turn.get("question", "")})
                messages.append({"role": "assistant", "content": sql})

    messages.append({"role": "user", "content": question})

    return messages


def _extract_sql(content: str) -> str:
    """Extract the SQL query from the LLM response."""
    print(f"[SQL_EXTRACT] Extracting SQL from content ({len(content)} chars)")

    # Try code block with sql tag
    sql_match = re.search(r"```sql\s*(.+?)\s*```", content, re.DOTALL)
    if sql_match:
        result = sql_match.group(1).strip()
        print(f"[SQL_EXTRACT] Found in ```sql block: {result[:100]}...")
        return result

    # Try generic code block
    sql_match = re.search(r"```\s*(.+?)\s*```", content, re.DOTALL)
    if sql_match:
        candidate = sql_match.group(1).strip()
        if candidate.upper().startswith(("SELECT", "WITH")):
            print(f"[SQL_EXTRACT] Found in ``` block: {candidate[:100]}...")
            return candidate

    # Find the FIRST line starting with SELECT or WITH and collect all subsequent lines
    lines = content.strip().split("\n")
    sql_lines = []
    in_sql = False
    for line in lines:
        stripped = line.strip()
        if stripped.upper().startswith(("SELECT", "WITH")):
            in_sql = True
        if in_sql:
            sql_lines.append(stripped)

    if sql_lines:
        result = " ".join(sql_lines)
        print(f"[SQL_EXTRACT] Found SELECT/WITH block: {result[:100]}...")
        return result

    # If entire content looks like SQL
    if content.strip().upper().startswith(("SELECT", "WITH")):
        print(f"[SQL_EXTRACT] Entire content is SQL")
        return content.strip()

    print(f"[SQL_EXTRACT] No SQL found in content")
    return ""
