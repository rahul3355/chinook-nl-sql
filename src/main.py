"""
main.py — Entry point for the Chinook NL-SQL CLI app.

Flow:
  1. Ask user for a natural-language question
  2. Generate SQL via LLM
  3. Validate SQL (safety check)
  4. Execute SQL against SQLite
  5. Generate plain-English answer via LLM
  6. Save to history
  7. Print everything
"""

import sys
from tabulate import tabulate

from src.sql_generator import generate_sql
from src.safety import is_safe_sql
from src.db import run_query
from src.answer_generator import generate_answer
from src.history_manager import save_history


DIVIDER = "-" * 60


def run_once(question: str) -> None:
    print(f"\n{DIVIDER}")
    print(f"Question : {question}")

    # Step 1 — Generate SQL
    print("Generating SQL...")
    sql = generate_sql(question)
    print(f"SQL      : {sql}")

    # Step 2 — Safety check
    if not is_safe_sql(sql):
        print("ERROR: Unsafe SQL detected. Query blocked.")
        return

    # Step 3 — Execute
    try:
        columns, rows = run_query(sql)
    except Exception as e:
        print(f"ERROR executing SQL: {e}")
        return

    # Step 4 — Display raw results
    if rows:
        print("\nResults:")
        print(tabulate(rows, headers=columns, tablefmt="rounded_outline"))
    else:
        print("Results: (no rows)")

    # Step 5 — Generate English answer
    print("\nGenerating answer...")
    answer = generate_answer(question, sql, rows)
    print(f"\nAnswer   : {answer}")

    # Step 6 — Save history
    save_history(question, sql, answer)
    print(f"{DIVIDER}\n")


def main() -> None:
    print("=" * 60)
    print("  Olist E-commerce NL-SQL Query App")
    print("  Type your question, or 'quit' / 'exit' to stop.")
    print("=" * 60)

    while True:
        try:
            question = input("\nYour question: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            sys.exit(0)

        if not question:
            continue

        if question.lower() in ("quit", "exit", "q"):
            print("Bye!")
            sys.exit(0)

        run_once(question)


if __name__ == "__main__":
    main()
