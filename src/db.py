import sqlite3
from src.config import DB_PATH


def run_query(sql: str) -> tuple[list[str], list[tuple]]:
    """Execute a SQL query and return (columns, rows)."""
    print(f"[DB] === EXECUTING QUERY ===")
    print(f"[DB] SQL: {sql}")
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        print(f"[DB] Result: {len(rows)} rows, columns: {columns}")
        if rows:
            print(f"[DB] First row: {rows[0]}")
        return columns, rows
    except Exception as e:
        print(f"[DB] === EXECUTION FAILED ===")
        print(f"[DB] Error: {e}")
        print(f"[DB] SQL: {sql}")
        raise
    finally:
        conn.close()
