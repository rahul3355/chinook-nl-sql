import sqlite3
from src.config import DB_PATH


def get_connection():
    """Return a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def run_query(sql: str):
    """
    Execute a SQL query and return (columns, rows).
    columns: list of column name strings
    rows:    list of tuples
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description] if cursor.description else []
        return columns, [tuple(row) for row in rows]
    finally:
        conn.close()
