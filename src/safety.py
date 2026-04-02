import re

# SQL keywords that are always safe to start a query
ALLOWED_STARTS = ("select", "with")

# Patterns that should never appear anywhere in the query
BLOCKED_PATTERNS = [
    r"\binsert\b",
    r"\bupdate\b",
    r"\bdelete\b",
    r"\bdrop\b",
    r"\balter\b",
    r"\bcreate\b",
    r"\btruncate\b",
    r"\battach\b",
    r"\bdetach\b",
    r"\bpragma\b",
]


def is_safe_sql(sql: str) -> bool:
    """
    Return True only if sql is a safe read-only query.
    - Must start with SELECT or WITH
    - Must not contain any write/admin keywords
    """
    normalized = sql.strip().lower()

    if not normalized.startswith(ALLOWED_STARTS):
        return False

    for pattern in BLOCKED_PATTERNS:
        if re.search(pattern, normalized):
            return False

    return True
