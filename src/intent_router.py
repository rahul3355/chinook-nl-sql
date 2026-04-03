CAUSAL_KEYWORDS = [
    "driving",
    "causes",
    "what caused",
    "why did",
    "why is",
    "spike",
    "dip",
    "surge",
    "drop",
    "decline",
    "increase",
    "what drove",
    "what explains",
    "root cause",
    "reason for",
    "biggest change",
    "what changed",
    "what happened",
    "what is driving",
    "what's driving",
    "what is behind",
]

TIME_SERIES_KEYWORDS = [
    "trend",
    "over time",
    "monthly",
    "yearly",
    "growth",
    "month-over-month",
    "mom",
    "yoy",
    "time series",
    "how has",
    "how have",
    "over the months",
    "over the years",
]

BREAKDOWN_KEYWORDS = ["by", "per", "breakdown", "split", "distribution", "across"]
COMPARISON_KEYWORDS = [
    "compare",
    "vs",
    "versus",
    "difference",
    "between",
    "compared to",
]
RANKING_KEYWORDS = ["top", "bottom", "highest", "lowest", "most", "least", "rank"]


class IntentRouter:
    def classify(self, question: str) -> str:
        lowered = question.lower()

        if any(kw in lowered for kw in CAUSAL_KEYWORDS):
            return "causal"
        if any(kw in lowered for kw in TIME_SERIES_KEYWORDS):
            return "time_series"
        if any(kw in lowered for kw in COMPARISON_KEYWORDS):
            return "comparison"
        if any(kw in lowered for kw in RANKING_KEYWORDS):
            return "ranking"
        if any(kw in lowered for kw in BREAKDOWN_KEYWORDS):
            return "breakdown"

        return "standard"

    def should_trigger_rca(self, intent: str, row_count: int, columns: list) -> bool:
        if intent == "causal":
            return True
        if intent == "time_series" and row_count >= 6:
            return True
        return False
