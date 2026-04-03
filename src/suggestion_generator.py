import os
import re

from src.config import PROMPTS_DIR, SUGGESTION_MODEL_NAME, SUGGESTION_PROVIDER_ORDER
from src.llm import call_llm_routed
from src.result_profiler import profile_result, profile_to_prompt_text
from src.structured_output import compact_text, extract_json_payload


def _load_prompt(filename: str) -> str:
    path = os.path.join(PROMPTS_DIR, filename)
    with open(path, "r", encoding="utf-8") as file:
        return file.read()


def _normalize_question(question: str) -> str:
    text = compact_text(question)
    return re.sub(r"[^\w\s]", "", text).lower()


def _looks_context_dependent(text: str) -> bool:
    lowered = compact_text(text).lower()
    context_markers = [
        "this result",
        "this answer",
        "this trend",
        "this metric",
        "this segment",
        "this category",
        "how does this",
        "what should we investigate next",
        "what explains this",
        "which segment contributes the most to this",
        "the result",
        "the answer",
        "the metric",
        "the trend",
        "these orders",
        "these customers",
        "those segments",
    ]
    if any(marker in lowered for marker in context_markers):
        return True
    standalone_banned = [" this ", " that ", " these ", " those "]
    for banned in standalone_banned:
        if banned in f" {lowered} " and any(
            ref in lowered
            for ref in [
                "result",
                "answer",
                "metric",
                "trend",
                "segment",
                "category",
                "orders",
                "customers",
                "revenue",
                "freight",
            ]
        ):
            return True
    return False


def _infer_subject(question: str, answer: str, profile: dict) -> str:
    lowered = question.lower()
    if "delivered" in lowered and "order" in lowered:
        return "delivered orders"
    if "order" in lowered and "cancel" in lowered:
        return "canceled orders"
    if "revenue" in lowered or "sales" in lowered:
        return "revenue"
    if "freight" in lowered:
        return "freight value"
    if "delivery time" in lowered:
        return "delivery time"
    if "review" in lowered or "rating" in lowered or "score" in lowered:
        return "review scores"
    if "customer" in lowered:
        return "customers"
    if "seller" in lowered:
        return "sellers"
    if "category" in lowered:
        return "product categories"
    if "payment" in lowered:
        return "payment methods"
    if (
        "repeat" in lowered
        or "retention" in lowered
        or "lifetime" in lowered
        or "ltv" in lowered
    ):
        return "customer lifetime value"
    return "order data"


def _dedupe_suggestions(
    question: str, suggestions: list[dict], exclude_questions: list[str] | None = None
) -> list[dict]:
    original = _normalize_question(question)
    deduped = []
    seen = {original}
    for exclude in exclude_questions or []:
        normalized = _normalize_question(exclude)
        if normalized:
            seen.add(normalized)

    for item in suggestions:
        if not isinstance(item, dict):
            continue
        candidate = compact_text(item.get("question"))
        normalized = _normalize_question(candidate)
        if (
            len(candidate) < 8
            or not normalized
            or normalized in seen
            or _looks_context_dependent(candidate)
        ):
            continue
        seen.add(normalized)
        deduped.append(
            {
                "question": candidate if candidate.endswith("?") else f"{candidate}?",
                "category": compact_text(item.get("category")) or "follow_up",
                "rationale": compact_text(item.get("rationale"))
                or "Builds on the previous answer.",
            }
        )
    return deduped


def _fallback_suggestions(
    question: str,
    answer: str,
    profile: dict,
    count: int = 3,
    exclude_questions: list[str] | None = None,
) -> list[dict]:
    shape = profile.get("shape")
    subject = _infer_subject(question, answer, profile)
    fallback = []

    if shape == "time_series":
        fallback.extend(
            [
                {
                    "question": f"What is driving the biggest spike or dip in {subject} over time?",
                    "category": "driver",
                    "rationale": f"Explains the largest change in {subject}.",
                },
                {
                    "question": f"Which segment contributes the most to {subject} over time?",
                    "category": "segment",
                    "rationale": f"Shows which slice matters most for {subject}.",
                },
            ]
        )

    fallback.extend(
        [
            {
                "question": f"Which segment contributes the most to {subject}?",
                "category": "segment",
                "rationale": f"Breaks {subject} into its main contributors.",
            },
            {
                "question": f"How has {subject} changed over time?",
                "category": "trend",
                "rationale": f"Adds time-based context to {subject}.",
            },
            {
                "question": f"Which regions or categories perform best on {subject}?",
                "category": "comparison",
                "rationale": f"Compares {subject} across major segments.",
            },
            {
                "question": f"How concentrated is {subject} across the top segments?",
                "category": "driver",
                "rationale": f"Checks whether {subject} is dominated by a small number of contributors.",
            },
        ]
    )
    return _dedupe_suggestions(question, fallback, exclude_questions)[:count]


def generate_suggestions(
    question: str,
    answer: str,
    sql: str,
    columns: list[str],
    rows: list[tuple],
    *,
    count: int = 3,
    exclude_questions: list[str] | None = None,
) -> list[dict]:
    if not rows:
        return []

    profile = profile_result(columns, rows, sql)
    system_prompt = _load_prompt("suggestion_system_prompt.txt")
    exclusion_text = ""
    if exclude_questions:
        exclusion_text = (
            "Do not return any of these questions:\n"
            + "\n".join(f"- {item}" for item in exclude_questions)
            + "\n\n"
        )
    user_prompt = (
        f"Original question:\n{question}\n\n"
        f"Answer shown to the user:\n{answer}\n\n"
        f"SQL query:\n{sql}\n\n"
        f"Result profile:\n{profile_to_prompt_text(profile)}\n\n"
        f"{exclusion_text}"
        f"Return exactly {count} suggestion objects as JSON."
    )

    try:
        raw = call_llm_routed(
            system_prompt,
            user_prompt,
            model=SUGGESTION_MODEL_NAME,
            provider_order=SUGGESTION_PROVIDER_ORDER,
            allow_fallbacks=True,
            max_tokens=220,
            temperature=0.4,
        )
        parsed = extract_json_payload(raw, default=[])
        if not isinstance(parsed, list):
            parsed = []
        suggestions = _dedupe_suggestions(question, parsed, exclude_questions)[:count]
        return suggestions or _fallback_suggestions(
            question, answer, profile, count=count, exclude_questions=exclude_questions
        )
    except Exception as exc:
        print(f"Error generating suggestions: {exc}")
        return _fallback_suggestions(
            question, answer, profile, count=count, exclude_questions=exclude_questions
        )
