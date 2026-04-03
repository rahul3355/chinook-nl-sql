import json
import time

from src.config import MODEL_NAME
from src.db import run_query
from src.llm import call_llm_routed
from src.query_planner import decompose_question
from src.query_executor import execute_plan
from src.data_integrator import integrate_results


MIN_MOM = 30
MIN_ANOMALIES = 2
BASELINE = 3
MIN_BASELINE_VAL = 100000


def analyze_stream(question, send_event):
    send_event(
        {
            "type": "planning",
            "step": "decomposing",
            "message": "Breaking down question into subqueries...",
        }
    )

    t0 = time.time()
    try:
        plan = decompose_question(question)
    except Exception as e:
        send_event({"type": "error", "message": f"Planning failed: {e}"})
        return

    send_event(
        {
            "type": "planning",
            "step": "plan_ready",
            "message": f"Decomposed into {len(plan.subqueries)} subqueries",
            "time": round(time.time() - t0, 2),
        }
    )
    send_event(
        {
            "type": "planning",
            "subqueries": [
                {"id": s.id, "description": s.description, "tables": s.tables}
                for s in plan.subqueries
            ],
        }
    )

    t0 = time.time()
    send_event(
        {
            "type": "executing",
            "step": "base_query",
            "message": "Running base time-series query...",
        }
    )

    try:
        _, rows = run_query(plan.base_query_sql)
    except Exception as e:
        send_event({"type": "error", "message": f"Base query failed: {e}"})
        return

    if not rows or len(rows) < 6:
        send_event({"type": "error", "message": "Insufficient data for analysis"})
        return

    send_event(
        {
            "type": "executing",
            "step": "base_done",
            "message": f"Base query returned {len(rows)} time periods",
            "rows": len(rows),
        }
    )

    anomalies = _detect_anomalies(rows)
    send_event(
        {
            "type": "executing",
            "step": "anomalies",
            "message": f"Detected {len(anomalies)} anomalies",
            "anomalies": anomalies,
        }
    )

    if len(anomalies) < MIN_ANOMALIES:
        send_event(
            {
                "type": "error",
                "message": f"Only {len(anomalies)} significant changes found",
            }
        )
        return

    plan_results = execute_plan(plan, anomalies, callback=send_event)

    send_event(
        {
            "type": "analyzing",
            "step": "integrating",
            "message": "Calculating contributions and patterns...",
        }
    )
    analysis = integrate_results(plan_results)

    send_event(
        {"type": "synthesizing", "step": "llm", "message": "Generating explanation..."}
    )
    explanation = _synthesize(question, anomalies, analysis)

    send_event(
        {
            "type": "done",
            "explanation": explanation,
            "anomalies": anomalies,
            "attribution": analysis.get("attribution", []),
            "patterns": analysis.get("patterns", []),
            "causal_chain": analysis.get("causal_chain", []),
        }
    )


def _detect_anomalies(rows):
    values = [float(r[-1]) if r[-1] is not None else 0 for r in rows]
    dates = [str(r[0]) for r in rows]
    anomalies = []

    for i in range(1, len(values)):
        if values[i - 1] == 0:
            continue

        bl = max(0, i - BASELINE)
        bl_mean = sum(values[bl:i]) / (i - bl)

        if bl_mean < MIN_BASELINE_VAL:
            continue

        mom = ((values[i] - values[i - 1]) / abs(values[i - 1])) * 100

        if abs(mom) >= MIN_MOM:
            anomalies.append(
                {
                    "date": dates[i],
                    "value": round(values[i], 2),
                    "baseline_mean": round(bl_mean, 2),
                    "mom_change_pct": round(mom, 1),
                    "direction": "spike" if mom > 0 else "dip",
                    "baseline_start": dates[bl],
                    "baseline_end": dates[i - 1],
                }
            )

    anomalies.sort(key=lambda x: abs(x["mom_change_pct"]), reverse=True)
    return anomalies


def _synthesize(question, anomalies, analysis):
    prompt = (
        f"Question: {question}\n\n"
        f"Anomalies: {json.dumps(anomalies, indent=2)}\n\n"
        f"Contributors: {json.dumps(analysis.get('attribution', [])[:5], indent=2)}\n\n"
        f"Causal chain: {json.dumps(analysis.get('causal_chain', []), indent=2)}\n\n"
        "Write a clear, business-focused explanation under 200 words."
    )

    try:
        return call_llm_routed(
            "You are an expert data analyst. Explain what drove the observed anomalies in plain language with specific numbers. Under 200 words.",
            prompt,
            model=MODEL_NAME,
            provider_order=["openrouter"],
            allow_fallbacks=True,
            max_tokens=400,
            temperature=0.3,
        ).strip()
    except Exception:
        return f"Found {len(anomalies)} anomalies. Top contributors: {', '.join(a.get('dimension', '') for a in analysis.get('attribution', [])[:3])}"
