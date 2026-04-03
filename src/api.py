import datetime
import json

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.answer_generator import generate_answer
from src.correlation_analyzer import CorrelationAnalyzer
from src.db import run_query
from src.history_manager import (
    delete_entry,
    get_conversation_context,
    load_history,
    save_history,
)
from src.intent_router import IntentRouter
from src.safety import is_safe_sql
from src.sql_generator import generate_sql
from src.streaming_analyzer import analyze_stream
from src.suggestion_generator import generate_suggestions
from src.vanna_logic import vn_engine

intent_router = IntentRouter()

app = FastAPI(title="Olist E-commerce NL-SQL API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SuggestionItem(BaseModel):
    question: str
    category: str
    rationale: str


class ChatRequest(BaseModel):
    question: str


class ChatResponse(BaseModel):
    answer: str
    sql: str
    row_count: int
    timestamp: str
    history_id: str | None = None
    suggestions: list[SuggestionItem] = Field(default_factory=list)
    intent: str = "standard"
    trigger_rca: bool = False
    reasoning: list[dict] = Field(default_factory=list)


class ChartRequest(BaseModel):
    question: str
    sql: str


class ChartResponse(BaseModel):
    chart_json: str


class CorrelationRequest(BaseModel):
    question: str
    sql: str


class CorrelationResponse(BaseModel):
    anomalies: list[dict] = Field(default_factory=list)
    attribution: list[dict] = Field(default_factory=list)
    patterns: list[dict] = Field(default_factory=list)
    explanation: str = ""
    breakdown_data: dict = Field(default_factory=dict)
    error: str | None = None


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    print(f"[API] ========================================")
    print(f"[API] === INCOMING REQUEST ===")
    print(f"[API] Question: {request.question}")
    print(f"[API] ========================================")

    question = request.question.strip()
    if not question:
        print(f"[API] Empty question, returning 400")
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    conversation = get_conversation_context(n=3)
    print(f"[API] Conversation history: {len(conversation)} turns")
    for i, turn in enumerate(conversation):
        print(
            f"[API]   Turn {i + 1}: Q='{turn.get('question', '')[:50]}...' SQL='{turn.get('sql', '')[:80]}...'"
        )

    print(f"[API] Calling generate_sql()...")
    try:
        sql, reasoning = generate_sql(question, conversation_history=conversation)
    except Exception as e:
        print(f"[API] === SQL GENERATION FAILED ===")
        print(f"[API] Error: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Failed to generate SQL.")

    print(f"[API] SQL generated: '{sql[:200]}...'")
    print(f"[API] Reasoning steps: {len(reasoning)}")

    ts = datetime.datetime.now().isoformat(timespec="seconds")

    # Check if the result is an analytical answer (from the agent) rather than SQL
    is_analytical = not sql or not is_safe_sql(sql)
    if is_analytical and len(reasoning) > 0:
        print(f"[API] Analytical answer from agent, returning directly")
        history_id = save_history(
            question,
            "",
            sql,
            row_count=0,
            suggestions=[],
            reasoning=reasoning,
        )
        return ChatResponse(
            answer=sql,
            sql="",
            row_count=0,
            timestamp=ts,
            history_id=history_id,
            suggestions=[],
            intent="analytical",
            trigger_rca=False,
            reasoning=reasoning,
        )

    print(f"[API] Checking SQL safety...")
    if not is_safe_sql(sql):
        print(f"[API] SQL marked as UNSAFE: {sql}")
        return ChatResponse(
            answer="I can only answer read-only questions about e-commerce data.",
            sql=sql,
            row_count=0,
            timestamp=ts,
            reasoning=reasoning,
        )

    print(f"[API] Executing SQL...")
    try:
        columns, rows = run_query(sql)
        print(
            f"[API] Query executed successfully: {len(rows)} rows, {len(columns)} columns"
        )
        print(f"[API] Columns: {columns}")
        if rows:
            print(f"[API] First row: {rows[0]}")
    except Exception as e:
        print(f"[API] === SQL EXECUTION FAILED ===")
        print(f"[API] Error: {e}")
        print(f"[API] SQL that failed: {sql}")
        import traceback

        traceback.print_exc()
        return ChatResponse(
            answer="I couldn't execute that query. Please try rephrasing.",
            sql=sql,
            row_count=0,
            timestamp=ts,
            reasoning=reasoning,
        )

    print(f"[API] Generating answer...")
    answer = generate_answer(question, sql, rows)
    print(f"[API] Generating suggestions...")
    suggestions = generate_suggestions(question, answer, sql, columns, rows)

    print(f"[API] Saving to history...")
    history_id = save_history(
        question,
        sql,
        answer,
        row_count=len(rows),
        suggestions=suggestions,
        reasoning=reasoning,
    )

    intent = intent_router.classify(question)
    trigger_rca = intent_router.should_trigger_rca(intent, len(rows), columns)

    print(f"[API] Intent: {intent}, trigger_rca: {trigger_rca}")
    print(f"[API] === REQUEST COMPLETE ===")
    print(f"[API] ========================================")

    return ChatResponse(
        answer=answer,
        sql=sql,
        row_count=len(rows),
        timestamp=ts,
        history_id=history_id,
        suggestions=suggestions,
        intent=intent,
        trigger_rca=trigger_rca,
        reasoning=reasoning,
    )


@app.post("/generate_chart", response_model=ChartResponse)
async def generate_chart(request: ChartRequest):
    if not is_safe_sql(request.sql):
        raise HTTPException(status_code=400, detail="Unsafe SQL")

    try:
        print(f"[CHART API] Request: question='{request.question.strip()[:80]}'")
        print(f"[CHART API] SQL: {request.sql[:120]}...")
        df = vn_engine.run_sql(request.sql)
        if df.empty:
            print(f"[CHART API] Query returned empty DataFrame")
            raise HTTPException(
                status_code=400, detail="No data available for charting"
            )
        print(
            f"[CHART API] DataFrame shape: {df.shape}, columns: {df.columns.tolist()}"
        )
        df = vn_engine.prepare_dataframe_for_charting(df)
        print(f"[CHART API] After prepare: dtypes={df.dtypes.to_dict()}")
        fig = vn_engine.get_deterministic_figure(df, title=request.question.strip())

        if fig is None:
            print(f"[CHART API] get_deterministic_figure returned None")
            raise HTTPException(
                status_code=400,
                detail="Chart type not supported for this data shape. Try a different query.",
            )

        try:
            chart_json = fig.to_json()
            print(f"[CHART API] Chart JSON size: {len(chart_json)} bytes")
            return ChartResponse(chart_json=chart_json)
        except Exception as json_exc:
            print(f"[CHART API] Failed to serialize chart to JSON: {json_exc}")
            raise HTTPException(
                status_code=500, detail="Failed to serialize chart data."
            )
    except HTTPException:
        raise
    except Exception as exc:
        print(f"Chart Error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/analyze")
async def analyze_endpoint(request: ChatRequest):
    async def event_stream():
        events = []

        def capture(event):
            events.append(event)

        analyze_stream(request.question, capture)

        for e in events:
            yield f"data: {json.dumps(e)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/history")
async def get_history():
    return load_history()


@app.post("/correlation_analysis", response_model=CorrelationResponse)
async def correlation_analysis(request: CorrelationRequest):
    try:
        columns, rows = run_query(request.sql)
        if not rows:
            return CorrelationResponse(error="No data returned from query")

        analyzer = CorrelationAnalyzer()
        result = analyzer.analyze(
            question=request.question,
            sql=request.sql,
            columns=columns,
            rows=rows,
        )
        if "error" in result:
            return CorrelationResponse(error=result["error"])
        return CorrelationResponse(
            anomalies=result.get("anomalies", []),
            attribution=result.get("attribution", []),
            patterns=result.get("patterns", []),
            explanation=result.get("explanation", ""),
            breakdown_data=result.get("breakdown_data", {}),
        )
    except Exception as exc:
        print(f"[CORR API] Error: {exc}")
        return CorrelationResponse(error=str(exc))


@app.delete("/history/{idx}")
async def remove_history_entry(idx: int):
    delete_entry(idx)
    return {"ok": True}


app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
