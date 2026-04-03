import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.answer_generator import generate_answer
from src.db import run_query
from src.history_manager import delete_entry, load_history, save_history
from src.safety import is_safe_sql
from src.sql_generator import generate_sql
from src.suggestion_generator import generate_suggestions
from src.vanna_logic import vn_engine

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


class ChartRequest(BaseModel):
    question: str
    sql: str


class ChartResponse(BaseModel):
    chart_json: str


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    question = request.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    try:
        sql = generate_sql(question)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to generate SQL.")

    ts = datetime.datetime.now().isoformat(timespec="seconds")

    if not is_safe_sql(sql):
        return ChatResponse(
            answer="I can only answer read-only questions about e-commerce data.",
            sql=sql,
            row_count=0,
            timestamp=ts,
        )

    try:
        columns, rows = run_query(sql)
    except Exception:
        return ChatResponse(
            answer="I couldn't execute that query. Please try rephrasing.",
            sql=sql,
            row_count=0,
            timestamp=ts,
        )

    answer = generate_answer(question, sql, rows)
    suggestions = generate_suggestions(question, answer, sql, columns, rows)
    history_id = save_history(
        question,
        sql,
        answer,
        row_count=len(rows),
        suggestions=suggestions,
    )

    return ChatResponse(
        answer=answer,
        sql=sql,
        row_count=len(rows),
        timestamp=ts,
        history_id=history_id,
        suggestions=suggestions,
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


@app.get("/history")
async def get_history():
    return load_history()


@app.delete("/history/{idx}")
async def remove_history_entry(idx: int):
    delete_entry(idx)
    return {"ok": True}


app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
