import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.sql_generator import generate_sql
from src.safety import is_safe_sql
from src.db import run_query
from src.answer_generator import generate_answer
from src.history_manager import save_history, load_history, delete_entry

app = FastAPI(title="Chinook NL-SQL API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    question: str


class ChatResponse(BaseModel):
    answer: str
    sql: str
    row_count: int
    timestamp: str


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
            answer="I can only answer read-only questions about customer data.",
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
    save_history(question, sql, answer)

    return ChatResponse(answer=answer, sql=sql, row_count=len(rows), timestamp=ts)


@app.get("/history")
async def get_history():
    return load_history()


@app.delete("/history/{idx}")
async def remove_history_entry(idx: int):
    delete_entry(idx)
    return {"ok": True}


# Must be last — serves frontend/index.html at "/"
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
