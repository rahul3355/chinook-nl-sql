# Olist E-commerce NL-SQL — Business Intelligence Assistant

A powerful Python + FastAPI app that lets you ask complex business questions about the Olist Brazilian E-commerce dataset in plain English. Built with Vanna AI for robust multi-table SQL generation.

**Stack:** Python · FastAPI · SQLite · Vanna AI · OpenRouter · Vanilla HTML/CSS/JS

---

## Live Demo

Ask complex questions like:
- *What is the total revenue per product category?*
- *Which state has the highest revenue?*
- *Show the top 5 product categories by revenue in English.*
- *What is the average delivery time in days?*
- *Which payment method is most popular?*

The app converts your question to SQL, executes it safely, and returns a plain English answer.

---

## Setup

### Step 1 — Clone and enter the folder

**Command Prompt (CMD):**
```
git clone https://github.com/rahul3355/chinook-nl-sql.git
cd chinook-nl-sql
```

---

### Step 2 — Create virtual environment and install dependencies

**Command Prompt (CMD):**
```
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

**PowerShell:**
```
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

### Step 3 — Add your API key

Copy the example file and add your API key:

```
copy .env.example .env
```

Then edit `.env` and replace the placeholder with your actual key:

```
OPENROUTER_API_KEY=your_openrouter_key_here
```

Get a free key at [openrouter.ai](https://openrouter.ai).

---

### Step 4 — Run the app

```
uvicorn src.api:app --host localhost --port 8000
```

Open **http://localhost:8000** in your browser.

---

## Project Structure

```
chinook-nl-sql/
├── data/
│   └── olist.sqlite            # Brazilian E-commerce database (11 tables)
├── prompts/
│   ├── schema.txt              # Customer table schema fed to LLM
│   ├── sql_system_prompt.txt   # Instructions for SQL generation
│   └── answer_system_prompt.txt# Instructions for answer generation
├── history/
│   └── query_history.json      # Auto-saved query log
├── frontend/
│   ├── index.html              # Chat UI
│   ├── styles.css              # Premium dark sidebar + clean chat styles
│   └── script.js               # All client-side interactivity
├── src/
│   ├── config.py               # Env vars and constants
│   ├── db.py                   # SQLite connection and queries
│   ├── llm.py                  # OpenRouter API wrapper
│   ├── vanna_logic.py          # Custom Vanna AI engine (Direct Schema)
│   ├── sql_generator.py        # NL → SQL via Vanna
│   ├── answer_generator.py     # Results → English via LLM
│   ├── safety.py               # Blocks unsafe SQL (only SELECT/WITH allowed)
│   ├── history_manager.py      # Save/load/delete query history
│   ├── api.py                  # FastAPI backend
│   └── main.py                 # CLI entry point (terminal mode)
├── .env.example
├── requirements.txt
└── README.md
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/chat` | Submit a question, get SQL + English answer |
| `GET` | `/history` | Load all saved queries |
| `DELETE` | `/history/{idx}` | Delete a history entry by index |

### POST `/chat`

```json
// Request
{ "question": "What is the total revenue?" }

// Response
{
  "answer": "The total revenue across all orders is R$ 13,591,643.70.",
  "sql": "SELECT SUM(price) FROM order_items",
  "row_count": 1,
  "timestamp": "2026-04-03T12:30:00"
}
```

---

## How It Works

```
User question
    │
    ▼
vanna_logic.py (Direct Schema RAG)
    │
    ▼
generate_sql()  →  LLM Call (Grok)  →  SQL query
    │
    ▼
safety check    →  block if not SELECT/WITH
    │
    ▼
run_query()     →  SQLite (< 1ms)
    │
    ▼
generate_answer()  →  LLM Call #2  →  English answer
    │
    ▼
save_history()  →  JSON file
```

---

## Safety

Only `SELECT` and `WITH` queries are allowed. Any SQL containing `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`, `ATTACH`, `DETACH`, or `PRAGMA` is blocked before execution.

---

## Model

Configured via `src/config.py`. Defaults to `x-ai/grok-4.1-fast` via OpenRouter. Swap to any OpenRouter-compatible model by changing `MODEL_NAME`.

---

## The Bigger Picture

In the current form, it is mostly a proof of concept showing that natural language to SQL works reliably.

Real-world usefulness starts when you move from toy customer tables to business data that people actually ask questions about every day.

For example, if you had an ecommerce SQLite or PostgreSQL database with orders, customers, products, refunds, and campaigns, then someone non-technical could ask:

- *"Which products made the most revenue last month?"*
- *"Which customers have not purchased in 90 days?"*
- *"What country has the highest average order value?"*
- *"Which campaigns bring the highest lifetime value customers?"*
- *"How many repeat buyers do we have?"*
- *"Which support rep has the most customers?"*

That becomes genuinely useful for founders, marketers, sales teams, support teams, and operations people because they no longer need to know SQL.

**A few realistic use cases:**

- Small ecommerce business owner querying orders and customer trends
- Agency querying ad campaign performance for clients
- Sales manager querying CRM data
- SaaS founder querying subscription and churn data
- Recruiter querying candidate pipelines
- Finance team querying invoice and payment data
- Operations team querying warehouse or shipping data

The value is not really "chatbot". The value is reducing the friction between business questions and database answers.

Right now, normally the flow is:

> Business person → analyst → SQL → answer

With a system like this it becomes:

> Business person → answer directly

That saves time and makes data accessible to non-technical people.

The bigger opportunity is when you move beyond one-shot SQL generation and start layering:

- Saved question history
- Dashboards
- Scheduled reports
- Anomaly detection
- Plain English summaries
- Charts
- Follow-up questions
- Multi-table joins
- User permissions
- CSV upload
- Multiple databases
- Memory of prior questions
- Business-specific metric definitions

Then it starts becoming closer to an internal analytics assistant.

For example, imagine a founder logs in every morning and asks:

- *"What changed yesterday?"*
- *"Why are conversions down?"*
- *"Which customers are most valuable?"*
- *"Which campaigns are wasting money?"*

That is where this becomes commercially valuable rather than just technically impressive.

---

## License

MIT
