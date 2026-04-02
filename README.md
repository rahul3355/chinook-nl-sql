# Chinook NL-SQL — Natural Language to SQL Query App

A lightweight Python + FastAPI app that lets you ask plain English questions about a SQLite database and get plain English answers back. Built on the Chinook sample database, querying only the `Customer` table.

**Stack:** Python · FastAPI · SQLite · OpenRouter API · Vanilla HTML/CSS/JS

---

## Live Demo

Ask questions like:
- *How many customers are there?*
- *Which country has the most customers?*
- *List all customers from Germany*
- *Which customers have a phone number but no fax?*
- *How many customers are assigned to each support rep?*

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

Create a `.env` file in the project root with this content:

```
OPENROUTER_API_KEY=your_openrouter_key_here
```

Get a free key at [openrouter.ai](https://openrouter.ai).

---

### Step 4 — Run the app

```
uvicorn src.api:app --host 0.0.0.0 --port 8000
```

Open **http://localhost:8000** in your browser.

---

## Project Structure

```
chinook-nl-sql/
├── data/
│   └── Chinook.sqlite          # SQLite database
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
│   ├── sql_generator.py        # NL → SQL via LLM
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
{ "question": "How many customers are there?" }

// Response
{
  "answer": "There are 59 customers in total.",
  "sql": "SELECT COUNT(*) FROM Customer",
  "row_count": 1,
  "timestamp": "2026-04-03T09:15:00"
}
```

---

## How It Works

```
User question
    │
    ▼
generate_sql()  →  LLM Call #1  →  SQL query
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
