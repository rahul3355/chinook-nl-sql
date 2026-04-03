import json
from dataclasses import dataclass, field

from src.config import MODEL_NAME
from src.llm import call_llm_routed
from src.structured_output import extract_json_payload
from src.vanna_logic import vn_engine


@dataclass
class SubQuery:
    id: str
    description: str
    tables: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    metrics: list[str] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)
    date_filter: bool = True
    sql: str = ""
    result: dict = field(default_factory=dict)
    status: str = "pending"
    execution_time: float = 0.0


@dataclass
class QueryPlan:
    question: str
    base_query_description: str
    base_query_sql: str
    subqueries: list[SubQuery] = field(default_factory=list)
    execution_order: list[str] = field(default_factory=list)


PLANNER_PROMPT = """You are an expert data analyst. Given a question about an e-commerce database, decompose it into subqueries for deep analysis.

Schema:
{schema}

Return ONLY valid JSON:
{{
  "base_query_description": "What the base time-series query answers",
  "base_query_sql": "Complete SQL returning month + metric time series",
  "subqueries": [
    {{"id": "Q1", "description": "...", "tables": ["customers", "orders"], "dimensions": ["customer_state"], "metrics": ["COUNT(DISTINCT o.order_id)"], "depends_on": []}}
  ]
}}

Rules:
- base_query_sql must use strftime('%Y-%m', o.order_purchase_timestamp) as month
- Use 'o' for orders, 'oi' for order_items, 'c' for customers, 'op' for order_payments, 'p' for products, 's' for sellers, 'r' for order_reviews
- Filter by order_status = 'delivered' when analyzing delivered orders
- Subqueries analyze dimensions that explain anomalies
- Return ONLY JSON, no markdown"""


def decompose_question(question: str) -> QueryPlan:
    sg = vn_engine.schema_graph
    schema_parts = []
    for tn, t in sg.tables.items():
        dims = [cn for cn, c in t.columns.items() if c.role == "dimension"]
        metrics = [cn for cn, c in t.columns.items() if c.role == "metric"]
        times = [cn for cn, c in t.columns.items() if c.role == "time"]
        schema_parts.append(f"{tn}: dimensions={dims}, metrics={metrics}, time={times}")
    for r in sg.relationships:
        schema_parts.append(
            f"  {r.source_table}.{r.source_column} -> {r.target_table}.{r.target_column}"
        )
    schema_ctx = "\n".join(schema_parts)

    raw = call_llm_routed(
        PLANNER_PROMPT.format(schema=schema_ctx),
        f"Question: {question}\n\nDecompose into subqueries.",
        model=MODEL_NAME,
        provider_order=["openrouter"],
        allow_fallbacks=True,
        max_tokens=1500,
        temperature=0.2,
    )
    parsed = extract_json_payload(raw, default={})
    if not isinstance(parsed, dict) or not parsed.get("base_query_sql"):
        return _fallback_plan(question)

    plan = QueryPlan(
        question=question,
        base_query_description=parsed.get("base_query_description", ""),
        base_query_sql=parsed["base_query_sql"],
    )
    for sq in parsed.get("subqueries", []):
        plan.subqueries.append(
            SubQuery(
                id=sq.get("id", f"Q{len(plan.subqueries) + 1}"),
                description=sq.get("description", ""),
                tables=sq.get("tables", []),
                dimensions=sq.get("dimensions", []),
                metrics=sq.get("metrics", []),
                depends_on=sq.get("depends_on", []),
            )
        )

    plan.execution_order = _topo_sort(plan.subqueries)
    return plan


def _topo_sort(subqueries):
    order, visited, visiting = [], set(), set()

    def visit(sid):
        if sid in visited or sid in visiting:
            return
        visiting.add(sid)
        sq = next((s for s in subqueries if s.id == sid), None)
        if sq:
            for dep in sq.depends_on:
                visit(dep)
        visiting.remove(sid)
        visited.add(sid)
        order.append(sid)

    for sq in subqueries:
        visit(sq.id)
    return order


def _fallback_plan(question):
    return QueryPlan(
        question=question,
        base_query_description="Monthly revenue for delivered orders",
        base_query_sql="SELECT strftime('%Y-%m', o.order_purchase_timestamp) as month, SUM(oi.price) as revenue, COUNT(DISTINCT o.order_id) as order_count FROM orders o JOIN order_items oi ON o.order_id = oi.order_id WHERE o.order_status = 'delivered' GROUP BY 1 ORDER BY 1",
        subqueries=[
            SubQuery(
                id="Q1",
                description="Orders by customer state",
                tables=["customers", "orders"],
                dimensions=["customer_state"],
                metrics=["COUNT(DISTINCT o.order_id)"],
            ),
            SubQuery(
                id="Q2",
                description="Payment type analysis",
                tables=["order_payments", "orders"],
                dimensions=["payment_type"],
                metrics=["COUNT(DISTINCT o.order_id)", "AVG(payment_value)"],
            ),
            SubQuery(
                id="Q3",
                description="Revenue by product category",
                tables=[
                    "products",
                    "order_items",
                    "orders",
                    "product_category_name_translation",
                ],
                dimensions=["product_category_name_english"],
                metrics=["SUM(oi.price)"],
            ),
            SubQuery(
                id="Q4",
                description="Revenue by seller state",
                tables=["sellers", "order_items", "orders"],
                dimensions=["seller_state"],
                metrics=["SUM(oi.price)"],
            ),
            SubQuery(
                id="Q5",
                description="Review score distribution",
                tables=["order_reviews", "orders"],
                dimensions=["review_score"],
                metrics=["COUNT(DISTINCT o.order_id)", "AVG(review_score)"],
            ),
        ],
        execution_order=["Q1", "Q2", "Q3", "Q4", "Q5"],
    )
