import json
import re

from src.config import MODEL_NAME
from src.db import run_query
from src.llm import call_reasoning_llm
from src.vanna_logic import vn_engine


ANALYTICAL_AGENT_SYSTEM_PROMPT = """You are an expert data analyst exploring an e-commerce database to answer a user's question.

DATABASE SCHEMA SUMMARY:
{schema}

AVAILABLE TABLES:
- orders: order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
- order_items: order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value
- customers: customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
- products: product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm
- sellers: seller_id, seller_zip_code_prefix, seller_city, seller_state
- order_payments: order_id, payment_sequential, payment_type, payment_installments, payment_value
- order_reviews: review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp
- product_category_name_translation: product_category_name, product_category_name_english

RELATIONSHIPS:
- orders.customer_id → customers.customer_id
- orders.order_id → order_items.order_id
- orders.order_id → order_payments.order_id
- orders.order_id → order_reviews.order_id
- order_items.product_id → products.product_id
- order_items.seller_id → sellers.seller_id
- products.product_category_name → product_category_name_translation.product_category_name

YOUR TASK:
Answer the user's question by exploring the data step by step.

At each step, you can:
1. EXPLORE_TABLE: Get the schema and sample data from a table you haven't explored yet
2. QUERY: Execute a SQL query to get specific data
3. ANSWER: You have enough information to answer the question

RESPONSE FORMAT:
You must respond with a JSON object:
{{
  "action": "explore_table" | "query" | "answer",
  "table": "table_name" (only for explore_table),
  "sql": "SELECT ..." (only for query),
  "reasoning": "Why you're taking this step"
}}

RULES:
1. Start by exploring the most relevant table for the question
2. After exploring a table, run a query to get the data you need
3. Use intermediate results to decide what to explore next
4. Build queries incrementally — start simple, add complexity as needed
5. When you have enough information, answer the question
6. Maximum 8 steps
7. For time-based questions, always start with the orders table
8. For revenue/price questions, always include the order_items table
9. For customer-related questions, always include the customers table
10. For category questions, always include products and product_category_name_translation tables"""


class AnalyticalAgent:
    def __init__(self):
        self.schema_graph = vn_engine.schema_graph
        self.state = {
            "question": "",
            "steps": [],
            "explored_tables": set(),
            "intermediate_results": {},
            "reasoning_history": [],
        }

    def answer(self, question: str) -> dict:
        print(f"[AGENT] === STARTING ANALYSIS ===")
        print(f"[AGENT] Question: {question}")

        self.state = {
            "question": question,
            "steps": [],
            "explored_tables": set(),
            "intermediate_results": {},
            "reasoning_history": [],
        }

        max_steps = 8
        step = 0

        while step < max_steps:
            print(f"[AGENT] --- Step {step + 1}/{max_steps} ---")

            decision = self._reason_next_step()
            action = decision.get("action", "")
            reasoning = decision.get("reasoning", "")

            print(f"[AGENT] Decision: action={action}")
            print(f"[AGENT] Reasoning: {reasoning[:200]}...")

            self.state["reasoning_history"].append(
                {
                    "step": step + 1,
                    "action": action,
                    "reasoning": reasoning,
                }
            )

            if action == "answer":
                print(f"[AGENT] Answering question...")
                final_answer = self._synthesize_answer()
                return {
                    "answer": final_answer,
                    "steps": self.state["steps"],
                    "reasoning": self.state["reasoning_history"],
                }

            if action == "explore_table":
                table_name = decision.get("table", "")
                if not table_name or table_name in self.state["explored_tables"]:
                    print(f"[AGENT] Table already explored or invalid: {table_name}")
                    step += 1
                    continue

                print(f"[AGENT] Exploring table: {table_name}")
                schema = self._get_table_schema(table_name)
                sample_row = self._get_sample_row(table_name)

                self.state["explored_tables"].add(table_name)
                self.state["steps"].append(
                    {
                        "step": step + 1,
                        "action": "explore",
                        "table": table_name,
                        "schema": schema,
                        "sample_row": sample_row,
                        "reasoning": reasoning,
                    }
                )
                print(f"[AGENT] Schema: {list(schema.get('columns', {}).keys())}")
                print(f"[AGENT] Sample: {sample_row}")

            if action == "query":
                sql = decision.get("sql", "")
                if not sql:
                    print(f"[AGENT] Empty SQL, skipping")
                    step += 1
                    continue

                print(f"[AGENT] Executing query: {sql[:200]}...")
                try:
                    columns, rows = run_query(sql)
                    print(f"[AGENT] Result: {len(rows)} rows, columns: {columns}")

                    self.state["intermediate_results"][f"step_{step}"] = {
                        "sql": sql,
                        "columns": columns,
                        "rows": rows[:10],
                        "row_count": len(rows),
                    }
                    self.state["steps"].append(
                        {
                            "step": step + 1,
                            "action": "query",
                            "sql": sql,
                            "result_summary": f"{len(rows)} rows, columns: {columns}",
                            "result_preview": str(rows[:3])[:200]
                            if rows
                            else "No results",
                            "reasoning": reasoning,
                        }
                    )
                except Exception as e:
                    print(f"[AGENT] Query failed: {e}")
                    self.state["steps"].append(
                        {
                            "step": step + 1,
                            "action": "query_error",
                            "sql": sql,
                            "error": str(e),
                            "reasoning": reasoning,
                        }
                    )

            step += 1

        print(f"[AGENT] Max steps reached, synthesizing answer...")
        final_answer = self._synthesize_answer()
        return {
            "answer": final_answer,
            "steps": self.state["steps"],
            "reasoning": self.state["reasoning_history"],
        }

    def _reason_next_step(self) -> dict:
        context = self._build_context()

        messages = [
            {
                "role": "system",
                "content": ANALYTICAL_AGENT_SYSTEM_PROMPT.format(
                    schema=self._build_schema_summary()
                ),
            },
            {"role": "user", "content": context},
        ]

        print(f"[AGENT] Calling reasoning LLM with {len(messages)} messages...")
        reasoning_details, content = call_reasoning_llm(messages, max_tokens=35000)
        print(f"[AGENT] LLM response: {len(content)} chars")
        print(f"[AGENT] Content preview: {content[:300]}...")

        decision = self._parse_decision(content)
        print(f"[AGENT] Parsed decision: {decision}")

        return decision

    def _parse_decision(self, content: str) -> dict:
        content = content.strip()

        json_match = re.search(r"```json\s*(.+?)\s*```", content, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass

        json_match = re.search(r"```\s*(.+?)\s*```", content, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass

        json_match = re.search(r'\{[^{}]*"action"[^{}]*\}', content, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(0))
            except json.JSONDecodeError:
                pass

        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

        return {
            "action": "answer",
            "reasoning": "Could not parse decision, attempting to answer.",
        }

    def _get_table_schema(self, table_name: str) -> dict:
        table = self.schema_graph.tables.get(table_name)
        if not table:
            return {"columns": {}}

        return {
            "columns": {
                cn: {"type": c.dtype, "role": c.role} for cn, c in table.columns.items()
            }
        }

    def _get_sample_row(self, table_name: str) -> list:
        try:
            _, rows = run_query(f"SELECT * FROM {table_name} LIMIT 1")
            return rows[0] if rows else []
        except Exception:
            return []

    def _build_context(self) -> str:
        parts = [f"Question: {self.state['question']}", ""]

        if self.state["steps"]:
            parts.append("Steps completed so far:")
            for step in self.state["steps"]:
                parts.append(f"  Step {step['step']}: {step['action']}")
                if step.get("table"):
                    parts.append(f"    Table: {step['table']}")
                    parts.append(
                        f"    Schema: {list(step.get('schema', {}).get('columns', {}).keys())}"
                    )
                    parts.append(f"    Sample: {step.get('sample_row', [])}")
                if step.get("result_summary"):
                    parts.append(f"    Result: {step['result_summary']}")
                    parts.append(f"    Preview: {step.get('result_preview', '')}")
                if step.get("error"):
                    parts.append(f"    Error: {step['error']}")
                if step.get("reasoning"):
                    parts.append(f"    Reasoning: {step['reasoning'][:200]}")
                parts.append("")
        else:
            parts.append(
                "No steps completed yet. Start by exploring the most relevant table."
            )

        parts.append("What should I do next? Respond with a JSON object.")
        return "\n".join(parts)

    def _build_schema_summary(self) -> str:
        parts = []
        for table_name, table in self.schema_graph.tables.items():
            cols = list(table.columns.keys())
            parts.append(f"- {table_name}: {', '.join(cols)}")
        return "\n".join(parts)

    def _synthesize_answer(self) -> str:
        context = self._build_context()

        messages = [
            {
                "role": "system",
                "content": (
                    "You are an expert data analyst. Based on the analysis steps and results, "
                    "synthesize a clear, concise answer to the user's question. "
                    "Use specific numbers and data from the results. "
                    "Write in a natural, conversational tone. "
                    "Keep it under 300 words. "
                    "Do NOT use markdown formatting like **bold** or *italic*. "
                    "Do NOT use em dashes (—). Use regular hyphens or commas instead."
                ),
            },
            {"role": "user", "content": context},
        ]

        print(f"[AGENT] Synthesizing answer...")
        _, content = call_reasoning_llm(messages, max_tokens=35000)

        content = (
            content.replace("**", "")
            .replace("*", "")
            .replace("\u2014", "-")
            .replace("\u2013", "-")
            .replace("\u2015", "-")
        )
        print(f"[AGENT] Final answer: {content[:300]}...")
        return content
