import sqlite3
import pandas as pd
from vanna.base import VannaBase
from src.llm import call_llm
from src.config import DB_PATH


class DirectSchemaVanna(VannaBase):
    def __init__(self, config=None):
        super().__init__(config=config)
        self.db_path = DB_PATH

        # Olist DDL (all 11 tables)
        self.ddl = """
        CREATE TABLE customers (
            customer_id TEXT PRIMARY KEY,
            customer_unique_id TEXT,
            customer_zip_code_prefix INTEGER,
            customer_city TEXT,
            customer_state TEXT
        );

        CREATE TABLE geolocation (
            geolocation_zip_code_prefix INTEGER,
            geolocation_lat REAL,
            geolocation_lng REAL,
            geolocation_city TEXT,
            geolocation_state TEXT
        );

        CREATE TABLE order_items (
            order_id TEXT,
            order_item_id INTEGER,
            product_id TEXT,
            seller_id TEXT,
            shipping_limit_date TEXT,
            price REAL,
            freight_value REAL,
            PRIMARY KEY (order_id, order_item_id)
        );

        CREATE TABLE order_payments (
            order_id TEXT,
            payment_sequential INTEGER,
            payment_type TEXT,
            payment_installments INTEGER,
            payment_value REAL,
            PRIMARY KEY (order_id, payment_sequential)
        );

        CREATE TABLE order_reviews (
            review_id TEXT PRIMARY KEY,
            order_id TEXT,
            review_score INTEGER,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TEXT,
            review_answer_timestamp TEXT
        );

        CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            order_status TEXT,
            order_purchase_timestamp TEXT,
            order_approved_at TEXT,
            order_delivered_carrier_date TEXT,
            order_delivered_customer_date TEXT,
            order_estimated_delivery_date TEXT
        );

        CREATE TABLE products (
            product_id TEXT PRIMARY KEY,
            product_category_name TEXT,
            product_name_lenght REAL,
            product_description_lenght REAL,
            product_photos_qty REAL,
            product_weight_g REAL,
            product_length_cm REAL,
            product_height_cm REAL,
            product_width_cm REAL
        );

        CREATE TABLE sellers (
            seller_id TEXT PRIMARY KEY,
            seller_zip_code_prefix INTEGER,
            seller_city TEXT,
            seller_state TEXT
        );

        CREATE TABLE product_category_name_translation (
            product_category_name TEXT PRIMARY KEY,
            product_category_name_english TEXT
        );

        CREATE TABLE leads_qualified (
            mql_id TEXT PRIMARY KEY,
            first_contact_date TEXT,
            landing_page_id TEXT,
            origin TEXT
        );

        CREATE TABLE leads_closed (
            mql_id TEXT PRIMARY KEY,
            seller_id TEXT,
            sdr_id TEXT,
            sr_id TEXT,
            won_date TEXT,
            business_segment TEXT,
            lead_type TEXT,
            lead_behaviour_profile TEXT,
            has_company INTEGER,
            has_gtin INTEGER,
            average_stock TEXT,
            business_type TEXT,
            declared_product_catalog_size REAL,
            declared_monthly_revenue REAL
        );
        """

        self.documentation = [
            "Revenue is the sum of the 'price' column in the 'order_items' table.",
            "Total freight is the sum of the 'freight_value' column in 'order_items'.",
            "An order can have multiple items, each as a row in 'order_items'.",
            "To get English names for categories, join 'products' with 'product_category_name_translation'.",
            "Orders are linked to customers via 'customer_id'.",
            "Sellers are in the 'sellers' table, linked to 'order_items' via 'seller_id'.",
            "Review scores (1-5) are in 'order_reviews'.",
            "Lead data is in 'leads_qualified' and 'leads_closed', linked via 'mql_id'.",
            "IMPORTANT: 'customer_id' is unique per order. To track a single human across multiple orders (Retention/LTV), you MUST use 'customer_unique_id' from the 'customers' table.",
            "To calculate Month-over-Month (MoM) growth, use a Common Table Expression (CTE) and the 'LAG' window function on aggregated revenue.",
            "IMPORTANT: Olist products do NOT have human-readable names (e.g. 'iPhone'). They are identified by 'product_id'. When a user asks for 'products', ALWAYS return the 'product_category_name_english' from the translation table so they can understand what the products are.",
        ]

        # Gold Standard Q&A for high accuracy
        self.train_data = [
            {
                "question": "What is the total revenue by order status?",
                "sql": "SELECT o.order_status, SUM(oi.price) as revenue, COUNT(DISTINCT o.order_id) as order_count FROM orders o JOIN order_items oi ON o.order_id = oi.order_id GROUP BY o.order_status ORDER BY revenue DESC",
            },
            {
                "question": "How much does each state contribute to total revenue?",
                "sql": "SELECT c.customer_state, SUM(oi.price) as revenue, COUNT(DISTINCT o.order_id) as order_count, ROUND(AVG(oi.price), 2) as avg_order_value FROM orders o JOIN order_items oi ON o.order_id = oi.order_id JOIN customers c ON o.customer_id = c.customer_id GROUP BY c.customer_state ORDER BY revenue DESC",
            },
            {
                "question": "What is the average order value including freight?",
                "sql": "SELECT o.order_id, SUM(oi.price + oi.freight_value) as total_value FROM orders o JOIN order_items oi ON o.order_id = oi.order_id WHERE o.order_status = 'delivered' GROUP BY o.order_id ORDER BY total_value DESC LIMIT 10",
            },
            {
                "question": "Which month had the highest delivered revenue?",
                "sql": "SELECT strftime('%Y-%m', o.order_purchase_timestamp) as month, SUM(oi.price) as revenue, COUNT(DISTINCT o.order_id) as order_count FROM orders o JOIN order_items oi ON o.order_id = oi.order_id WHERE o.order_status = 'delivered' GROUP BY 1 ORDER BY revenue DESC LIMIT 12",
            },
            {
                "question": "What percentage of orders were cancelled vs delivered?",
                "sql": "SELECT o.order_status, COUNT(DISTINCT o.order_id) as order_count, ROUND(COUNT(DISTINCT o.order_id) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM orders), 1) as pct FROM orders o GROUP BY o.order_status ORDER BY order_count DESC",
            },
            {
                "question": "Which product categories generate the most revenue in São Paulo?",
                "sql": "SELECT t.product_category_name_english, SUM(oi.price) as revenue, COUNT(DISTINCT oi.order_id) as order_count FROM order_items oi JOIN orders o ON oi.order_id = o.order_id JOIN customers c ON o.customer_id = c.customer_id JOIN products p ON oi.product_id = p.product_id JOIN product_category_name_translation t ON p.product_category_name = t.product_category_name WHERE c.customer_state = 'SP' GROUP BY t.product_category_name_english ORDER BY revenue DESC LIMIT 10",
            },
            {
                "question": "How does average freight value vary by customer state?",
                "sql": "SELECT c.customer_state, ROUND(AVG(oi.freight_value), 2) as avg_freight, ROUND(SUM(oi.freight_value), 2) as total_freight, COUNT(DISTINCT oi.order_id) as order_count FROM orders o JOIN order_items oi ON o.order_id = oi.order_id JOIN customers c ON o.customer_id = c.customer_id GROUP BY c.customer_state ORDER BY avg_freight DESC",
            },
            {
                "question": "Which states have the highest average review scores?",
                "sql": "SELECT c.customer_state, ROUND(AVG(r.review_score), 2) as avg_score, COUNT(DISTINCT r.order_id) as reviewed_orders FROM orders o JOIN customers c ON o.customer_id = c.customer_id JOIN order_reviews r ON o.order_id = r.order_id GROUP BY c.customer_state ORDER BY avg_score DESC",
            },
            {
                "question": "Top 5 product categories by number of sellers",
                "sql": "SELECT t.product_category_name_english, COUNT(DISTINCT oi.seller_id) as seller_count, SUM(oi.price) as revenue FROM order_items oi JOIN products p ON oi.product_id = p.product_id JOIN product_category_name_translation t ON p.product_category_name = t.product_category_name GROUP BY t.product_category_name_english ORDER BY seller_count DESC LIMIT 5",
            },
            {
                "question": "Which cities have the most repeat customers?",
                "sql": "SELECT c.customer_city, COUNT(*) as repeat_customer_count FROM (SELECT cu.customer_unique_id, cu.customer_city, COUNT(DISTINCT o.order_id) as order_count FROM customers cu JOIN orders o ON cu.customer_id = o.customer_id GROUP BY cu.customer_unique_id, cu.customer_city HAVING order_count > 1) c GROUP BY c.customer_city ORDER BY repeat_customer_count DESC LIMIT 10",
            },
            {
                "question": "What is the average payment value by payment type for delivered orders?",
                "sql": "SELECT op.payment_type, ROUND(AVG(op.payment_value), 2) as avg_payment, COUNT(DISTINCT op.order_id) as order_count, SUM(op.payment_value) as total_paid FROM orders o JOIN order_payments op ON o.order_id = op.order_id WHERE o.order_status = 'delivered' GROUP BY op.payment_type ORDER BY total_paid DESC",
            },
            {
                "question": "How many installments do customers use for high-value orders?",
                "sql": "SELECT op.payment_type, ROUND(AVG(op.payment_installments), 1) as avg_installments, ROUND(AVG(op.payment_value), 2) as avg_value FROM orders o JOIN order_payments op ON o.order_id = op.order_id WHERE op.payment_value > 500 GROUP BY op.payment_type ORDER BY avg_value DESC",
            },
            {
                "question": "Which payment method has the highest average order value?",
                "sql": "SELECT op.payment_type, ROUND(AVG(order_total.value), 2) as avg_order_value, COUNT(DISTINCT op.order_id) as order_count FROM order_payments op JOIN (SELECT order_id, SUM(price + freight_value) as value FROM order_items GROUP BY order_id) order_total ON op.order_id = order_total.order_id GROUP BY op.payment_type ORDER BY avg_order_value DESC",
            },
            {
                "question": "What is the relationship between review score and order value?",
                "sql": "SELECT r.review_score, ROUND(AVG(oi.price), 2) as avg_order_value, COUNT(DISTINCT r.order_id) as order_count FROM order_reviews r JOIN order_items oi ON r.order_id = oi.order_id GROUP BY r.review_score ORDER BY r.review_score",
            },
            {
                "question": "How many customers use multiple payment methods per order?",
                "sql": "SELECT COUNT(*) as multi_payment_orders FROM (SELECT order_id, COUNT(DISTINCT payment_type) as method_count FROM order_payments GROUP BY order_id HAVING method_count > 1)",
            },
            {
                "question": "Which sellers have the highest total revenue?",
                "sql": "SELECT s.seller_id, s.seller_state, SUM(oi.price) as revenue, COUNT(DISTINCT oi.order_id) as order_count, COUNT(DISTINCT oi.product_id) as product_count FROM order_items oi JOIN sellers s ON oi.seller_id = s.seller_id GROUP BY s.seller_id, s.seller_state ORDER BY revenue DESC LIMIT 10",
            },
            {
                "question": "What is the average delivery time by seller state?",
                "sql": "SELECT s.seller_state, ROUND(AVG(julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp)), 1) as avg_delivery_days, COUNT(DISTINCT o.order_id) as order_count FROM orders o JOIN order_items oi ON o.order_id = oi.order_id JOIN sellers s ON oi.seller_id = s.seller_id WHERE o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL GROUP BY s.seller_state ORDER BY avg_delivery_days",
            },
            {
                "question": "Which product categories have the highest freight-to-price ratio?",
                "sql": "SELECT t.product_category_name_english, ROUND(SUM(oi.freight_value) / SUM(oi.price) * 100, 1) as freight_pct, SUM(oi.price) as total_price, SUM(oi.freight_value) as total_freight FROM order_items oi JOIN products p ON oi.product_id = p.product_id JOIN product_category_name_translation t ON p.product_category_name = t.product_category_name GROUP BY t.product_category_name_english ORDER BY freight_pct DESC LIMIT 10",
            },
            {
                "question": "How many products does each seller offer and what is their average price?",
                "sql": "SELECT s.seller_id, s.seller_state, COUNT(DISTINCT oi.product_id) as product_count, ROUND(AVG(oi.price), 2) as avg_price, SUM(oi.price) as total_revenue FROM order_items oi JOIN sellers s ON oi.seller_id = s.seller_id GROUP BY s.seller_id, s.seller_state ORDER BY product_count DESC LIMIT 10",
            },
            {
                "question": "Which sellers serve the most customer states?",
                "sql": "SELECT s.seller_id, s.seller_state, COUNT(DISTINCT c.customer_state) as states_served, COUNT(DISTINCT o.order_id) as total_orders FROM order_items oi JOIN sellers s ON oi.seller_id = s.seller_id JOIN orders o ON oi.order_id = o.order_id JOIN customers c ON o.customer_id = c.customer_id GROUP BY s.seller_id, s.seller_state ORDER BY states_served DESC LIMIT 10",
            },
            {
                "question": "How has monthly revenue trended over time for delivered orders?",
                "sql": "SELECT strftime('%Y-%m', o.order_purchase_timestamp) as month, SUM(oi.price) as revenue, COUNT(DISTINCT o.order_id) as order_count FROM orders o JOIN order_items oi ON o.order_id = oi.order_id WHERE o.order_status = 'delivered' GROUP BY 1 ORDER BY 1",
            },
            {
                "question": "What is the average delivery delay by month?",
                "sql": "SELECT strftime('%Y-%m', o.order_purchase_timestamp) as month, ROUND(AVG(julianday(o.order_estimated_delivery_date) - julianday(o.order_delivered_customer_date)), 1) as avg_delay_days, COUNT(DISTINCT o.order_id) as delivered_count FROM orders o WHERE o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL AND o.order_estimated_delivery_date IS NOT NULL GROUP BY 1 ORDER BY 1",
            },
            {
                "question": "Which day of the week has the most order purchases?",
                "sql": "SELECT CASE CAST(strftime('%w', o.order_purchase_timestamp) AS INTEGER) WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday' WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday' WHEN 6 THEN 'Saturday' END as day_name, COUNT(DISTINCT o.order_id) as order_count, SUM(oi.price) as revenue FROM orders o JOIN order_items oi ON o.order_id = oi.order_id GROUP BY 1 ORDER BY order_count DESC",
            },
            {
                "question": "How does delivery time affect review scores?",
                "sql": "SELECT CASE WHEN julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp) <= 7 THEN 'Under 7 days' WHEN julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp) <= 14 THEN '7-14 days' WHEN julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp) <= 21 THEN '14-21 days' ELSE 'Over 21 days' END as delivery_bucket, ROUND(AVG(r.review_score), 2) as avg_review_score, COUNT(DISTINCT o.order_id) as order_count FROM orders o JOIN order_reviews r ON o.order_id = r.order_id WHERE o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL GROUP BY 1 ORDER BY avg_review_score DESC",
            },
            {
                "question": "What is the month-over-month growth in delivered orders?",
                "sql": "WITH monthly_data AS (SELECT strftime('%Y-%m', o.order_purchase_timestamp) as month, COUNT(DISTINCT o.order_id) as order_count, SUM(oi.price) as revenue FROM orders o JOIN order_items oi ON o.order_id = oi.order_id WHERE o.order_status = 'delivered' GROUP BY 1) SELECT month, order_count, revenue, LAG(order_count) OVER (ORDER BY month) as prev_orders, ROUND((order_count - LAG(order_count) OVER (ORDER BY month)) * 100.0 / LAG(order_count) OVER (ORDER BY month), 1) as order_growth_pct FROM monthly_data ORDER BY 1",
            },
        ]

    def system_message(self, **kwargs) -> str:
        return (
            "You are a SQL expert for the Olist E-commerce SQLite database. "
            "Your goal is to generate accurate, efficient SQL queries based on the provided schema. "
            "Use standard SQLite syntax. Always use table aliases and explicit joins. "
            "For loyalty, retention, or repeat customer analysis, ALWAYS use 'customer_unique_id'. "
            "Only return the SQL query, no markdown, no explanations."
        )

    def submit_prompt(self, prompt, **kwargs) -> str:
        # Use the existing LLM infrastructure
        return call_llm(self.system_message(), prompt)

    def get_related_ddl(self, question: str, **kwargs) -> list:
        return [self.ddl]

    def get_related_documentation(self, question: str, **kwargs) -> list:
        return self.documentation

    def get_similar_question_sql(self, question: str, **kwargs) -> list:
        # Return few-shot examples as context
        context = []
        for pair in self.train_data:
            context.append(f"Question: {pair['question']}\nSQL: {pair['sql']}")
        return context

    def run_sql(self, sql: str) -> pd.DataFrame:
        conn = sqlite3.connect(self.db_path)
        try:
            print(f"[CHART] run_sql executing: {sql[:150]}...")
            df = pd.read_sql_query(sql, conn)
            print(
                f"[CHART] run_sql result: {df.shape[0]} rows, {df.shape[1]} cols, dtypes={df.dtypes.to_dict()}"
            )
            return df
        except Exception as e:
            print(f"[CHART] SQL Error: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def generate_sql(self, question: str, **kwargs) -> str:
        # Construct the prompt manually since we are overriding retrieval
        ddl_context = "\n".join(self.get_related_ddl(question))
        doc_context = "\n".join(self.get_related_documentation(question))
        example_context = "\n\n".join(self.get_similar_question_sql(question))

        full_prompt = (
            f"SCHEMA DDL:\n{ddl_context}\n\n"
            f"DOCUMENTATION:\n{doc_context}\n\n"
            f"EXAMPLES:\n{example_context}\n\n"
            f"USER QUESTION: {question}\n\n"
            f"Generate the SQLite query for this question."
        )

        sql = self.submit_prompt(full_prompt)
        # Clean up common LLM artifacts if any
        sql = sql.replace("```sql", "").replace("```", "").strip()
        return sql

    def prepare_dataframe_for_charting(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize SQLite result columns so aggregates stay numeric for plotting."""
        print(
            f"[CHART] prepare_dataframe_for_charting: input shape={df.shape}, dtypes={df.dtypes.to_dict()}"
        )
        plot_df = df.copy()

        for col in plot_df.columns:
            series = plot_df[col]
            if pd.api.types.is_numeric_dtype(
                series
            ) or pd.api.types.is_datetime64_any_dtype(series):
                continue

            cleaned = series.astype(str).str.strip()
            cleaned = cleaned.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
            cleaned = (
                cleaned.str.replace(",", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.replace("%", "", regex=False)
            )
            numeric_series = pd.to_numeric(cleaned, errors="coerce")

            if (
                series.notna().sum() > 0
                and numeric_series.notna().sum() == series.notna().sum()
            ):
                print(
                    f"[CHART] Converted column '{col}' from {series.dtype} to numeric"
                )
                plot_df[col] = numeric_series

        print(
            f"[CHART] prepare_dataframe_for_charting: output dtypes={plot_df.dtypes.to_dict()}"
        )
        return plot_df

    def _looks_like_time_column(self, col_name: str) -> bool:
        name = col_name.lower()
        return any(
            keyword in name
            for keyword in ("date", "time", "month", "year", "week", "day", "quarter")
        )

    def _looks_like_id_column(self, col_name: str) -> bool:
        name = col_name.lower()
        return name == "id" or name.endswith("_id") or name.startswith("id_")

    def _metric_family(self, col_name: str) -> str:
        name = col_name.lower()
        if any(keyword in name for keyword in ("prev", "prior", "previous", "lag")):
            return "helper"
        if any(
            keyword in name for keyword in ("growth", "pct", "percent", "rate", "ratio")
        ):
            return "rate"
        if any(
            keyword in name
            for keyword in (
                "revenue",
                "rev",
                "sales",
                "price",
                "amount",
                "value",
                "profit",
                "total",
                "spent",
            )
        ):
            return "currency"
        if any(
            keyword in name
            for keyword in (
                "count",
                "orders",
                "customers",
                "qty",
                "quantity",
                "volume",
                "score",
            )
        ):
            return "count"
        return "numeric"

    def _metric_score(self, col_name: str, context: str = "") -> int:
        family = self._metric_family(col_name)
        score_map = {
            "currency": 120,
            "count": 100,
            "rate": 90,
            "numeric": 40,
            "helper": -180,
        }
        score = score_map.get(family, 0)
        context = context.lower()
        name = col_name.lower()

        if (
            any(
                keyword in context
                for keyword in ("revenue", "sales", "gmv", "ltv", "value")
            )
            and family == "currency"
        ):
            score += 240
        if (
            any(
                keyword in context
                for keyword in (
                    "count",
                    "orders",
                    "customers",
                    "quantity",
                    "volume",
                    "how many",
                )
            )
            and family == "count"
        ):
            score += 240
        if (
            any(
                keyword in context
                for keyword in ("growth", "percent", "percentage", "rate")
            )
            and family == "rate"
        ):
            score += 300
        if any(keyword in context for keyword in ("average", "avg")) and "avg" in name:
            score += 180

        if self._looks_like_id_column(col_name):
            score -= 80

        return score

    def _select_label_column(self, df: pd.DataFrame):
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        label_cols = [col for col in df.columns if col not in numeric_cols]
        if not label_cols:
            return None

        def label_score(col_name: str) -> int:
            score = 0
            name = col_name.lower()
            if self._looks_like_time_column(col_name):
                score += 300
            if any(
                keyword in name
                for keyword in (
                    "name",
                    "category",
                    "state",
                    "city",
                    "country",
                    "segment",
                    "type",
                    "month",
                )
            ):
                score += 120
            if self._looks_like_id_column(col_name):
                score -= 120
            return score

        return max(label_cols, key=label_score)

    def _select_metric_columns(self, df: pd.DataFrame, context: str, x_col: str | None):
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if not numeric_cols:
            return []

        context = context.lower()
        non_helper_cols = [
            col for col in numeric_cols if self._metric_family(col) != "helper"
        ]
        candidate_cols = non_helper_cols or numeric_cols
        scored_cols = sorted(
            ((self._metric_score(col, context), col) for col in candidate_cols),
            key=lambda item: item[0],
            reverse=True,
        )
        if not scored_cols:
            return []

        compare_query = any(
            keyword in context
            for keyword in ("compare", "comparison", "vs", "versus", "against")
        )
        time_series = bool(x_col and self._looks_like_time_column(x_col))
        positive_cols = [col for score, col in scored_cols if score >= 0]
        family_groups = {}
        for col in positive_cols:
            family_groups.setdefault(self._metric_family(col), []).append(col)

        if compare_query:
            for family in ("currency", "count", "rate", "numeric"):
                cols = family_groups.get(family, [])
                if 2 <= len(cols) <= 4:
                    return cols

        if time_series:
            for family in ("rate", "currency", "count", "numeric"):
                cols = family_groups.get(family, [])
                if 1 <= len(cols) <= 3:
                    if family == "rate" and any(
                        keyword in context
                        for keyword in ("growth", "percent", "percentage", "rate")
                    ):
                        return [cols[0]]
                    if family != "rate" and len(cols) >= 2 and compare_query:
                        return cols
                    if (
                        family != "rate"
                        and len(cols) >= 2
                        and all(self._metric_family(col) == family for col in cols)
                    ):
                        return cols[:3]
                    if cols:
                        return [cols[0]]

        return [scored_cols[0][1]]

    def _build_chart_plan(self, df: pd.DataFrame, context: str = ""):
        plot_df = self.prepare_dataframe_for_charting(df)
        numeric_cols = plot_df.select_dtypes(include=["number"]).columns.tolist()
        print(f"[CHART] DataFrame shape: {plot_df.shape}, numeric_cols: {numeric_cols}")

        if plot_df.empty or not numeric_cols:
            print(
                f"[CHART] No chart possible: empty={plot_df.empty}, numeric_cols={numeric_cols}"
            )
            return None

        if len(plot_df) == 1:
            single_row_metrics = [
                col
                for col in numeric_cols
                if self._metric_family(col) != "helper"
                and self._metric_score(col, context) >= 0
            ]
            print(f"[CHART] Single row: single_row_metrics={single_row_metrics}")
            if single_row_metrics:
                metric_cols = single_row_metrics[:4]
            else:
                metric_cols = self._select_metric_columns(plot_df, context, x_col=None)

            if not metric_cols:
                print(f"[CHART] Single row: no usable metrics, returning None")
                return None

            if len(metric_cols) == 1:
                print(f"[CHART] Plan: indicator, metric={metric_cols[0]}")
                return {"kind": "indicator", "df": plot_df, "metric_cols": metric_cols}
            print(f"[CHART] Plan: metric_bar, metrics={metric_cols[:4]}")
            return {"kind": "metric_bar", "df": plot_df, "metric_cols": metric_cols[:4]}

        x_col = self._select_label_column(plot_df)
        if not x_col:
            print(
                f"[CHART] No label column found, all columns are numeric: {plot_df.columns.tolist()}"
            )
            print(
                f"[CHART] Fallback: using first column '{plot_df.columns[0]}' as label"
            )
            x_col = plot_df.columns[0]
            numeric_cols = [c for c in plot_df.columns if c != x_col]
            if not numeric_cols:
                print(f"[CHART] Fallback failed: no remaining columns for metrics")
                return None
            metric_cols = self._select_metric_columns(plot_df, context, x_col=x_col)
        else:
            metric_cols = self._select_metric_columns(plot_df, context, x_col=x_col)

        if not metric_cols:
            print(f"[CHART] No metric columns found, returning None")
            return None

        print(f"[CHART] x_col='{x_col}', metric_cols={metric_cols}")

        time_series = self._looks_like_time_column(
            x_col
        ) or pd.api.types.is_datetime64_any_dtype(plot_df[x_col])
        label_lengths = plot_df[x_col].astype(str).str.len()
        horizontal = (not time_series) and (
            len(plot_df) > 8 or label_lengths.max() > 20 or label_lengths.mean() > 14
        )

        if time_series:
            if len(metric_cols) > 1:
                kind = "multi_line"
            elif any(
                keyword in context.lower()
                for keyword in (
                    "trend",
                    "over-time",
                    "over time",
                    "month-over-month",
                    "mom",
                    "growth",
                )
            ):
                kind = "line"
            else:
                kind = "bar"
        elif len(metric_cols) > 1:
            kind = "grouped_bar"
        else:
            kind = "horizontal_bar" if horizontal else "bar"

        print(
            f"[CHART] Plan: kind={kind}, time_series={time_series}, rows={len(plot_df)}"
        )
        return {"kind": kind, "df": plot_df, "x_col": x_col, "metric_cols": metric_cols}

    def plotly_system_message(self, **kwargs) -> str:
        return (
            "You are a Plotly Express expert. Given a pandas DataFrame 'df' and a user question, "
            "write the Python code to create a beautiful, accurate Plotly figure named 'fig'. "
            "STRICT DESIGN RULES:\n"
            "1. USE DATA COLUMNS: NEVER use the dataframe index for X or Y axes. Use the actual column names provided.\n"
            "2. MAPPING: For bar/line charts, map the categorical/time column to 'x' and the primary numeric/aggregate column to 'y'.\n"
            "3. NO GUESSING: If the data includes an aggregate column such as order_count, count, revenue, total, or value, plot that exact column on the Y axis.\n"
            "4. CORPORATE RGB COLORS: color_discrete_sequence=['#ef4444', '#22c55e', '#3b82f6'].\n"
            "5. SINGLE SERIES: If only one series, FORCE fig.update_traces(marker_color='#ef4444').\n"
            "6. VERTICAL ONLY: Use orientation='v' for bar charts and fig.update_xaxes(tickangle=45).\n"
            "7. SOLID WHITE: Set fig.update_layout(paper_bgcolor='white', plot_bgcolor='white').\n"
            "8. AXIS READABILITY: Set fig.update_layout(autosize=True, margin=dict(l=150, r=40, t=60, b=120)).\n"
            "9. FORMATTING: Use tickformat='$,.2f' for money and ',d' for counts.\n"
            "Only return Python code, NO markdown, NO explanations."
        )

    def generate_plotly_code(
        self, question: str, sql: str, df: pd.DataFrame, **kwargs
    ) -> str:
        # Construct a prompt for plotting
        plot_df = self.prepare_dataframe_for_charting(df)
        columns = ", ".join(plot_df.columns)
        # Increase sample to 10 so AI sees the data trend
        sample = plot_df.head(10).to_string()

        prompt = (
            f"Question: {question}\n"
            f"SQL: {sql}\n"
            f"DataFrame Columns: {columns}\n"
            f"DataFrame Sample (10 rows):\n{sample}\n\n"
            f"Write the Plotly Express code. Reference specific column names for x and y."
        )

        code = call_llm(self.plotly_system_message(), prompt)
        # Clean up any markdown
        code = code.replace("```python", "").replace("```", "").strip()
        return code

    def get_deterministic_figure(self, df: pd.DataFrame, title: str | None = None):
        """Rule-based plotter for common analytical result shapes without LLM fallback."""
        import plotly.graph_objects as go

        print(
            f"[CHART] get_deterministic_figure called: shape={df.shape}, columns={df.columns.tolist()}"
        )
        chart_plan = self._build_chart_plan(df, context=title or "")
        if not chart_plan:
            print(f"[CHART] No chart plan returned, cannot build figure")
            return None

        plot_df = chart_plan["df"]
        kind = chart_plan["kind"]
        metric_cols = chart_plan["metric_cols"]

        print(f"[CHART] Building figure: kind={kind}, metrics={metric_cols}")

        colors = ["#ef4444", "#f97316", "#3b82f6", "#22c55e"]
        fig = go.Figure()

        if kind == "indicator":
            metric_col = metric_cols[0]
            raw_value = plot_df.iloc[0][metric_col]
            value = pd.to_numeric(raw_value, errors="coerce")
            if pd.isna(value):
                print(
                    f"[CHART] Indicator: value for '{metric_col}' is NaN (raw={raw_value}), cannot render"
                )
                return None

            print(f"[CHART] Indicator: value={value}, metric={metric_col}")
            family = self._metric_family(metric_col)
            number_config = (
                {"valueformat": ",.0f"}
                if family == "count"
                else {"valueformat": ",.2f"}
            )
            if family == "currency":
                number_config["prefix"] = "$"
            elif family == "rate":
                number_config["suffix"] = "%"

            fig.add_trace(
                go.Indicator(
                    mode="number",
                    value=float(value),
                    number=number_config,
                    title={"text": title or metric_col},
                )
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                autosize=True,
                height=220,
                margin=dict(l=20, r=20, t=50, b=20),
                font=dict(family="Arial, sans-serif", size=14),
            )
            print(f"[CHART] Indicator figure built successfully")
            return fig

        x_col = chart_plan["x_col"]
        x_values = plot_df[x_col].tolist()
        yaxis_family = self._metric_family(metric_cols[0])

        print(
            f"[CHART] Chart: x_col='{x_col}', x_values_count={len(x_values)}, yaxis_family={yaxis_family}"
        )

        for index, metric_col in enumerate(metric_cols):
            y_series = pd.to_numeric(plot_df[metric_col], errors="coerce")
            nan_count = y_series.isna().sum()
            if nan_count > 0:
                print(
                    f"[CHART] Metric '{metric_col}': {nan_count} NaN values found, filling with 0"
                )
                y_series = y_series.fillna(0)
            y_values = y_series.tolist()

            color = colors[index % len(colors)]
            if kind in ("line", "multi_line"):
                fig.add_trace(
                    go.Scatter(
                        x=x_values,
                        y=y_values,
                        mode="lines+markers",
                        line=dict(color=color, width=3),
                        marker=dict(color=color, size=7),
                        name=metric_col,
                    )
                )
            elif kind == "horizontal_bar":
                fig.add_trace(
                    go.Bar(
                        x=y_values,
                        y=x_values,
                        orientation="h",
                        marker_color=color,
                        name=metric_col,
                    )
                )
            else:
                fig.add_trace(
                    go.Bar(
                        x=x_values,
                        y=y_values,
                        marker_color=color,
                        name=metric_col,
                    )
                )

        show_legend = len(metric_cols) > 1
        height = 540
        if kind == "horizontal_bar":
            height = min(900, max(420, 90 + len(plot_df) * 28))
        elif kind == "metric_bar":
            x_values = [col.replace("_", " ").title() for col in metric_cols]
            y_values = []
            for col in metric_cols:
                val = pd.to_numeric(plot_df.iloc[0][col], errors="coerce")
                if pd.isna(val):
                    val = 0
                y_values.append(float(val))
            fig = go.Figure(
                data=[
                    go.Bar(
                        x=x_values,
                        y=y_values,
                        marker_color=colors[: len(metric_cols)],
                        showlegend=False,
                    )
                ]
            )
            yaxis_family = self._metric_family(metric_cols[0])
            show_legend = False
            height = 420

        fig.update_layout(
            title=title,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            autosize=True,
            height=height,
            margin=dict(l=100 if kind == "horizontal_bar" else 80, r=30, t=60, b=90),
            font=dict(family="Arial, sans-serif", size=12),
            showlegend=show_legend,
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            bargap=0.2,
        )
        if kind == "grouped_bar":
            fig.update_layout(barmode="group")

        if kind == "horizontal_bar":
            fig.update_xaxes(showgrid=True, gridcolor="rgba(15,23,42,0.08)")
            fig.update_yaxes(automargin=True)
        else:
            fig.update_xaxes(tickangle=45, automargin=True)
            fig.update_yaxes(showgrid=True, gridcolor="rgba(15,23,42,0.08)")

        if kind not in ("line", "multi_line", "horizontal_bar"):
            fig.update_traces(marker_line_width=0)

        all_values = []
        for metric_col in metric_cols:
            numeric_values = (
                pd.to_numeric(plot_df[metric_col], errors="coerce").dropna().tolist()
            )
            all_values.extend(numeric_values)

        if all_values and min(all_values) >= 0:
            if kind == "horizontal_bar":
                fig.update_xaxes(rangemode="tozero")
            else:
                fig.update_yaxes(rangemode="tozero")
        else:
            if kind == "horizontal_bar":
                fig.update_xaxes(zeroline=True, zerolinecolor="rgba(15,23,42,0.25)")
            else:
                fig.update_yaxes(zeroline=True, zerolinecolor="rgba(15,23,42,0.25)")

        if yaxis_family == "currency":
            if kind == "horizontal_bar":
                fig.update_xaxes(tickprefix="$", tickformat=",.2f")
            else:
                fig.update_yaxes(tickprefix="$", tickformat=",.2f")
        elif yaxis_family == "rate":
            if kind == "horizontal_bar":
                fig.update_xaxes(ticksuffix="%", tickformat=",.2f")
            else:
                fig.update_yaxes(ticksuffix="%", tickformat=",.2f")
        else:
            if kind == "horizontal_bar":
                fig.update_xaxes(tickformat=",.0f")
            else:
                fig.update_yaxes(tickformat=",.0f")

        if kind == "horizontal_bar":
            fig.update_layout(xaxis_title=metric_cols[0], yaxis_title=x_col)
        elif kind == "metric_bar":
            fig.update_layout(xaxis_title="Metric", yaxis_title="Value")
        else:
            fig.update_layout(
                xaxis_title=x_col,
                yaxis_title=metric_cols[0] if len(metric_cols) == 1 else "Value",
            )

        print(f"[CHART] Figure built successfully: kind={kind}, traces={len(fig.data)}")
        return fig

    def get_plotly_figure(self, plotly_code: str, df: pd.DataFrame, **kwargs):
        """Execute the code and return a Plotly Figure object."""
        try:
            import plotly.express as px

            # Create a local namespace for execution
            local_vars = {"df": df, "px": px, "fig": None}
            exec(plotly_code, {}, local_vars)
            return local_vars.get("fig")
        except Exception as e:
            print(f"Error generating Plotly figure: {e}")
            return None

    # --- Abstract Method Stubs (Required by VannaBase 0.x) ---
    def add_ddl(self, ddl: str, **kwargs):
        pass

    def add_documentation(self, documentation: str, **kwargs):
        pass

    def add_question_sql(self, question: str, sql: str, **kwargs):
        pass

    def get_training_data(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame()

    def remove_training_data(self, id: str, **kwargs) -> bool:
        return True

    def generate_embedding(self, data: str, **kwargs) -> list:
        return []

    def user_message(self, message: str, **kwargs) -> str:
        return message

    def assistant_message(self, message: str, **kwargs) -> str:
        return message


# Singleton instance
vn_engine = DirectSchemaVanna()
