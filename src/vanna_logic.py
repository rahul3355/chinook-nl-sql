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
            "IMPORTANT: Olist products do NOT have human-readable names (e.g. 'iPhone'). They are identified by 'product_id'. When a user asks for 'products', ALWAYS return the 'product_category_name_english' from the translation table so they can understand what the products are."
        ]
        
        # Gold Standard Q&A for high accuracy
        self.train_data = [
            {"question": "How many orders are there?", "sql": "SELECT COUNT(*) FROM orders"},
            {"question": "What is the total revenue?", "sql": "SELECT SUM(price) FROM order_items"},
            {"question": "What is the average order value?", "sql": "SELECT SUM(price) / COUNT(DISTINCT order_id) FROM order_items"},
            {
                "question": "Top 5 categories by sales revenue in English",
                "sql": "SELECT t.product_category_name_english, SUM(oi.price) as revenue FROM order_items oi JOIN products p ON oi.product_id = p.product_id JOIN product_category_name_translation t ON p.product_category_name = t.product_category_name GROUP BY t.product_category_name_english ORDER BY revenue DESC LIMIT 5"
            },
            {
                "question": "Top 5 products by revenue",
                "sql": "SELECT t.product_category_name_english as product, SUM(oi.price) as revenue FROM order_items oi JOIN products p ON oi.product_id = p.product_id JOIN product_category_name_translation t ON p.product_category_name = t.product_category_name GROUP BY 1 ORDER BY revenue DESC LIMIT 5"
            },
            {
                "question": "Which state has the most customers?",
                "sql": "SELECT customer_state, COUNT(*) as customer_count FROM customers GROUP BY customer_state ORDER BY customer_count DESC LIMIT 1"
            },
            {
                "question": "Total revenue by customer state",
                "sql": "SELECT c.customer_state, SUM(oi.price) as revenue FROM order_items oi JOIN orders o ON oi.order_id = o.order_id JOIN customers c ON o.customer_id = c.customer_id GROUP BY c.customer_state ORDER BY revenue DESC"
            },
            {
                "question": "Which payment method is most popular?",
                "sql": "SELECT payment_type, COUNT(*) as count FROM order_payments GROUP BY payment_type ORDER BY count DESC"
            },
            {
                "question": "What is the average delivery time in days?",
                "sql": "SELECT AVG(julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp)) FROM orders WHERE order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL"
            },
            {
                "question": "What was our Month-over-Month (MoM) revenue growth in 2018?",
                "sql": "WITH monthly_rev AS (SELECT strftime('%Y-%m', o.order_purchase_timestamp) as month, SUM(oi.price) as rev FROM orders o JOIN order_items oi ON o.order_id = oi.order_id WHERE o.order_status = 'delivered' AND month LIKE '2018%' GROUP BY 1) SELECT month, rev, LAG(rev) OVER (ORDER BY month) as prev, ROUND(((rev - LAG(rev) OVER (ORDER BY month)) / LAG(rev) OVER (ORDER BY month)) * 100, 2) as growth_pct FROM monthly_rev"
            },
            {
                "question": "How many repeat customers do we have?",
                "sql": "SELECT COUNT(*) FROM (SELECT c.customer_unique_id, COUNT(DISTINCT o.order_id) as order_count FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY 1 HAVING order_count > 1)"
            },
            {
                "question": "What is the average lifetime value (LTV) per customer?",
                "sql": "SELECT AVG(total_spent) FROM (SELECT c.customer_unique_id, SUM(oi.price) as total_spent FROM customers c JOIN orders o ON c.customer_id = o.customer_id JOIN order_items oi ON o.order_id = oi.order_id GROUP BY 1)"
            }
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
            return pd.read_sql_query(sql, conn)
        except Exception as e:
            # Handle possible sql execution errors
            print(f"SQL Error: {e}")
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
        sql = sql.replace('```sql', '').replace('```', '').strip()
        return sql

    def prepare_dataframe_for_charting(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize SQLite result columns so aggregates stay numeric for plotting."""
        plot_df = df.copy()

        for col in plot_df.columns:
            series = plot_df[col]
            if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(series):
                continue

            cleaned = series.astype(str).str.strip()
            cleaned = cleaned.str.replace(",", "", regex=False).str.replace("$", "", regex=False)
            numeric_series = pd.to_numeric(cleaned, errors="coerce")

            if series.notna().sum() > 0 and numeric_series.notna().sum() == series.notna().sum():
                plot_df[col] = numeric_series

        return plot_df

    def _select_chart_columns(self, df: pd.DataFrame):
        metric_keywords = (
            "count", "total", "sum", "revenue", "sales", "price", "amount",
            "value", "qty", "quantity", "orders", "customers", "profit",
            "avg", "average", "score", "rate", "pct", "percent", "growth"
        )
        helper_keywords = ("rank", "index", "row", "row_num", "row_number", "sort")
        time_keywords = ("date", "time", "month", "year", "week", "day", "quarter")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        categorical_cols = [c for c in df.columns if c not in numeric_cols]

        if not numeric_cols or not categorical_cols:
            return None, None

        def metric_score(col: str) -> int:
            name = col.lower()
            score = 0
            if any(keyword in name for keyword in metric_keywords):
                score += 100
            if any(keyword in name for keyword in helper_keywords):
                score -= 100
            return score

        preferred_numeric_cols = [col for col in numeric_cols if metric_score(col) > 0]
        if len(preferred_numeric_cols) == 1:
            y_col = preferred_numeric_cols[0]
        elif len(numeric_cols) == 1:
            y_col = numeric_cols[0]
        else:
            non_helper_numeric_cols = [col for col in numeric_cols if metric_score(col) >= 0]
            if len(non_helper_numeric_cols) != 1:
                return None, None
            y_col = non_helper_numeric_cols[0]

        def category_score(col: str) -> int:
            name = col.lower()
            return 100 if any(keyword in name for keyword in time_keywords) else 0

        x_col = max(categorical_cols, key=category_score)
        return x_col, y_col

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

    def generate_plotly_code(self, question: str, sql: str, df: pd.DataFrame, **kwargs) -> str:
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
        code = code.replace('```python', '').replace('```', '').strip()
        return code

    def get_deterministic_figure(self, df: pd.DataFrame, title: str | None = None):
        """Rule-based plotter for single-metric result sets to avoid LLM hallucinations."""
        import plotly.graph_objects as go
        plot_df = self.prepare_dataframe_for_charting(df)
        if len(plot_df.columns) < 2:
            return None

        x_col, y_col = self._select_chart_columns(plot_df)
        if not x_col or not y_col:
            return None

        y_values = pd.to_numeric(plot_df[y_col], errors="coerce")
        if y_values.isna().any():
            return None

        x_values = plot_df[x_col].tolist()
        fig = go.Figure(
            data=[
                go.Bar(
                    x=x_values,
                    y=y_values.tolist(),
                    marker_color="#ef4444",
                    name=y_col,
                )
            ]
        )

        fig.update_layout(
            title=title,
            paper_bgcolor='white',
            plot_bgcolor='white',
            autosize=True,
            height=540,
            margin=dict(l=80, r=30, t=60, b=90),
            font=dict(family="Arial, sans-serif", size=12),
            showlegend=False,
            xaxis_title=x_col,
            yaxis_title=y_col,
            bargap=0.2,
        )
        fig.update_xaxes(tickangle=45)
        fig.update_yaxes(rangemode='tozero')
        
        if 'price' in y_col.lower() or 'revenue' in y_col.lower() or 'value' in y_col.lower():
            fig.update_layout(yaxis_tickformat='$,.2f')
        else:
            fig.update_layout(yaxis_tickformat=',d')
            
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
    def add_ddl(self, ddl: str, **kwargs): pass
    def add_documentation(self, documentation: str, **kwargs): pass
    def add_question_sql(self, question: str, sql: str, **kwargs): pass
    def get_training_data(self, **kwargs) -> pd.DataFrame: return pd.DataFrame()
    def remove_training_data(self, id: str, **kwargs) -> bool: return True
    def generate_embedding(self, data: str, **kwargs) -> list: return []
    def user_message(self, message: str, **kwargs) -> str: return message
    def assistant_message(self, message: str, **kwargs) -> str: return message

# Singleton instance
vn_engine = DirectSchemaVanna()
