import json
import numpy as np
from scipy.stats import pearsonr
from sklearn.ensemble import RandomForestRegressor

from src.config import MODEL_NAME
from src.db import run_query
from src.llm import call_llm_routed
from src.metric_decomposer import MetricDecomposer
from src.vanna_logic import vn_engine


MIN_MOM_CHANGE_PCT = 30
MIN_ANOMALIES_FOR_ANALYSIS = 2
BASELINE_MONTHS = 3
MIN_BASELINE_VALUE = 100000

EXCLUDED_TABLES = {"order_reviews", "leads_qualified", "leads_closed", "geolocation"}
EXCLUDED_COLUMNS = {
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "payment_sequential",
    "customer_zip_code_prefix",
    "seller_zip_code_prefix",
}


class CorrelationAnalyzer:
    def __init__(self):
        self.schema_graph = vn_engine.schema_graph

    def analyze(self, question: str, sql: str, columns: list, rows: list) -> dict:
        print(f"[CORR] Starting correlation analysis for: {question[:80]}")

        if not rows or len(rows) < 6:
            print(f"[CORR] Insufficient data ({len(rows)} rows, need 6+)")
            return {
                "error": "Insufficient data for correlation analysis. Need at least 6 time periods."
            }

        date_idx, metric_idx = self._infer_columns(columns, rows)
        if date_idx is None or metric_idx is None:
            return {
                "error": "Cannot identify time and metric columns in query results."
            }

        print(
            f"[CORR] Date column index: {date_idx}, Metric column index: {metric_idx}"
        )

        anomalies = self._detect_mom_anomalies(rows, date_idx, metric_idx)
        print(f"[CORR] Detected {len(anomalies)} anomalies")

        if len(anomalies) < MIN_ANOMALIES_FOR_ANALYSIS:
            return {
                "error": f"Only {len(anomalies)} significant change points found. Need at least {MIN_ANOMALIES_FOR_ANALYSIS}."
            }

        dimensions = self._discover_dimensions()
        print(f"[CORR] Discovered {len(dimensions)} dimensions from schema graph")

        baseline_data, anomaly_data = self._run_comparative_breakdowns(
            anomalies, dimensions
        )
        print(
            f"[CORR] Comparative breakdown: {len(baseline_data)} baselines, {len(anomaly_data)} anomalies"
        )

        attribution = self._calculate_contribution_attribution(
            anomalies, baseline_data, anomaly_data
        )
        print(
            f"[CORR] Attribution: {[(a['dimension'], a['contribution_pct']) for a in attribution]}"
        )

        patterns = self._discover_patterns(
            anomalies, baseline_data, anomaly_data, attribution
        )
        print(f"[CORR] Patterns discovered: {len(patterns)}")

        causal_chain = self._trace_causal_chain(anomalies, baseline_data, anomaly_data)
        print(f"[CORR] Causal chain: {len(causal_chain)} links")

        context = self._build_llm_context(
            question,
            anomalies,
            baseline_data,
            anomaly_data,
            attribution,
            patterns,
            causal_chain,
        )
        explanation = self._synthesize_explanation(context)
        print(f"[CORR] Explanation synthesized ({len(explanation)} chars)")

        return {
            "anomalies": anomalies,
            "attribution": attribution,
            "patterns": patterns,
            "causal_chain": causal_chain,
            "explanation": explanation,
            "breakdown_data": {
                "baseline": self._summarize_comparative(baseline_data),
                "anomaly": self._summarize_comparative(anomaly_data),
            },
        }

    def _infer_columns(self, columns: list, rows: list) -> tuple:
        date_idx = None
        metric_idx = None

        for i, col in enumerate(columns):
            lowered = col.lower()
            if any(
                kw in lowered
                for kw in ["month", "date", "year", "day", "time", "period"]
            ):
                date_idx = i
                break
        if date_idx is None:
            date_idx = 0

        for i, col in enumerate(columns):
            if i == date_idx:
                continue
            lowered = col.lower()
            if any(
                kw in lowered
                for kw in [
                    "revenue",
                    "total",
                    "sum",
                    "count",
                    "value",
                    "amount",
                    "orders",
                    "sales",
                    "freight",
                ]
            ):
                metric_idx = i
                break

        if metric_idx is None:
            for i, col in enumerate(columns):
                if i == date_idx:
                    continue
                try:
                    vals = [
                        float(row[i]) if row[i] is not None else 0 for row in rows[:5]
                    ]
                    if any(v > 0 for v in vals):
                        metric_idx = i
                        break
                except (ValueError, TypeError):
                    continue

        if metric_idx is None:
            metric_idx = 1 if len(columns) > 1 else 0

        return date_idx, metric_idx

    def _detect_mom_anomalies(self, rows: list, date_idx: int, metric_idx: int) -> list:
        values = []
        dates = []
        for row in rows:
            try:
                val = float(row[metric_idx]) if row[metric_idx] is not None else 0
                values.append(val)
                dates.append(str(row[date_idx]))
            except (ValueError, TypeError, IndexError):
                values.append(0)
                dates.append(str(row[0]) if row else "unknown")

        if len(values) < 3:
            return []

        anomalies = []
        for i in range(1, len(values)):
            if values[i - 1] == 0:
                continue

            baseline_start = max(0, i - BASELINE_MONTHS)
            baseline_vals = values[baseline_start:i]
            baseline_mean = np.mean(baseline_vals) if baseline_vals else values[i - 1]

            if baseline_mean < MIN_BASELINE_VALUE:
                continue

            mom_change = ((values[i] - values[i - 1]) / abs(values[i - 1])) * 100

            if abs(mom_change) >= MIN_MOM_CHANGE_PCT:
                baseline_start = max(0, i - BASELINE_MONTHS)
                baseline_vals = values[baseline_start:i]
                baseline_mean = (
                    np.mean(baseline_vals) if baseline_vals else values[i - 1]
                )

                anomalies.append(
                    {
                        "date": dates[i],
                        "date_index": i,
                        "value": round(values[i], 2),
                        "previous_value": round(values[i - 1], 2),
                        "baseline_mean": round(float(baseline_mean), 2),
                        "mom_change_pct": round(mom_change, 1),
                        "direction": "spike" if mom_change > 0 else "dip",
                        "baseline_start": dates[baseline_start]
                        if baseline_start < len(dates)
                        else None,
                        "baseline_end": dates[i - 1] if (i - 1) < len(dates) else None,
                    }
                )

        anomalies.sort(key=lambda x: abs(x["mom_change_pct"]), reverse=True)
        return anomalies

    def _discover_dimensions(self, base_table: str = "orders") -> list[dict]:
        decomposer = MetricDecomposer()
        filters = {"order_status": "'delivered'"}
        ranked = decomposer.decompose(
            base_table,
            metric_column="COUNT(DISTINCT o.order_id)",
            filters=filters,
        )

        filtered = []
        for r in ranked:
            if r["table"] in EXCLUDED_TABLES:
                continue
            if r["column"] in EXCLUDED_COLUMNS:
                continue
            filtered.append(r)

        print(
            f"[CORR] Dimension ranking: {[(r['dimension'], r['adjusted_score']) for r in filtered]}"
        )
        return [
            {
                "name": r["dimension"],
                "table": r["table"],
                "column": r["column"],
                "variance_explained": r["variance_explained"],
            }
            for r in filtered
        ]

    def _run_comparative_breakdowns(self, anomalies: list, dimensions: list) -> tuple:
        baseline_data = {}
        anomaly_data = {}

        for anomaly in anomalies:
            anomaly_date = anomaly["date"]
            baseline_start = anomaly.get("baseline_start")
            baseline_end = anomaly.get("baseline_end")

            if not baseline_start or not baseline_end:
                continue

            anomaly_breakdown = {}
            baseline_breakdown = {}

            for dim in dimensions:
                try:
                    anomaly_sql = self._generate_period_sql(dim, anomaly_date)
                    if anomaly_sql:
                        _, anomaly_rows = run_query(anomaly_sql)
                        if anomaly_rows:
                            anomaly_breakdown[dim["name"]] = {
                                "breakdown": [
                                    {
                                        "value": str(r[0]),
                                        "count": int(r[1]) if len(r) > 1 else 0,
                                    }
                                    for r in anomaly_rows[:50]
                                ],
                                "total": sum(
                                    int(r[1]) if len(r) > 1 else 0 for r in anomaly_rows
                                ),
                            }
                except Exception as e:
                    print(
                        f"[CORR] Anomaly breakdown failed for {dim['name']} in {anomaly_date}: {e}"
                    )

                try:
                    baseline_sql = self._generate_baseline_sql(
                        dim, baseline_start, baseline_end
                    )
                    if baseline_sql:
                        _, baseline_rows = run_query(baseline_sql)
                        if baseline_rows:
                            baseline_breakdown[dim["name"]] = {
                                "breakdown": [
                                    {
                                        "value": str(r[0]),
                                        "count": int(r[1]) if len(r) > 1 else 0,
                                    }
                                    for r in baseline_rows[:50]
                                ],
                                "total": sum(
                                    int(r[1]) if len(r) > 1 else 0
                                    for r in baseline_rows
                                ),
                            }
                except Exception as e:
                    print(
                        f"[CORR] Baseline breakdown failed for {dim['name']} in {baseline_start}-{baseline_end}: {e}"
                    )

            if anomaly_breakdown:
                anomaly_data[anomaly_date] = {
                    "anomaly": anomaly,
                    "dimensions": anomaly_breakdown,
                }

            if baseline_breakdown:
                baseline_key = f"{baseline_start}_to_{baseline_end}"
                baseline_data[baseline_key] = {
                    "period_start": baseline_start,
                    "period_end": baseline_end,
                    "dimensions": baseline_breakdown,
                }

        return baseline_data, anomaly_data

    def _generate_period_sql(self, dim: dict, period: str) -> str | None:
        try:
            dim_table = dim["table"]
            dim_column = dim["column"]

            time_columns = self.schema_graph.get_time_columns_for("orders")
            if not time_columns:
                return None
            time_col = time_columns[0]["column"]

            if dim_table == "orders":
                return (
                    f"SELECT {dim_column}, COUNT(DISTINCT order_id) as order_count "
                    f"FROM orders "
                    f"WHERE strftime('%Y-%m', {time_col}) = '{period}' "
                    f"AND order_status = 'delivered' "
                    f"AND {dim_column} IS NOT NULL "
                    f"GROUP BY {dim_column} "
                    f"ORDER BY order_count DESC"
                )

            join_sql = self._build_join_sql(dim_table)
            if not join_sql:
                return None

            return (
                f"SELECT {dim_table}.{dim_column}, COUNT(DISTINCT o.order_id) as order_count "
                f"FROM orders o "
                f"{join_sql} "
                f"WHERE strftime('%Y-%m', o.{time_col}) = '{period}' "
                f"AND o.order_status = 'delivered' "
                f"AND {dim_table}.{dim_column} IS NOT NULL "
                f"GROUP BY {dim_table}.{dim_column} "
                f"ORDER BY order_count DESC"
            )
        except Exception as e:
            print(f"[CORR] Failed to generate period SQL for {dim['name']}: {e}")
            return None

    def _generate_baseline_sql(self, dim: dict, start: str, end: str) -> str | None:
        try:
            dim_table = dim["table"]
            dim_column = dim["column"]

            time_columns = self.schema_graph.get_time_columns_for("orders")
            if not time_columns:
                return None
            time_col = time_columns[0]["column"]

            if dim_table == "orders":
                return (
                    f"SELECT {dim_column}, COUNT(DISTINCT order_id) as order_count "
                    f"FROM orders "
                    f"WHERE strftime('%Y-%m', {time_col}) >= '{start}' "
                    f"AND strftime('%Y-%m', {time_col}) <= '{end}' "
                    f"AND order_status = 'delivered' "
                    f"AND {dim_column} IS NOT NULL "
                    f"GROUP BY {dim_column} "
                    f"ORDER BY order_count DESC"
                )

            join_sql = self._build_join_sql(dim_table)
            if not join_sql:
                return None

            return (
                f"SELECT {dim_table}.{dim_column}, COUNT(DISTINCT o.order_id) as order_count "
                f"FROM orders o "
                f"{join_sql} "
                f"WHERE strftime('%Y-%m', o.{time_col}) >= '{start}' "
                f"AND strftime('%Y-%m', o.{time_col}) <= '{end}' "
                f"AND o.order_status = 'delivered' "
                f"AND {dim_table}.{dim_column} IS NOT NULL "
                f"GROUP BY {dim_table}.{dim_column} "
                f"ORDER BY order_count DESC"
            )
        except Exception as e:
            print(f"[CORR] Failed to generate baseline SQL for {dim['name']}: {e}")
            return None

    def _build_join_sql(self, target_table: str) -> str | None:
        if target_table == "orders":
            return ""

        join_path = self.schema_graph.get_join_path("orders", target_table)
        if not join_path:
            return None

        joins = []
        for rel in join_path:
            if rel.source_table == "orders" or rel.target_table == "orders":
                other_table = (
                    rel.target_table
                    if rel.source_table == "orders"
                    else rel.source_table
                )
                joins.append(
                    f"JOIN {other_table} ON o.{rel.source_column} = {other_table}.{rel.target_column}"
                )
            else:
                joins.append(
                    f"JOIN {rel.target_table} ON {rel.source_table}.{rel.source_column} = {rel.target_table}.{rel.target_column}"
                )

        return " ".join(joins)

    def _trace_causal_chain(
        self,
        anomalies: list,
        baseline_data: dict,
        anomaly_data: dict,
        base_table: str = "orders",
    ) -> list[dict]:
        chains = []
        for anomaly in anomalies:
            anomaly_date = anomaly["date"]
            baseline_start = anomaly.get("baseline_start")
            baseline_end = anomaly.get("baseline_end")

            if not baseline_start or not baseline_end:
                continue

            chain = []
            self._trace_upstream(
                base_table,
                anomaly_date,
                baseline_start,
                baseline_end,
                chain,
                visited=set(),
                depth=0,
            )
            chain.sort(key=lambda x: abs(x["change_pct"]), reverse=True)
            chains.append(
                {
                    "anomaly_date": anomaly_date,
                    "chain": chain[:10],
                }
            )
        return chains

    def _trace_upstream(
        self,
        table: str,
        anomaly_date: str,
        baseline_start: str,
        baseline_end: str,
        chain: list,
        visited: set,
        depth: int,
    ):
        if depth > 3 or table in visited:
            return
        visited.add(table)

        table_profile = self.schema_graph.tables.get(table)
        if not table_profile:
            return

        time_columns = self.schema_graph.get_time_columns_for(table)
        has_time = len(time_columns) > 0

        metrics = [
            (cn, c) for cn, c in table_profile.columns.items() if c.role == "metric"
        ]
        if has_time and not metrics:
            metrics = [("_row_count", None)]

        for metric_name, metric_col in metrics:
            anomaly_val = self._get_metric_value(table, metric_name, anomaly_date)
            baseline_val = self._get_metric_value_range(
                table, metric_name, baseline_start, baseline_end
            )

            if baseline_val and baseline_val > 0 and anomaly_val is not None:
                change_pct = ((anomaly_val - baseline_val) / baseline_val) * 100
                if abs(change_pct) > 20:
                    chain.append(
                        {
                            "table": table,
                            "metric": metric_name,
                            "anomaly_value": round(anomaly_val, 2),
                            "baseline_value": round(baseline_val, 2),
                            "change_pct": round(change_pct, 1),
                            "depth": depth + 1,
                        }
                    )

        for rel in table_profile.relationships:
            upstream_table = (
                rel.target_table if rel.source_table == table else rel.source_table
            )
            if upstream_table not in visited:
                self._trace_upstream(
                    upstream_table,
                    anomaly_date,
                    baseline_start,
                    baseline_end,
                    chain,
                    visited,
                    depth + 1,
                )

    def _get_metric_value(self, table: str, metric: str, period: str) -> float | None:
        try:
            time_columns = self.schema_graph.get_time_columns_for(table)
            if not time_columns:
                return None
            time_col = time_columns[0]["column"]
            sql = (
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE strftime('%Y-%m', {time_col}) = '{period}'"
            )
            _, rows = run_query(sql)
            if rows and rows[0] and rows[0][0] is not None:
                return float(rows[0][0])
            return None
        except Exception as e:
            print(
                f"[CORR] Failed to get metric value for {table}.{metric} in {period}: {e}"
            )
            return None

    def _get_metric_value_range(
        self, table: str, metric: str, start: str, end: str
    ) -> float | None:
        try:
            time_columns = self.schema_graph.get_time_columns_for(table)
            if not time_columns:
                return None
            time_col = time_columns[0]["column"]
            sql = f"SELECT AVG(cnt) FROM (SELECT COUNT(*) as cnt FROM {table} WHERE strftime('%Y-%m', {time_col}) >= '{start}' AND strftime('%Y-%m', {time_col}) <= '{end}' GROUP BY strftime('%Y-%m', {time_col}))"
            _, rows = run_query(sql)
            if rows and rows[0] and rows[0][0] is not None:
                return float(rows[0][0])
            return None
        except Exception as e:
            print(
                f"[CORR] Failed to get metric range for {table}.{metric} in {start} to {end}: {e}"
            )
            return None

    def _calculate_contribution_attribution(
        self, anomalies: list, baseline_data: dict, anomaly_data: dict
    ) -> list:
        if len(anomaly_data) < MIN_ANOMALIES_FOR_ANALYSIS:
            return []

        all_dim_names = set()
        for period_data in anomaly_data.values():
            all_dim_names.update(period_data["dimensions"].keys())

        all_dim_names = sorted(all_dim_names)
        if not all_dim_names:
            return []

        feature_matrix = []
        target = []

        for anomaly in anomalies:
            anomaly_date = anomaly["date"]
            if anomaly_date not in anomaly_data:
                continue

            a_data = anomaly_data[anomaly_date]
            total_spike = anomaly["value"] - anomaly["baseline_mean"]

            row = []
            for dim_name in all_dim_names:
                if dim_name in a_data["dimensions"]:
                    a_dim = a_data["dimensions"][dim_name]

                    b_dim = None
                    for b_data in baseline_data.values():
                        if dim_name in b_data["dimensions"]:
                            b_dim = b_data["dimensions"][dim_name]
                            break

                    a_top = a_dim["breakdown"][0] if a_dim["breakdown"] else None
                    b_top = (
                        b_dim["breakdown"][0] if b_dim and b_dim["breakdown"] else None
                    )

                    a_count = a_top["count"] if a_top else 0
                    b_count = b_top["count"] if b_top else 0
                    absolute_change = a_count - b_count

                    contribution = (
                        absolute_change / total_spike if total_spike > 0 else 0
                    )
                    row.append(abs(contribution))
                else:
                    row.append(0)

            feature_matrix.append(row)
            target.append(abs(anomaly["mom_change_pct"]))

        if len(feature_matrix) < 2 or len(feature_matrix[0]) < 1:
            return []

        try:
            model = RandomForestRegressor(n_estimators=100, random_state=42)
            model.fit(feature_matrix, target)
        except Exception as e:
            print(f"[CORR] RandomForest failed: {e}")
            return []

        attribution = []
        for i, dim_name in enumerate(all_dim_names):
            importance = float(model.feature_importances_[i])
            dim_values = [row[i] for row in feature_matrix]

            try:
                corr, p_value = pearsonr(dim_values, target)
            except Exception:
                corr, p_value = 0.0, 1.0

            top_value = None
            avg_contribution = 0
            for a_data in anomaly_data.values():
                if dim_name in a_data["dimensions"]:
                    a_dim = a_data["dimensions"][dim_name]
                    if a_dim["breakdown"]:
                        tv = a_dim["breakdown"][0]
                        top_value = tv["value"]
                        avg_contribution = (
                            (tv["count"] / a_dim["total"] * 100)
                            if a_dim["total"] > 0
                            else 0
                        )
                        break

            attribution.append(
                {
                    "dimension": dim_name,
                    "importance": round(importance, 4),
                    "correlation": round(float(corr), 4),
                    "p_value": round(float(p_value), 4),
                    "statistically_significant": bool(p_value < 0.05),
                    "top_value": top_value,
                    "contribution_pct": round(avg_contribution, 1),
                }
            )

        attribution.sort(key=lambda x: x["importance"], reverse=True)
        return attribution

    def _discover_patterns(
        self,
        anomalies: list,
        baseline_data: dict,
        anomaly_data: dict,
        attribution: list,
    ) -> list:
        patterns = []

        for attr in attribution:
            if not attr["statistically_significant"]:
                continue

            confidence = "high" if attr["p_value"] < 0.01 else "medium"
            patterns.append(
                {
                    "factor": attr["dimension"],
                    "top_value": attr["top_value"],
                    "strength": attr["importance"],
                    "correlation": attr["correlation"],
                    "confidence": confidence,
                    "description": f"{attr['dimension']} ({attr['top_value']}) explains {attr['importance'] * 100:.0f}% of variance",
                }
            )

        if len(attribution) >= 2:
            top_two = attribution[:2]
            patterns.append(
                {
                    "factor": f"{top_two[0]['dimension']} + {top_two[1]['dimension']}",
                    "top_value": f"{top_two[0]['top_value']} + {top_two[1]['top_value']}",
                    "strength": round(
                        top_two[0]["importance"] + top_two[1]["importance"], 4
                    ),
                    "correlation": None,
                    "confidence": "medium",
                    "description": f"Combined effect of {top_two[0]['dimension']} and {top_two[1]['dimension']} explains {(top_two[0]['importance'] + top_two[1]['importance']) * 100:.0f}% of variance",
                }
            )

        for attr in attribution[:3]:
            if not attr["top_value"]:
                continue

            period_details = []
            for period_key, period_data in anomaly_data.items():
                if attr["dimension"] in period_data["dimensions"]:
                    dim_data = period_data["dimensions"][attr["dimension"]]
                    top_v = dim_data["breakdown"][0] if dim_data["breakdown"] else None
                    if top_v:
                        period_details.append(
                            {
                                "period": period_key,
                                "value": top_v["value"],
                                "count": top_v["count"],
                                "share": round(
                                    (top_v["count"] / dim_data["total"] * 100)
                                    if dim_data["total"] > 0
                                    else 0,
                                    1,
                                ),
                            }
                        )

            if len(period_details) >= 2:
                values_seen = set(d["value"] for d in period_details)
                if len(values_seen) == 1:
                    patterns.append(
                        {
                            "factor": f"consistent_{attr['dimension']}",
                            "top_value": list(values_seen)[0],
                            "strength": attr["importance"],
                            "correlation": None,
                            "confidence": "high",
                            "description": f"{attr['dimension']} = {list(values_seen)[0]} consistently dominates across {len(period_details)} anomalies",
                        }
                    )

        return patterns

    def _build_llm_context(
        self,
        question: str,
        anomalies: list,
        baseline_data: dict,
        anomaly_data: dict,
        attribution: list,
        patterns: list,
        causal_chain: list | None = None,
    ) -> dict:
        return {
            "original_question": question,
            "anomalies_detected": len(anomalies),
            "anomaly_details": anomalies,
            "attribution_ranking": attribution,
            "patterns_discovered": patterns,
            "causal_chain": causal_chain or [],
            "baseline_summary": self._summarize_comparative(baseline_data),
            "anomaly_summary": self._summarize_comparative(anomaly_data),
            "statistical_summary": {
                "total_spikes_analyzed": len(anomalies),
                "dimensions_tested": len(attribution),
                "significant_factors": [
                    a for a in attribution if a["statistically_significant"]
                ],
            },
        }

    def _summarize_comparative(self, data: dict) -> dict:
        summary = {}
        for period_key, period_data in data.items():
            summary[period_key] = {
                "dimensions": {
                    dim_name: {
                        "top_values": dim_data["breakdown"][:5],
                        "total": dim_data["total"],
                    }
                    for dim_name, dim_data in period_data["dimensions"].items()
                }
            }
        return summary

    def _synthesize_explanation(self, context: dict) -> str:
        system_prompt = (
            "You are an expert data analyst specializing in e-commerce correlation analysis.\n\n"
            "Your task is to produce a clear, business-focused explanation of what drives the observed patterns.\n\n"
            "Rules:\n"
            "1. Start with a one-sentence summary of the main finding\n"
            "2. Identify the top 2-3 drivers with specific numbers\n"
            "3. Explain what changed between baseline and anomaly periods\n"
            "4. Use plain language, not statistical jargon\n"
            "5. Mention patterns that repeat across multiple anomalies\n"
            "6. Be specific: use actual numbers, percentages, and time periods\n"
            "7. Do NOT speculate beyond the data\n"
            "8. Keep it under 200 words\n"
            "9. Structure: summary -> top drivers -> patterns -> confidence"
        )

        user_prompt = (
            f"Question: {context['original_question']}\n\n"
            f"Anomalies found: {context['anomalies_detected']}\n"
            f"Anomaly details: {json.dumps(context['anomaly_details'], indent=2)}\n\n"
            f"Top drivers (by importance): {json.dumps(context['attribution_ranking'][:5], indent=2)}\n\n"
            f"Patterns: {json.dumps(context['patterns_discovered'], indent=2)}\n\n"
            f"Causal chain: {json.dumps(context['causal_chain'], indent=2)}\n\n"
            f"Baseline data: {json.dumps(context['baseline_summary'], indent=2)}\n\n"
            f"Anomaly data: {json.dumps(context['anomaly_summary'], indent=2)}\n\n"
            "Write a clear, business-focused explanation."
        )

        try:
            raw = call_llm_routed(
                system_prompt,
                user_prompt,
                model=MODEL_NAME,
                provider_order=["openrouter"],
                allow_fallbacks=True,
                max_tokens=400,
                temperature=0.3,
            )
            return raw.strip()
        except Exception as e:
            print(f"[CORR] LLM synthesis failed: {e}")
            return self._fallback_explanation(context)

    def _fallback_explanation(self, context: dict) -> str:
        attribution = context["attribution_ranking"]
        anomalies = context["anomaly_details"]
        patterns = context["patterns_discovered"]

        if not attribution:
            return "No significant correlation patterns were found in the data."

        parts = []
        parts.append(
            f"Analysis of {len(anomalies)} significant change points reveals the following drivers:"
        )

        for i, attr in enumerate(attribution[:3], 1):
            sig = (
                "statistically significant"
                if attr["statistically_significant"]
                else "not statistically significant"
            )
            parts.append(
                f"{i}. {attr['dimension'].replace('_', ' ').title()} ({attr['top_value']}): explains {attr['importance'] * 100:.0f}% of variance ({sig}, p={attr['p_value']})"
            )

        if patterns:
            parts.append("\nKey patterns:")
            for pattern in patterns[:3]:
                parts.append(f"- {pattern['description']}")

        return "\n".join(parts)
