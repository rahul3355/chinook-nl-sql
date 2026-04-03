import numpy as np

from src.db import run_query
from src.vanna_logic import vn_engine


MAX_DIMENSIONS = 10


class MetricDecomposer:
    def __init__(self):
        self.schema_graph = vn_engine.schema_graph

    def decompose(
        self,
        base_table: str,
        metric_column: str | None = None,
        filters: dict | None = None,
    ) -> list[dict]:
        reachable_tables = set(self.schema_graph.get_all_reachable_tables(base_table))
        reachable_tables.add(base_table)

        dimensions = []
        for table_name in sorted(reachable_tables):
            if table_name not in self.schema_graph.tables:
                continue
            table = self.schema_graph.tables[table_name]
            for col_name, col in table.columns.items():
                if col.role == "dimension":
                    dimensions.append(
                        {
                            "name": col_name,
                            "table": table_name,
                            "column": col_name,
                        }
                    )

        if not dimensions:
            return []

        ranked = []
        for dim in dimensions:
            try:
                sql = self._generate_breakdown_sql(
                    base_table, dim, metric_column, filters
                )
                if not sql:
                    continue

                _, rows = run_query(sql)
                if not rows or len(rows) < 2:
                    continue

                values = []
                for r in rows:
                    if len(r) > 1 and r[1] is not None:
                        try:
                            values.append(float(r[1]))
                        except (ValueError, TypeError):
                            values.append(0)
                    else:
                        values.append(0)

                total = sum(values)
                if total == 0:
                    continue

                variance_explained = self._calculate_variance_explained(values, total)
                herfindahl = sum((v / total) ** 2 for v in values) if total > 0 else 1
                concentration = 1 - herfindahl

                distinct_count = len(rows)
                cardinality_penalty = max(0, 1 - (distinct_count / 50))
                adjusted_score = (
                    variance_explained * concentration * cardinality_penalty
                )

                ranked.append(
                    {
                        "dimension": dim["name"],
                        "table": dim["table"],
                        "column": dim["column"],
                        "variance_explained": round(variance_explained, 4),
                        "concentration": round(concentration, 4),
                        "distinct_values": distinct_count,
                        "total": total,
                        "adjusted_score": round(adjusted_score, 4),
                        "breakdown": [
                            {
                                "value": str(r[0]),
                                "count": int(r[1]) if len(r) > 1 else 0,
                            }
                            for r in rows[:20]
                        ],
                    }
                )
            except Exception as e:
                print(f"[DECOMPOSER] Failed for {dim['table']}.{dim['column']}: {e}")
                continue

        ranked.sort(key=lambda x: x["adjusted_score"], reverse=True)
        return ranked[:MAX_DIMENSIONS]

    def _generate_breakdown_sql(
        self,
        base_table: str,
        dim: dict,
        metric_column: str | None = None,
        filters: dict | None = None,
    ) -> str | None:
        try:
            dim_table = dim["table"]
            dim_column = dim["column"]

            if dim_table == base_table:
                metric = metric_column or "COUNT(*)"
                alias = "o" if base_table == "orders" else base_table[0]
                where = self._build_where(filters, alias)
                return (
                    f"SELECT {dim_column}, {metric} as metric_value "
                    f"FROM {base_table} {alias} "
                    f"{where}"
                    f"GROUP BY {dim_column} "
                    f"ORDER BY metric_value DESC"
                )

            join_sql = self._build_join_sql(base_table, dim_table)
            if not join_sql:
                return None

            if base_table == "orders":
                metric = "COUNT(DISTINCT o.order_id)"
                alias = "o"
            else:
                metric = metric_column or "COUNT(*)"
                alias = base_table[0]

            where = self._build_where(filters, alias)

            return (
                f"SELECT {dim_table}.{dim_column}, {metric} as metric_value "
                f"FROM {base_table} {alias} "
                f"{join_sql} "
                f"{where}"
                f"GROUP BY {dim_table}.{dim_column} "
                f"ORDER BY metric_value DESC"
            )
        except Exception as e:
            print(
                f"[DECOMPOSER] Failed to generate breakdown SQL for {dim['table']}.{dim['column']}: {e}"
            )
            return None

    def _build_join_sql(self, base_table: str, target_table: str) -> str | None:
        if base_table == target_table:
            return ""

        join_path = self.schema_graph.get_join_path(base_table, target_table)
        if not join_path:
            return None

        joins = []
        alias = "o" if base_table == "orders" else base_table[0]

        for rel in join_path:
            if rel.source_table == base_table:
                joins.append(
                    f"JOIN {rel.target_table} ON {alias}.{rel.source_column} = {rel.target_table}.{rel.target_column}"
                )
            elif rel.target_table == base_table:
                joins.append(
                    f"JOIN {rel.source_table} ON {alias}.{rel.target_column} = {rel.source_table}.{rel.source_column}"
                )
            else:
                joins.append(
                    f"JOIN {rel.target_table} ON {rel.source_table}.{rel.source_column} = {rel.target_table}.{rel.target_column}"
                )

        return " ".join(joins)

    def _build_where(self, filters: dict | None, alias: str) -> str:
        if not filters:
            return ""
        clauses = []
        for k, v in filters.items():
            if "." not in k:
                col_ref = f"{alias}.{k}"
            else:
                col_ref = k
            if v.startswith("'") and v.endswith("'"):
                clauses.append(f"{col_ref} = {v}")
            else:
                clauses.append(f"{col_ref} = {v!r}")
        return f"WHERE {' AND '.join(clauses)} " if clauses else ""

    def _calculate_variance_explained(self, values: list, total: float) -> float:
        if len(values) < 2:
            return 0.0

        mean_val = np.mean(values)
        total_variance = np.sum((np.array(values) - mean_val) ** 2)

        if total_variance == 0:
            return 0.0

        weights = np.array(values) / total
        within_group_variance = np.sum(weights * (np.array(values) - mean_val) ** 2)

        variance_explained = 1 - (within_group_variance / total_variance)
        return max(0.0, min(1.0, variance_explained))
