import time

from src.db import run_query
from src.vanna_logic import vn_engine


def execute_plan(plan, anomalies, callback=None):
    results = {"base_query": None, "anomalies": anomalies, "subqueries": {}}

    for sq_id in plan.execution_order:
        sq = next((s for s in plan.subqueries if s.id == sq_id), None)
        if not sq:
            continue

        sq.status = "executing"
        if callback:
            callback(
                {
                    "type": "executing",
                    "subquery": sq_id,
                    "description": sq.description,
                    "tables": sq.tables,
                }
            )

        t0 = time.time()
        try:
            sq.sql = _build_sql(sq)
            sq.result = _execute_subquery(sq.sql)
            sq.status = "done"
            sq.execution_time = round(time.time() - t0, 2)
            results["subqueries"][sq_id] = sq.result
            if callback:
                callback(
                    {
                        "type": "done",
                        "subquery": sq_id,
                        "rows": len(sq.result.get("data", [])),
                        "time": sq.execution_time,
                    }
                )
        except Exception as e:
            sq.status = "error"
            sq.execution_time = round(time.time() - t0, 2)
            if callback:
                callback({"type": "error", "subquery": sq_id, "error": str(e)})

    return results


def _build_sql(sq):
    if not sq.dimensions:
        return ""

    dim = sq.dimensions[0]
    table = sq.tables[0] if sq.tables else "orders"
    metric = sq.metrics[0] if sq.metrics else "COUNT(DISTINCT o.order_id)"

    joins = _get_joins(table)
    where = "o.order_status = 'delivered'"

    if table == "orders":
        return (
            f"SELECT {dim}, {metric} as value FROM orders "
            f"WHERE {where} AND {dim} IS NOT NULL "
            f"GROUP BY {dim} ORDER BY value DESC"
        )

    return (
        f"SELECT {table}.{dim}, {metric} as value FROM orders o "
        f"{joins} WHERE {where} AND {table}.{dim} IS NOT NULL "
        f"GROUP BY {table}.{dim} ORDER BY value DESC"
    )


def _get_joins(target_table):
    if target_table == "orders":
        return ""

    path = vn_engine.schema_graph.get_join_path("orders", target_table)
    if not path:
        return ""

    parts = []
    for rel in path:
        if rel.source_table == "orders" or rel.target_table == "orders":
            other = (
                rel.target_table if rel.source_table == "orders" else rel.source_table
            )
            parts.append(
                f"JOIN {other} ON o.{rel.source_column} = {other}.{rel.target_column}"
            )
        else:
            parts.append(
                f"JOIN {rel.target_table} ON {rel.source_table}.{rel.source_column} = {rel.target_table}.{rel.target_column}"
            )

    return " ".join(parts)


def _execute_subquery(sql):
    if not sql:
        return {"data": []}

    _, rows = run_query(sql)
    return {
        "data": [
            {"value": str(r[0]), "count": r[1] if len(r) > 1 else 0}
            for r in (rows or [])
        ]
    }
