def integrate_results(plan_results):
    anomalies = plan_results.get("anomalies", [])
    subqueries = plan_results.get("subqueries", {})
    analysis = {
        "anomalies": anomalies,
        "attribution": [],
        "patterns": [],
        "causal_chain": [],
    }

    dim_contributions = {}
    for sq_id, sq_data in subqueries.items():
        data = sq_data.get("data", [])
        if not data:
            continue
        top = data[0] if data else None
        if top:
            dim_contributions[sq_id] = {
                "dimension": sq_id,
                "top_value": top.get("value", ""),
                "count": top.get("count", 0),
                "contribution_pct": 0,
            }

    total = sum(d["count"] for d in dim_contributions.values()) or 1
    for info in dim_contributions.values():
        info["contribution_pct"] = round(info["count"] / total * 100, 1)

    analysis["attribution"] = sorted(
        dim_contributions.values(), key=lambda x: x["count"], reverse=True
    )

    for anomaly in anomalies:
        chain = []
        for sq_id, sq_data in subqueries.items():
            data = sq_data.get("data", [])
            if data:
                chain.append(
                    {"subquery": sq_id, "total": sum(d.get("count", 0) for d in data)}
                )
        analysis["causal_chain"].append(
            {"anomaly_date": anomaly["date"], "chain": chain}
        )

    if len(analysis["attribution"]) >= 2:
        t = analysis["attribution"][:2]
        analysis["patterns"].append(
            {
                "description": f"{t[0]['dimension']} ({t[0]['top_value']}) and {t[1]['dimension']} ({t[1]['top_value']}) are top contributors",
                "confidence": "medium",
            }
        )

    return analysis
