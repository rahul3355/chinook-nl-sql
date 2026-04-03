import sqlite3
from dataclasses import dataclass, field


@dataclass
class ColumnProfile:
    name: str
    table: str
    dtype: str
    role: str  # identifier, dimension, metric, time, text
    is_primary_key: bool = False
    distinct_count: int = 0
    null_rate: float = 0.0
    sample_values: list = field(default_factory=list)
    min_val: float | None = None
    max_val: float | None = None
    mean_val: float | None = None
    date_range: tuple | None = None
    row_count: int = 0


@dataclass
class Relationship:
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    cardinality: str  # "1:N", "N:1"
    confidence: float = 1.0


@dataclass
class TableProfile:
    name: str
    row_count: int = 0
    columns: dict[str, ColumnProfile] = field(default_factory=dict)
    primary_keys: list[str] = field(default_factory=list)
    relationships: list[Relationship] = field(default_factory=list)


class SchemaGraph:
    def __init__(self):
        self.tables: dict[str, TableProfile] = {}
        self.relationships: list[Relationship] = []

    def get_dimensions_for(
        self, table: str, include_reachable: bool = True
    ) -> list[dict]:
        dims = []
        if table not in self.tables:
            return dims

        for col_name, col in self.tables[table].columns.items():
            if col.role in ("dimension",):
                dims.append({"table": table, "column": col_name, "profile": col})

        if include_reachable:
            for rel in self.relationships:
                if rel.source_table == table or rel.target_table == table:
                    other_table = (
                        rel.target_table
                        if rel.source_table == table
                        else rel.source_table
                    )
                    if other_table in self.tables:
                        for col_name, col in self.tables[other_table].columns.items():
                            if col.role == "dimension":
                                dims.append(
                                    {
                                        "table": other_table,
                                        "column": col_name,
                                        "profile": col,
                                    }
                                )

        seen = set()
        unique_dims = []
        for d in dims:
            key = f"{d['table']}.{d['column']}"
            if key not in seen:
                seen.add(key)
                unique_dims.append(d)

        return unique_dims

    def get_metrics_for(self, table: str) -> list[dict]:
        metrics = []
        if table not in self.tables:
            return metrics
        for col_name, col in self.tables[table].columns.items():
            if col.role == "metric":
                metrics.append({"table": table, "column": col_name, "profile": col})
        return metrics

    def get_time_columns_for(self, table: str) -> list[dict]:
        time_cols = []
        if table not in self.tables:
            return time_cols
        for col_name, col in self.tables[table].columns.items():
            if col.role == "time":
                time_cols.append({"table": table, "column": col_name, "profile": col})
        return time_cols

    def get_join_path(self, source: str, target: str) -> list[Relationship]:
        if source == target:
            return []

        visited = set()
        queue = [(source, [])]

        while queue:
            current, path = queue.pop(0)
            if current == target:
                return path

            if current in visited:
                continue
            visited.add(current)

            for rel in self.relationships:
                if rel.source_table == current and rel.target_table not in visited:
                    queue.append((rel.target_table, path + [rel]))
                elif rel.target_table == current and rel.source_table not in visited:
                    queue.append((rel.source_table, path + [rel]))

        return []

    def get_all_reachable_tables(self, source: str) -> list[str]:
        reachable = set()
        queue = [source]

        while queue:
            current = queue.pop(0)
            for rel in self.relationships:
                if rel.source_table == current and rel.target_table not in reachable:
                    reachable.add(rel.target_table)
                    queue.append(rel.target_table)
                elif rel.target_table == current and rel.source_table not in reachable:
                    reachable.add(rel.source_table)
                    queue.append(rel.source_table)

        return sorted(reachable)

    def get_ddl(self) -> str:
        parts = []
        for table_name, table in self.tables.items():
            col_defs = []
            for col_name, col in table.columns.items():
                pk_suffix = " PRIMARY KEY" if col.is_primary_key else ""
                col_defs.append(f"    {col_name} {col.dtype}{pk_suffix}")
            cols = ",\n".join(col_defs)
            parts.append(f"CREATE TABLE {table_name} (\n{cols}\n);")
        return "\n\n".join(parts)

    def get_documentation(self) -> list[str]:
        docs = []
        for table_name, table in self.tables.items():
            dims = [c for c in table.columns.values() if c.role == "dimension"]
            metrics = [c for c in table.columns.values() if c.role == "metric"]
            time_cols = [c for c in table.columns.values() if c.role == "time"]

            if dims:
                docs.append(
                    f"Table {table_name} has dimensions: {', '.join(d.name for d in dims)}."
                )
            if metrics:
                docs.append(
                    f"Table {table_name} has metrics: {', '.join(m.name for m in metrics)}."
                )
            if time_cols:
                docs.append(
                    f"Table {table_name} has time columns: {', '.join(t.name for t in time_cols)}."
                )

            for rel in table.relationships:
                other = (
                    rel.target_table
                    if rel.source_table == table_name
                    else rel.source_table
                )
                docs.append(
                    f"Table {table_name} is related to {other} via {rel.source_column} = {rel.target_column}."
                )

        return docs

    def to_dict(self) -> dict:
        return {
            "tables": {
                name: {
                    "row_count": t.row_count,
                    "columns": {
                        cn: {
                            "dtype": c.dtype,
                            "role": c.role,
                            "is_primary_key": c.is_primary_key,
                            "distinct_count": c.distinct_count,
                            "null_rate": round(c.null_rate, 3),
                            "sample_values": c.sample_values[:5],
                            "min_val": c.min_val,
                            "max_val": c.max_val,
                            "mean_val": round(c.mean_val, 2)
                            if c.mean_val is not None
                            else None,
                            "date_range": c.date_range,
                        }
                        for cn, c in t.columns.items()
                    },
                }
                for name, t in self.tables.items()
            },
            "relationships": [
                {
                    "source_table": r.source_table,
                    "source_column": r.source_column,
                    "target_table": r.target_table,
                    "target_column": r.target_column,
                    "cardinality": r.cardinality,
                    "confidence": r.confidence,
                }
                for r in self.relationships
            ],
        }


class SchemaDiscovery:
    IDENTIFIER_KEYWORDS = ["id", "key", "_id"]
    TIME_KEYWORDS = ["date", "time", "timestamp", "year", "month", "day"]
    DIMENSION_CARDINALITY_THRESHOLD = 50
    DIMENSION_CARDINALITY_RATIO = 0.01

    def __init__(self, db_path: str):
        self.db_path = db_path

    def discover(self) -> SchemaGraph:
        graph = SchemaGraph()
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            self._discover_tables(cursor, graph)
            for table_name in graph.tables:
                self._profile_table(cursor, graph, table_name)
            self._discover_relationships(conn, graph)
        finally:
            conn.close()

        return graph

    def _discover_tables(self, cursor, graph: SchemaGraph):
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        table_names = [r[0] for r in cursor.fetchall()]

        for name in table_names:
            cursor.execute(f'SELECT COUNT(*) FROM "{name}"')
            row_count = cursor.fetchone()[0]
            graph.tables[name] = TableProfile(name=name, row_count=row_count)

    def _profile_table(self, cursor, graph: SchemaGraph, table_name: str):
        table = graph.tables[table_name]

        cursor.execute(f'PRAGMA table_info("{table_name}")')
        col_infos = cursor.fetchall()

        pk_columns = [ci[1] for ci in col_infos if ci[5] > 0]
        table.primary_keys = pk_columns

        for ci in col_infos:
            col_name = ci[1]
            dtype = ci[2]
            is_pk = ci[5] > 0

            profile = self._profile_column(
                cursor, table_name, col_name, dtype, is_pk, table.row_count
            )
            table.columns[col_name] = profile

        if not pk_columns:
            candidates = []
            for col_name, col in table.columns.items():
                lowered = col_name.lower()
                is_fk_hint = (
                    lowered.endswith("_id")
                    and col_name != table.name.rstrip("s") + "_id"
                )
                if (
                    col.role == "identifier"
                    and col.distinct_count == table.row_count
                    and table.row_count > 0
                    and not is_fk_hint
                ):
                    candidates.append((col_name, col))

            if len(candidates) == 1:
                col_name, col = candidates[0]
                col.is_primary_key = True
                table.primary_keys.append(col_name)
            elif len(candidates) > 1:
                for col_name, col in candidates:
                    lowered = col_name.lower()
                    if (
                        "translation" in lowered
                        or "english" in lowered
                        or "name" in lowered
                    ):
                        continue
                    col.is_primary_key = True
                    table.primary_keys.append(col_name)
                    break
                if not table.primary_keys:
                    col_name, col = candidates[0]
                    col.is_primary_key = True
                    table.primary_keys.append(col_name)
            elif len(candidates) > 1:
                for col_name, col in candidates:
                    lowered = col_name.lower()
                    if (
                        "translation" in lowered
                        or "english" in lowered
                        or "name" in lowered
                    ):
                        continue
                    col.is_primary_key = True
                    table.primary_keys.append(col_name)
                    break
                if not table.primary_keys:
                    col_name, col = candidates[0]
                    col.is_primary_key = True
                    table.primary_keys.append(col_name)

    def _profile_column(
        self,
        cursor,
        table_name: str,
        col_name: str,
        dtype: str,
        is_pk: bool,
        row_count: int,
    ) -> ColumnProfile:
        cursor.execute(f'SELECT COUNT(DISTINCT "{col_name}") FROM "{table_name}"')
        distinct_count = cursor.fetchone()[0]

        cursor.execute(
            f'SELECT COUNT(*) FROM "{table_name}" WHERE "{col_name}" IS NULL'
        )
        null_count = cursor.fetchone()[0]
        null_rate = null_count / row_count if row_count > 0 else 0

        cursor.execute(
            f'SELECT "{col_name}", COUNT(*) as cnt FROM "{table_name}" '
            f'WHERE "{col_name}" IS NOT NULL GROUP BY 1 ORDER BY cnt DESC LIMIT 10'
        )
        samples = cursor.fetchall()
        sample_values = [str(r[0]) for r in samples]

        min_val = max_val = mean_val = None
        date_range = None

        if dtype in ("INTEGER", "REAL"):
            cursor.execute(
                f'SELECT MIN("{col_name}"), MAX("{col_name}"), AVG("{col_name}") '
                f'FROM "{table_name}" WHERE "{col_name}" IS NOT NULL'
            )
            min_val, max_val, mean_val = cursor.fetchone()

        lowered = col_name.lower()
        if any(kw in lowered for kw in self.TIME_KEYWORDS):
            cursor.execute(
                f'SELECT MIN("{col_name}"), MAX("{col_name}") '
                f'FROM "{table_name}" WHERE "{col_name}" IS NOT NULL'
            )
            date_range = cursor.fetchone()

        role = self._classify_column(
            col_name, dtype, distinct_count, row_count, sample_values, is_pk
        )

        return ColumnProfile(
            name=col_name,
            table=table_name,
            dtype=dtype,
            role=role,
            is_primary_key=is_pk,
            distinct_count=distinct_count,
            null_rate=null_rate,
            sample_values=sample_values,
            min_val=min_val,
            max_val=max_val,
            mean_val=mean_val,
            date_range=date_range,
            row_count=row_count,
        )

    def _classify_column(
        self,
        col_name: str,
        dtype: str,
        distinct_count: int,
        row_count: int,
        sample_values: list,
        is_pk: bool,
    ) -> str:
        lowered = col_name.lower()

        if is_pk:
            if any(kw in lowered for kw in self.TIME_KEYWORDS):
                return "time"
            return "identifier"

        if any(kw in lowered for kw in self.IDENTIFIER_KEYWORDS):
            return "identifier"

        if any(kw in lowered for kw in self.TIME_KEYWORDS):
            if sample_values and self._looks_like_date(sample_values[0]):
                return "time"

        if dtype in ("INTEGER", "REAL"):
            cardinality_ratio = distinct_count / row_count if row_count > 0 else 0
            if distinct_count <= 20:
                return "dimension"
            if cardinality_ratio > 0.1:
                return "metric"
            return "dimension"

        if dtype == "TEXT":
            cardinality_threshold = min(
                self.DIMENSION_CARDINALITY_THRESHOLD,
                row_count * self.DIMENSION_CARDINALITY_RATIO,
            )
            if distinct_count <= cardinality_threshold:
                return "dimension"
            if distinct_count > row_count * 0.5:
                return "identifier"
            return "text"

        return "metric"

    def _looks_like_date(self, value: str) -> bool:
        if not value:
            return False
        import re

        return bool(re.match(r"\d{4}-\d{2}-\d{2}", str(value)))

    def _discover_relationships(self, conn, graph: SchemaGraph):
        id_columns = {}

        for table_name, table in graph.tables.items():
            for col_name, col in table.columns.items():
                if col.role in ("identifier", "dimension", "text"):
                    if col_name not in id_columns:
                        id_columns[col_name] = []
                    id_columns[col_name].append(
                        (table_name, col.is_primary_key, col.distinct_count)
                    )

        for col_name, occurrences in id_columns.items():
            pk_tables = [(t, dc) for t, is_pk, dc in occurrences if is_pk]
            fk_tables = [(t, dc) for t, is_pk, dc in occurrences if not is_pk]

            for pk_table, pk_distinct in pk_tables:
                for fk_table, fk_distinct in fk_tables:
                    if pk_table == fk_table:
                        continue

                    orphan_rate = self._check_orphan_rate(
                        conn, fk_table, col_name, pk_table, col_name
                    )
                    confidence = 1.0 - orphan_rate

                    if confidence < 0.5:
                        continue

                    rel = Relationship(
                        source_table=fk_table,
                        source_column=col_name,
                        target_table=pk_table,
                        target_column=col_name,
                        cardinality="N:1",
                        confidence=round(confidence, 3),
                    )
                    graph.relationships.append(rel)
                    graph.tables[fk_table].relationships.append(rel)

                    reverse_rel = Relationship(
                        source_table=pk_table,
                        source_column=col_name,
                        target_table=fk_table,
                        target_column=col_name,
                        cardinality="1:N",
                        confidence=round(confidence, 3),
                    )
                    graph.tables[pk_table].relationships.append(reverse_rel)

    def _check_orphan_rate(
        self, conn, fk_table: str, fk_col: str, pk_table: str, pk_col: str
    ) -> float:
        try:
            cursor = conn.cursor()
            cursor.execute(
                f'SELECT COUNT(DISTINCT "{fk_col}") FROM "{fk_table}" WHERE "{fk_col}" IS NOT NULL'
            )
            fk_total = cursor.fetchone()[0]
            if fk_total == 0:
                return 0.0

            cursor.execute(
                f'SELECT COUNT(DISTINCT "{fk_table}"."{fk_col}") FROM "{fk_table}" '
                f'LEFT JOIN "{pk_table}" ON "{fk_table}"."{fk_col}" = "{pk_table}"."{pk_col}" '
                f'WHERE "{pk_table}"."{pk_col}" IS NULL AND "{fk_table}"."{fk_col}" IS NOT NULL'
            )
            orphan_count = cursor.fetchone()[0]
            return orphan_count / fk_total if fk_total > 0 else 0.0
        except Exception:
            return 0.5
