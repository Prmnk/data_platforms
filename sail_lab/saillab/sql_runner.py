"""Run a .sql file statement by statement on the current Spark Connect session.

    python -m saillab.bootstrap ... --module saillab.sql_runner -- file.sql [--max-rows N]

Queries (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, VALUES, TABLE, FROM) print their rows;
everything else (CREATE, INSERT, MERGE, ...) just runs. A failing statement stops the file.
"""
from __future__ import annotations

import argparse
import re
import time
from typing import List

QUERY_KEYWORDS = {"select", "with", "show", "describe", "desc", "explain", "values", "table", "from"}


def split_statements(sql: str) -> List[str]:
    """Split on semicolons outside quotes and comments; drop empty statements."""
    out, buf, i, n = [], [], 0, len(sql)
    quote = None
    while i < n:
        c = sql[i]
        if quote:
            buf.append(c)
            if c == quote:
                if i + 1 < n and sql[i + 1] == quote:  # doubled quote escapes itself
                    buf.append(sql[i + 1])
                    i += 1
                else:
                    quote = None
            elif c == "\\" and quote != "`" and i + 1 < n:
                buf.append(sql[i + 1])
                i += 1
        elif c in ("'", '"', "`"):
            quote = c
            buf.append(c)
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
            continue
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
            buf.append(" ")
            continue
        elif c == ";":
            out.append("".join(buf))
            buf = []
        else:
            buf.append(c)
        i += 1
    out.append("".join(buf))
    return [s.strip() for s in out if s.strip()]


def is_query(stmt: str) -> bool:
    m = re.match(r"\s*\(*\s*([A-Za-z]+)", stmt)
    return bool(m) and m.group(1).lower() in QUERY_KEYWORDS


def main() -> None:
    p = argparse.ArgumentParser(prog="saillab.sql_runner")
    p.add_argument("file")
    p.add_argument("--max-rows", type=int, default=50)
    a = p.parse_args()

    from pyspark.sql import SparkSession
    from saillab.sdk import report

    spark = SparkSession.builder.getOrCreate()
    with open(a.file) as f:
        statements = split_statements(f.read())
    t_all = time.monotonic()
    for n, stmt in enumerate(statements, 1):
        print(f"\n-- [{n}/{len(statements)}] {stmt}", flush=True)
        t0 = time.monotonic()
        df = spark.sql(stmt)
        if is_query(stmt):
            if stmt.lstrip("( ").lower().startswith("explain"):
                for row in df.collect():
                    print(row[0])
            else:
                df.show(a.max_rows, truncate=False)
        print(f"-- ok in {time.monotonic() - t0:.2f}s", flush=True)
    report(statements=len(statements), sql_seconds=round(time.monotonic() - t_all, 3))
    spark.stop()


if __name__ == "__main__":
    main()
