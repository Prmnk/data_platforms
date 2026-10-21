import pytest

from saillab.bootstrap import parse, remote_url
from saillab.sql_runner import is_query, split_statements


def test_split_respects_quotes_and_comments():
    sql = """
    -- leading comment; not a statement
    CREATE TABLE t USING parquet LOCATION 's3://a;b';
    SELECT 'it''s; fine', "x;y", `c;d` FROM t /* ; inside block */ ;
    ;
    SELECT 2 -- trailing; comment
    """
    assert split_statements(sql) == [
        "CREATE TABLE t USING parquet LOCATION 's3://a;b'",
        "SELECT 'it''s; fine', \"x;y\", `c;d` FROM t",
        "SELECT 2",
    ]


@pytest.mark.parametrize("stmt,query", [
    ("SELECT 1", True), ("  with x as (select 1) select * from x", True), ("(SELECT 1)", True),
    ("EXPLAIN SELECT 1", True), ("SHOW TABLES", True), ("CREATE TABLE t AS SELECT 1", False),
    ("INSERT INTO t SELECT 1", False), ("MERGE INTO t USING s ON 1=1", False),
])
def test_is_query(stmt, query):
    assert is_query(stmt) is query


def test_bootstrap_parse():
    a = parse(["--remote", "sc://sail:50051", "--session", "s", "--conf", "a=b", "--", "job.py", "--rows", "5"])
    assert a.app_args == ["job.py", "--rows", "5"] and a.conf == ["a=b"]
    a = parse(["--remote", "r", "--session", "s", "--module", "saillab.sql_runner", "--", "q.sql"])
    assert a.module == "saillab.sql_runner" and a.app_args == ["q.sql"]
    assert parse(["--remote", "r", "--session", "s", "--interrupt"]).interrupt
    with pytest.raises(SystemExit):
        parse(["--remote", "r", "--session", "s"])
    assert remote_url("sc://sail:50051/", "abc") == "sc://sail:50051/;session_id=abc"
