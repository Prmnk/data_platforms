"""Generates the provisioned dashboards. Edit here, then: python3 grafana/build_dashboards.py

    overview.json   Sail Lab - Overview: runs, engine operators, spill, containers, S3, logs
    run.json        Sail Lab - Run: one run (= one Spark Connect session) end to end

Sail's execution_* metrics are gauges that live for the duration of a query, one series per
(session_id, execution_job_id, operator). "max_over_time(...[$__range])" turns each series
into its final value, and summing those gives totals for the time range.
"""
import json
from pathlib import Path

PROM = {"type": "prometheus", "uid": "prometheus"}
LOKI = {"type": "loki", "uid": "loki"}
TEMPO = {"type": "tempo", "uid": "tempo"}
API = {"type": "yesoreyeram-infinity-datasource", "uid": "lab-api"}
API_URL = "http://lab-api:8010"

STATUS_COLORS = {"succeeded": "green", "running": "blue", "queued": "purple", "failed": "red",
                 "timed_out": "orange", "cancelled": "yellow", "lost": "dark-red"}
STATUS_MAPPING = [{"type": "value", "options": {k: {"color": v, "index": i} for i, (k, v) in enumerate(STATUS_COLORS.items())}}]


class Layout:
    """Places panels left to right on a 24-column grid, wrapping to new rows."""

    def __init__(self):
        self.x = self.y = self.row_h = 0
        self.next_id = 1

    def place(self, panel, w, h):
        if self.x + w > 24:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        panel["gridPos"] = {"x": self.x, "y": self.y, "w": w, "h": h}
        panel["id"] = self.next_id
        self.next_id += 1
        self.x += w
        self.row_h = max(self.row_h, h)
        return panel

    def row(self, title):
        if self.x:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        p = {"type": "row", "title": title, "collapsed": False, "panels": [],
             "gridPos": {"x": 0, "y": self.y, "w": 24, "h": 1}, "id": self.next_id}
        self.next_id += 1
        self.y += 1
        return p


def prom(expr, legend="", instant=False, fmt="time_series", ref="A"):
    t = {"datasource": PROM, "refId": ref, "expr": expr, "legendFormat": legend or "__auto", "format": fmt}
    if instant:
        t.update(instant=True, range=False)
    return t


def infinity(url, columns, root="", ref="A"):
    return {"datasource": API, "refId": ref, "type": "json", "source": "url", "format": "table",
            "url": url, "parser": "backend", "root_selector": root, "url_options": {"method": "GET"},
            "columns": [{"selector": s, "text": t, "type": typ} for s, t, typ in columns]}


def panel(kind, title, targets, ds=PROM, unit=None, desc="", **extra):
    p = {"type": kind, "title": title, "datasource": ds, "targets": targets, "description": desc,
         "fieldConfig": {"defaults": {}, "overrides": []}, "options": {}}
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    for k, v in extra.items():
        p[k] = v
    return p


def stat(title, expr, unit=None, color="blue", desc="", thresholds=None, **extra):
    p = panel("stat", title, [prom(expr, instant=True)], unit=unit, desc=desc, **extra)
    p["fieldConfig"]["defaults"]["color"] = {"mode": "thresholds"}
    p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds or [{"color": color, "value": None}]}
    p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "none", "textMode": "value", "justifyMode": "center"}
    return p


def timeseries(title, targets, unit=None, desc="", stack=False, bars=False, **extra):
    p = panel("timeseries", title, targets, unit=unit, desc=desc, **extra)
    p["fieldConfig"]["defaults"]["custom"] = {
        "drawStyle": "bars" if bars else "line", "lineWidth": 2, "fillOpacity": 60 if bars else 12,
        "showPoints": "never", "spanNulls": True,
        "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    p["options"] = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "desc"}}
    return p


def bars(title, expr, unit=None, desc="", legend="{{execution_operator_name}}"):
    p = panel("bargauge", title, [prom(expr, legend=legend, instant=True)], unit=unit, desc=desc)
    p["options"] = {"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                    "valueMode": "color", "namePlacement": "left", "sizing": "auto",
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}
    p["fieldConfig"]["defaults"]["color"] = {"mode": "continuous-BlPu"}
    p["fieldConfig"]["defaults"]["min"] = 0
    return p


def logs(title, expr, desc=""):
    p = panel("logs", title, [{"datasource": LOKI, "refId": "A", "expr": expr, "queryType": "range"}], ds=LOKI, desc=desc)
    p["options"] = {"showTime": True, "wrapLogMessage": True, "sortOrder": "Ascending",
                    "enableLogDetails": True, "dedupStrategy": "none", "prettifyLogMessage": False}
    return p


def status_override(field="status"):
    return {"matcher": {"id": "byName", "options": field},
            "properties": [{"id": "mappings", "value": STATUS_MAPPING},
                           {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}


def dashboard(uid, title, panels, templating=(), desc="", links=(), time_from="now-1h"):
    return {"uid": uid, "title": title, "description": desc, "tags": ["sail", "lakesail", "spark-connect"],
            "timezone": "browser", "schemaVersion": 39, "version": 1, "editable": True,
            "refresh": "10s", "time": {"from": time_from, "to": "now"}, "graphTooltip": 1,
            "templating": {"list": list(templating)}, "annotations": {"list": []},
            "links": list(links), "panels": panels}


# ----------------------------------------------------------------------------- overview
def overview():
    L, P = Layout(), []
    P.append(L.row("Runs"))
    P += [
        L.place(stat("Running", 'sum(lab_runs_active{state="running"})', color="blue",
                     desc="Runs whose PySpark client is executing now"), 4, 4),
        L.place(stat("Queued", 'sum(lab_runs_active{state="queued"})', color="purple",
                     desc="Runs waiting for one of MAX_CONCURRENT_RUNS slots"), 4, 4),
        L.place(stat("Succeeded (range)", 'sum(increase(lab_runs_finished_total{status="succeeded"}[$__range])) or vector(0)',
                     color="green", decimals=0), 4, 4),
        L.place(stat("Failed / timed out (range)",
                     'sum(increase(lab_runs_finished_total{status=~"failed|timed_out|lost"}[$__range])) or vector(0)',
                     decimals=0, thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}]), 4, 4),
        L.place(stat("Rows produced by Sail (range)",
                     "sum(max_over_time(execution_output_row_count[$__range])) or vector(0)", unit="short",
                     desc="Sum of every operator's output rows over all queries in the range"), 4, 4),
        L.place(stat("Spilled to disk (range)", "sum(max_over_time(execution_spill_size_byte[$__range])) or vector(0)",
                     unit="bytes", thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}],
                     desc="Bytes Sail spilled because its memory pool (SAIL_RUNTIME__MEMORY_POOL__*) was full"), 4, 4),
    ]

    runs = panel("table", "Recent runs (click a run for its dashboard)", [infinity(f"{API_URL}/runs?limit=30", [
        ("short_id", "run", "string"), ("id", "id", "string"), ("job", "job", "string"),
        ("status", "status", "string"), ("submitted", "submitted", "timestamp"),
        ("duration_seconds", "duration", "number"), ("queue_seconds", "queued", "number"),
        ("max_rss_bytes", "client peak RSS", "number"), ("args", "args", "string"), ("error", "error", "string")])],
        ds=API, desc="Straight from the lab API (GET /runs)")
    runs["fieldConfig"]["overrides"] = [
        status_override(),
        {"matcher": {"id": "byName", "options": "run"}, "properties": [{"id": "links", "value": [{
            "title": "Run dashboard", "url": "/d/sail-lab-run/sail-lab-run?var-run=${__data.fields.id}&${__url_time_range}"}]}]},
        {"matcher": {"id": "byName", "options": "id"}, "properties": [{"id": "custom.hidden", "value": True}]},
        {"matcher": {"id": "byRegexp", "options": "duration|queued"}, "properties": [{"id": "unit", "value": "s"}, {"id": "decimals", "value": 1}]},
        {"matcher": {"id": "byName", "options": "client peak RSS"}, "properties": [{"id": "unit", "value": "bytes"}]},
    ]
    runs["options"] = {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}
    P.append(L.place(runs, 15, 10))
    P.append(L.place(timeseries("Runs finished per minute", [
        prom('sum by (status) (increase(lab_runs_finished_total[1m]))', "{{status}}")], bars=True, stack=True,
        desc="By final status"), 9, 10))
    P[-1]["fieldConfig"]["overrides"] = [
        {"matcher": {"id": "byName", "options": k}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": v}}]}
        for k, v in STATUS_COLORS.items()]
    dur = bars("Duration of recent runs by job (latest run)", 'max by (lab_job) (lab_run_info{status="succeeded"})',
               unit="s", legend="{{lab_job}}", desc="lab_run_info: the most recent succeeded run per job")
    P.append(L.place(dur, 12, 8))
    P.append(L.place(timeseries("Run duration p50 / p95", [
        prom('histogram_quantile(0.5, sum by (le) (increase(lab_run_duration_seconds_bucket[30m])))', "p50", ref="A"),
        prom('histogram_quantile(0.95, sum by (le) (increase(lab_run_duration_seconds_bucket[30m])))', "p95", ref="B")],
        unit="s", desc="Over a sliding 30-minute window, all jobs"), 12, 8))

    P.append(L.row("Sail engine (from Sail's own OpenTelemetry metrics)"))
    P += [
        L.place(bars("Compute time by operator (range)",
                     "topk(12, sum by (execution_operator_name) (max_over_time(execution_elapsed_compute_time_seconds[$__range])))",
                     unit="s", desc="CPU time summed over partitions; Python UDFs show up under ProjectionExec"), 8, 9),
        L.place(bars("Rows produced by operator (range)",
                     "topk(12, sum by (execution_operator_name) (max_over_time(execution_output_row_count[$__range])))",
                     unit="short"), 8, 9),
        L.place(bars("Spill by operator (range)",
                     "sum by (execution_operator_name) (max_over_time(execution_spill_size_byte[$__range])) > 0",
                     unit="bytes", desc="Empty means nothing spilled. Try jobs/memory_pressure.py"), 8, 9),
        L.place(timeseries("Rows produced per operator (live)", [
            prom("sum by (execution_operator_name) (execution_output_row_count)", "{{execution_operator_name}}")],
            unit="short", desc="Gauges grow while a query runs and disappear ~3 min after it ends"), 12, 8),
        L.place(timeseries("Queries and sessions seen", [
            prom("count(count by (session_id, execution_job_id) (execution_output_row_count)) or vector(0)", "queries", ref="A"),
            prom("count(count by (session_id) (execution_output_row_count)) or vector(0)", "sessions", ref="B")],
            desc="Distinct Sail queries/sessions with metrics in the last ~3 minutes"), 12, 8),
    ]

    P.append(L.row("Resources (cgroup per container) and storage (MinIO)"))
    P += [
        L.place(timeseries("CPU by container", [
            prom("sum by (service) (rate(lab_container_cpu_seconds_total[$__rate_interval]))", "{{service}}")],
            unit="none", desc="Cores in use. lab-api hosts the PySpark clients; sail does the work"), 8, 8),
        L.place(timeseries("Memory by container (working set)", [
            prom("sum by (service) (lab_container_memory_working_set_bytes)", "{{service}}")], unit="bytes"), 8, 8),
        L.place(timeseries("Sail memory vs limit", [
            prom('lab_container_memory_working_set_bytes{service="sail"}', "working set", ref="A"),
            prom('lab_container_memory_limit_bytes{service="sail"}', "container limit", ref="B")],
            unit="bytes", desc="The memory pool is capped below the limit, so Sail spills rather than gets OOM-killed"), 8, 8),
        L.place(timeseries("S3 traffic (MinIO)", [
            prom("sum(rate(minio_s3_traffic_received_bytes[$__rate_interval]))", "written to MinIO", ref="A"),
            prom("sum(rate(minio_s3_traffic_sent_bytes[$__rate_interval]))", "read from MinIO", ref="B")], unit="Bps"), 12, 8),
        L.place(timeseries("S3 requests by API (MinIO)", [
            prom("sum by (api) (rate(minio_s3_requests_total[$__rate_interval])) > 0", "{{api}}")], unit="reqps",
            stack=True), 12, 8),
    ]

    P.append(L.row("Logs"))
    P.append(L.place(logs("Sail server logs", '{service_name="sail-server"} != "option_created"',
                          desc="Sail's own logs, exported over OTLP to Loki"), 24, 10))
    return dashboard("sail-lab-overview", "Sail Lab - Overview", P,
                     desc="LakeSail Sail jobs submitted through the lab API",
                     links=[{"title": "Run detail", "type": "link", "url": "/d/sail-lab-run/sail-lab-run", "icon": "dashboard"}])


# ----------------------------------------------------------------------------- run detail
def run_detail():
    L, P = Layout(), []
    sel = '{session_id="$run"}'
    P.append(L.row("Run"))
    info = panel("table", "Run", [infinity(f"{API_URL}/runs/$run", [
        ("short_id", "run", "string"), ("job", "job", "string"), ("kind", "kind", "string"),
        ("status", "status", "string"), ("submitted", "submitted", "timestamp"),
        ("duration_seconds", "duration", "number"), ("queue_seconds", "queued", "number"),
        ("max_rss_bytes", "client peak RSS", "number"), ("exit_code", "exit", "number"),
        ("args", "args", "string"), ("conf", "conf", "string"), ("error", "error", "string")])],
        ds=API, desc="GET /runs/<id> on the lab API. The run id is also the Spark Connect session id.")
    info["fieldConfig"]["overrides"] = [
        status_override(),
        {"matcher": {"id": "byRegexp", "options": "duration|queued"}, "properties": [{"id": "unit", "value": "s"}, {"id": "decimals", "value": 2}]},
        {"matcher": {"id": "byName", "options": "client peak RSS"}, "properties": [{"id": "unit", "value": "bytes"}]}]
    info["options"] = {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}
    P.append(L.place(info, 24, 4))

    results = panel("table", "Values reported by the job (saillab.sdk.report / timed)",
                    [prom(f'max by (key) (lab_run_result{{run_id="$run"}})', instant=True, fmt="table")],
                    desc="session_ready_seconds is measured by the bootstrap; *_seconds come from timed() blocks")
    results["transformations"] = [{"id": "merge", "options": {}}, {"id": "organize", "options": {"excludeByName": {"Time": True},
                                                                  "renameByName": {"key": "key", "Value": "value"}}}]
    results["options"] = {"showHeader": True, "cellHeight": "sm", "sortBy": [{"displayName": "key"}]}
    P.append(L.place(results, 8, 10))
    P.append(L.place(bars("Compute time by operator", f"sum by (execution_operator_name) (max_over_time(execution_elapsed_compute_time_seconds{sel}[$__range]))",
                          unit="s", desc="Summed over partitions and over all queries of the run"), 8, 10))
    P.append(L.place(bars("Rows produced by operator", f"sum by (execution_operator_name) (max_over_time(execution_output_row_count{sel}[$__range]))",
                          unit="short"), 8, 10))

    P.append(L.row("Queries of this session"))
    q = panel("table", "Per query (Sail execution job)", [
        prom(f"sum by (execution_job_id) (max_over_time(execution_elapsed_compute_time_seconds{sel}[$__range]))", instant=True, fmt="table", ref="A"),
        prom(f"sum by (execution_job_id) (max_over_time(execution_spill_size_byte{sel}[$__range]))", instant=True, fmt="table", ref="B"),
        prom(f"count by (execution_job_id) (max_over_time(execution_output_row_count{sel}[$__range]))", instant=True, fmt="table", ref="C")],
        desc="One row per query Sail executed for this session")
    q["transformations"] = [
        {"id": "merge", "options": {}},
        {"id": "organize", "options": {"excludeByName": {"Time": True}, "renameByName": {
            "execution_job_id": "query", "Value #A": "compute time", "Value #B": "spilled", "Value #C": "operators"}}},
        {"id": "convertFieldType", "options": {"conversions": [{"targetField": "query", "destinationType": "number"}]}},
        {"id": "sortBy", "options": {"sort": [{"field": "query"}]}}]
    q["fieldConfig"]["overrides"] = [
        {"matcher": {"id": "byName", "options": "compute time"}, "properties": [{"id": "unit", "value": "s"}, {"id": "decimals", "value": 3}]},
        {"matcher": {"id": "byName", "options": "spilled"}, "properties": [{"id": "unit", "value": "bytes"}]}]
    q["options"] = {"showHeader": True, "cellHeight": "sm"}
    P.append(L.place(q, 8, 9))
    P.append(L.place(timeseries("Rows produced per operator (live)", [
        prom(f"sum by (execution_operator_name) ({sel[:-1]}, __name__='execution_output_row_count'}})", "{{execution_operator_name}}")],
        unit="short"), 8, 9))
    P.append(L.place(bars("Spill by operator", f"sum by (execution_operator_name) (max_over_time(execution_spill_size_byte{sel}[$__range])) > 0",
                          unit="bytes", desc="Empty means nothing spilled"), 8, 9))

    traces = panel("table", "Query traces (click a trace id to open it in Tempo)", [
        {"datasource": TEMPO, "refId": "A", "queryType": "traceql", "query": '{ span.session.id = "$run" }',
         "limit": 50, "tableType": "traces"}], ds=TEMPO,
        desc="Sail's spans: handle_execute_plan -> JobRunner::execute -> one span per operator")
    P.append(L.place(traces, 24, 8))

    P.append(L.row("Resources while the run was active"))
    P.append(L.place(timeseries("CPU (sail vs PySpark clients in lab-api)", [
        prom('sum by (service) (rate(lab_container_cpu_seconds_total{service=~"sail|lab-api"}[$__rate_interval]))', "{{service}}")],
        unit="none", desc="Cores in use. Shared by all runs active at the same time"), 8, 8))
    P.append(L.place(timeseries("Memory (working set)", [
        prom('sum by (service) (lab_container_memory_working_set_bytes{service=~"sail|lab-api"})', "{{service}}")], unit="bytes"), 8, 8))
    P.append(L.place(timeseries("S3 traffic (MinIO)", [
        prom("sum(rate(minio_s3_traffic_received_bytes[$__rate_interval]))", "written", ref="A"),
        prom("sum(rate(minio_s3_traffic_sent_bytes[$__rate_interval]))", "read", ref="B")], unit="Bps"), 8, 8))

    P.append(L.row("Logs"))
    P.append(L.place(logs("Run log (client stdout/stderr)", '{service_name="lab-run"} | run_id="$run"',
                          desc="The same text as ./lab logs <run>, shipped to Loki by the OTel collector"), 14, 14))
    P.append(L.place(logs("Sail server log lines for this session", '{service_name="sail-server"} |= "$run"'), 10, 14))

    run_var = {"name": "run", "label": "run", "type": "query", "datasource": PROM,
               "query": {"query": "lab_run_info", "refId": "run", "qryType": 3},
               "definition": "lab_run_info",
               # option text = first 8 characters of the run id, value = the whole id
               "regex": '/run_id="(?<value>(?<text>[0-9a-f]{8})[^"]*)"/',
               "refresh": 2, "sort": 0, "includeAll": False, "multi": False,
               "description": "Recent runs. Open from ./lab or the overview table to pick any run"}
    return dashboard("sail-lab-run", "Sail Lab - Run", P, templating=[run_var], time_from="now-30m",
                     desc="One run end to end: its Sail operators, queries, traces, resources and logs",
                     links=[{"title": "Overview", "type": "link", "url": "/d/sail-lab-overview/sail-lab-overview", "icon": "dashboard"}])


if __name__ == "__main__":
    out = Path(__file__).parent / "dashboards"
    out.mkdir(exist_ok=True)
    for name, d in (("overview.json", overview()), ("run.json", run_detail())):
        (out / name).write_text(json.dumps(d, indent=2) + "\n")
        print("wrote", out / name)
