import argparse
import asyncio
import csv
import threading
from datetime import datetime
from pathlib import Path

import mlperf_loadgen as lg

from uniform_load import Pool, one_request

SCENARIOS = {"Server": lg.TestScenario.Server, "Offline": lg.TestScenario.Offline,
             "SingleStream": lg.TestScenario.SingleStream}


def main(a):
    body = Path(a.body_file).read_bytes() if a.body_file else (a.body.encode() if a.body else None)
    headers = {k.strip(): v.strip() for k, v in (h.split(":", 1) for h in a.header)}
    head = [f"{a.method.upper()} {a.path} HTTP/1.1", f"Host: {a.host}:{a.port}", "Connection: keep-alive"]
    head += [f"{k}: {v}" for k, v in headers.items()]
    if body is not None:
        head.append(f"Content-Length: {len(body)}")
    raw = ("\r\n".join(head) + "\r\n\r\n").encode() + (body or b"")

    # HTTP client runs on its own asyncio loop; LoadGen calls issue_queries from its threads
    loop = asyncio.new_event_loop()
    threading.Thread(target=loop.run_forever, daemon=True).start()

    async def make_pool():
        return Pool(a.host, a.port, a.connections)
    pool = asyncio.run_coroutine_threadsafe(make_pool(), loop).result()

    status_counts = {}
    errors = {}
    lock = threading.Lock()

    async def handle(query_id):
        status, _, err = await one_request(pool, raw, a.timeout)
        with lock:
            status_counts[status] = status_counts.get(status, 0) + 1
            if err:
                errors[err] = errors.get(err, 0) + 1
        lg.QuerySamplesComplete([lg.QuerySampleResponse(query_id, 0, 0)])

    def issue_queries(query_samples):
        for q in query_samples:
            asyncio.run_coroutine_threadsafe(handle(q.id), loop)

    def flush_queries():
        pass

    sut = lg.ConstructSUT(issue_queries, flush_queries)
    qsl = lg.ConstructQSL(1024, 1024, lambda s: None, lambda s: None)   # dummy samples: the body is fixed

    settings = lg.TestSettings()
    settings.scenario = SCENARIOS[a.scenario]
    settings.mode = lg.TestMode.PerformanceOnly
    settings.min_duration_ms = int(a.duration * 1000)
    settings.min_query_count = a.min_queries
    if a.scenario == "Server":
        settings.server_target_qps = a.rate
        settings.server_target_latency_ns = int(a.slo_ms * 1e6)
        settings.server_target_latency_percentile = a.percentile / 100
    elif a.scenario == "Offline":
        settings.offline_expected_qps = a.rate
    else:
        settings.single_stream_expected_latency_ns = int(a.slo_ms * 1e6)
        settings.single_stream_target_latency_percentile = a.percentile / 100

    out = Path(a.out or Path(__file__).resolve().parent / "results"
               / f"mlperf_{a.scenario}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    out.mkdir(parents=True, exist_ok=True)
    log_out = lg.LogOutputSettings()
    log_out.outdir = str(out)
    log_out.copy_summary_to_stdout = False
    log_settings = lg.LogSettings()
    log_settings.log_output = log_out

    print(f"MLPerf LoadGen {a.scenario} -> {a.method.upper()} http://{a.host}:{a.port}{a.path}", flush=True)
    lg.StartTestWithLogSettings(sut, qsl, settings, log_settings)
    lg.DestroyQSL(qsl)
    lg.DestroySUT(sut)
    loop.call_soon_threadsafe(loop.stop)

    with open(out / "http.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["status", "count"])
        w.writerows(sorted(status_counts.items()))
        for e, n in errors.items():
            w.writerow([e, n])

    summary = (out / "mlperf_log_summary.txt").read_text()
    keep = ("Scenario", "Result is", "Scheduled samples per second", "Completed samples per second",
            "Samples per second", "latency (ns)", "percentile latency", "Min duration satisfied",
            "Min queries satisfied", "Early stopping satisfied", "Recommendations", " * ")
    for line in summary.splitlines():
        if line.strip().startswith(keep) or any(k in line for k in keep[:8]):
            print(line)
    ok = sum(n for s, n in status_counts.items() if 200 <= s < 300)
    total = sum(status_counts.values())
    print(f"HTTP: {ok}/{total} 2xx" + (f", errors: {max(errors, key=errors.get)}" if errors else ""))
    print(f"results: {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="MLPerf LoadGen against an HTTP endpoint")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--path", default="/")
    ap.add_argument("--method", default="GET")
    ap.add_argument("--body", default=None)
    ap.add_argument("--body_file", default=None)
    ap.add_argument("--header", action="append", default=[], help="'Name: value', repeatable")
    ap.add_argument("--scenario", choices=list(SCENARIOS), default="Server")
    ap.add_argument("--rate", type=float, default=100, help="Server: target QPS; Offline: expected QPS")
    ap.add_argument("--slo_ms", type=float, default=100, help="Server/SingleStream latency target")
    ap.add_argument("--percentile", type=float, default=99, help="latency percentile checked against --slo_ms")
    ap.add_argument("--duration", type=float, default=60, help="minimum run length, seconds")
    ap.add_argument("--min_queries", type=int, default=1, help="minimum query count (MLPerf official: much larger)")
    ap.add_argument("--connections", type=int, default=256, help="max HTTP requests in flight")
    ap.add_argument("--timeout", type=float, default=30.0, help="per-request timeout, seconds")
    ap.add_argument("--out", default=None)
    main(ap.parse_args())
