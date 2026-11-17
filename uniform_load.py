import argparse
import asyncio
import csv
import statistics
import time
from datetime import datetime
from pathlib import Path


class Pool:

    def __init__(self, host, port, size):
        self.host, self.port = host, port
        self.free = asyncio.Queue()
        for _ in range(size):
            self.free.put_nowait(None)          # None = not connected yet

    async def get(self):
        conn = await self.free.get()
        if conn is None or conn[1].is_closing():
            conn = await asyncio.open_connection(self.host, self.port)
        return conn

    def put(self, conn):
        self.free.put_nowait(conn)


async def read_response(reader):
    status_line = await reader.readline()
    if not status_line:
        raise ConnectionError("connection closed")
    status = int(status_line.split()[1])
    length, chunked, close = None, False, False
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        k, _, v = line.decode("latin-1").partition(":")
        k, v = k.strip().lower(), v.strip().lower()
        if k == "content-length":
            length = int(v)
        elif k == "transfer-encoding" and "chunked" in v:
            chunked = True
        elif k == "connection" and v == "close":
            close = True
    if chunked:
        while True:
            size = int((await reader.readline()).split(b";")[0], 16)
            await reader.readexactly(size + 2)
            if size == 0:
                break
    elif length is not None:
        await reader.readexactly(length)
    else:                                       # no length: body ends when the server closes
        await reader.read()
        close = True
    return status, close


async def one_request(pool, raw, timeout_s):
    conn = None
    sent = time.perf_counter()
    try:
        conn = await pool.get()
        sent = time.perf_counter()
        reader, writer = conn
        writer.write(raw)
        await writer.drain()
        status, close = await asyncio.wait_for(read_response(reader), timeout_s)
        if close:
            writer.close()
            conn = None
        return status, sent, ""
    except Exception as e:
        if conn:
            conn[1].close()
        conn = None
        return 0, sent, f"{type(e).__name__}: {e}"[:120]
    finally:
        pool.put(conn)


async def _run(host, port, path, method, body, headers, rate, duration_s, connections, timeout_s):
    head = [f"{method} {path} HTTP/1.1", f"Host: {host}:{port}", "Connection: keep-alive"]
    head += [f"{k}: {v}" for k, v in headers.items()]
    if body is not None:
        head.append(f"Content-Length: {len(body)}")
    raw = ("\r\n".join(head) + "\r\n\r\n").encode() + (body or b"")

    pool = Pool(host, port, connections)
    n = int(rate * duration_s)
    rows = [None] * n
    t0 = time.perf_counter() + 0.2

    async def fire(i, scheduled):
        status, sent, err = await one_request(pool, raw, timeout_s)
        done = time.perf_counter()
        rows[i] = {"id": i, "scheduled_ms": round((scheduled - t0) * 1000, 3),
                   "send_delay_ms": round((sent - scheduled) * 1000, 3),
                   "latency_ms": round((done - scheduled) * 1000, 3),
                   "server_ms": round((done - sent) * 1000, 3),
                   "status": status, "error": err}

    tasks = []
    for i in range(n):
        scheduled = t0 + i / rate
        delay = scheduled - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
        tasks.append(asyncio.create_task(fire(i, scheduled)))
    send_span = time.perf_counter() - t0
    await asyncio.gather(*tasks)
    return rows, send_span


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def run(port, rate, duration_s, slo_ms, host="localhost", path="/", method="GET", body=None,
        headers=None, connections=256, timeout_s=30.0, out_dir=None):
    if isinstance(body, str):
        body = body.encode()
    rows, send_span = asyncio.run(_run(host, port, path, method.upper(), body, headers or {},
                                       rate, duration_s, connections, timeout_s))
    ok = [r for r in rows if 200 <= r["status"] < 300]
    lat = [r["latency_ms"] for r in ok]
    good = [x for x in lat if x <= slo_ms]
    errors = {}
    for r in rows:
        if r["error"]:
            errors[r["error"]] = errors.get(r["error"], 0) + 1
    summary = {
        "target": f"{method.upper()} http://{host}:{port}{path}",
        "rate": rate, "duration_s": duration_s, "slo_ms": slo_ms,
        "sent": len(rows), "send_rps": round(len(rows) / send_span, 1) if send_span > 0 else 0,
        "ok": len(ok), "errors": len(rows) - len(ok),
        "goodput": len(good), "goodput_pct": round(100 * len(good) / len(rows), 2) if rows else 0,
        "goodput_rps": round(len(good) / duration_s, 1),
        "mean_ms": round(statistics.mean(lat), 3) if lat else float("nan"),
        "p50_ms": round(pct(lat, 50), 3), "p95_ms": round(pct(lat, 95), 3),
        "p99_ms": round(pct(lat, 99), 3), "max_ms": round(max(lat), 3) if lat else float("nan"),
        "send_delay_p99_ms": round(pct([r["send_delay_ms"] for r in rows], 99), 3),
        "top_error": max(errors, key=errors.get) if errors else "",
    }

    out = Path(out_dir or Path(__file__).resolve().parent / "results" / datetime.now().strftime("%Y%m%d_%H%M%S"))
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "requests.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    with open(out / "summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=summary.keys())
        w.writeheader()
        w.writerow(summary)
    summary["out_dir"] = str(out)
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Constant-rate HTTP load tester")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--path", default="/")
    ap.add_argument("--method", default="GET")
    ap.add_argument("--rate", type=float, required=True, help="requests per second")
    ap.add_argument("--duration", type=float, required=True, help="seconds")
    ap.add_argument("--slo_ms", type=float, required=True, help="latency limit for goodput")
    ap.add_argument("--body", default=None, help="request body as text")
    ap.add_argument("--body_file", default=None, help="request body from a file (binary ok)")
    ap.add_argument("--header", action="append", default=[], help="'Name: value', repeatable")
    ap.add_argument("--connections", type=int, default=256, help="max requests in flight")
    ap.add_argument("--timeout", type=float, default=30.0, help="per-request timeout, seconds")
    ap.add_argument("--out", default=None, help="results dir (default results/<timestamp>)")
    a = ap.parse_args()

    body = Path(a.body_file).read_bytes() if a.body_file else a.body
    headers = {k.strip(): v.strip() for k, v in (h.split(":", 1) for h in a.header)}
    s = run(a.port, a.rate, a.duration, a.slo_ms, a.host, a.path, a.method, body, headers,
            a.connections, a.timeout, a.out)

    print(f"{s['target']}  rate={s['rate']}/s  {s['duration_s']}s  SLO {s['slo_ms']} ms")
    print(f"sent {s['sent']} ({s['send_rps']}/s)  ok {s['ok']}  errors {s['errors']}"
          + (f"  ({s['top_error']})" if s["top_error"] else ""))
    print(f"goodput {s['goodput']} ({s['goodput_pct']}%, {s['goodput_rps']}/s)")
    print(f"latency ms: mean {s['mean_ms']}  p50 {s['p50_ms']}  p95 {s['p95_ms']}  p99 {s['p99_ms']}  max {s['max_ms']}")
    print(f"results: {s['out_dir']}")
