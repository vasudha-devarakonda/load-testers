# load_tester

Two HTTP load testers for any `host:port` (default `localhost`). Each request is one HTTP call.

| Script | Arrival pattern | Needs |
|---|---|---|
| `uniform_load.py` | Uniform: one request every `1/rate` s, exactly evenly spaced | Python 3 standard library |
| `mlperf_load.py` | MLPerf LoadGen: Server (Poisson), Offline (all at once) or SingleStream (one at a time) | `mlcommons-loadgen` + `uniform_load.py` in the same folder |

Both are open loop: a slow server does not slow down the sender.

## Setup

`uniform_load.py` runs with any Python 3.

`mlperf_load.py` needs MLCommons LoadGen, which is published as `mlcommons-loadgen` and has no build for Python 3.14:

```bash
conda create -y -n loadtest python=3.11
conda activate loadtest
pip install mlcommons-loadgen
```

On this machine it is already installed: `PY=/home/cc/miniconda3/envs/loadtest/bin/python`.

## Uniform load: `uniform_load.py`

```bash
python uniform_load.py --port 8000 --rate 200 --duration 30 --slo_ms 100

python uniform_load.py --port 8000 --path /predict --method POST \
    --body '{"x": [1, 2, 3]}' --header 'Content-Type: application/json' \
    --rate 500 --duration 60 --slo_ms 50

python uniform_load.py --port 8000 --body_file request.bin --rate 100 --duration 10
```

| Option | Default | Meaning |
|---|---|---|
| `--port` | required | server port |
| `--host` | `localhost` | server host |
| `--path` | `/` | URL path |
| `--method` | `GET` | HTTP method |
| `--rate` | required | requests per second |
| `--duration` | required | seconds |
| `--slo_ms` | required | latency limit for goodput |
| `--body` / `--body_file` | none | request body as text / from a file |
| `--header` | none | `'Name: value'`, repeatable |
| `--connections` | 256 | max requests in flight |
| `--timeout` | 30 | per-request timeout, seconds |
| `--out` | `results/<timestamp>` | output folder |

- **Latency:** measured from each request's scheduled send time to the full response, so waiting on the client side counts.
- **Goodput:** requests with a 2xx status and latency ≤ `--slo_ms`.

Output in `results/<timestamp>/`:
- `requests.csv`: one row per request (`scheduled_ms`, `send_delay_ms`, `latency_ms`, `server_ms`, `status`, `error`)
- `summary.csv`: sent, send rate, ok, errors, goodput (count, %, req/s), latency mean/p50/p95/p99/max

From Python:

```python
from uniform_load import run
s = run(port=8000, rate=200, duration_s=30, slo_ms=100)
print(s["goodput_pct"], s["p99_ms"])
```

## MLPerf load: `mlperf_load.py`

```bash
$PY mlperf_load.py --port 8000 --scenario Server --rate 300 --slo_ms 20 --duration 60
$PY mlperf_load.py --port 8000 --scenario Offline --rate 500 --duration 60
$PY mlperf_load.py --port 8000 --scenario SingleStream --slo_ms 20 --duration 30
```

| Scenario | How queries are sent | VALID when |
|---|---|---|
| `Server` | Poisson arrivals around `--rate` | the `--percentile` latency (default p99) ≤ `--slo_ms` |
| `Offline` | all at once | the run lasts at least `--duration` |
| `SingleStream` | next query when the previous one finishes | reports latency percentiles |

Options are the same as `uniform_load.py` (`--port`, `--host`, `--path`, `--method`, `--body`, `--body_file`, `--header`, `--connections`, `--timeout`, `--out`), plus:

| Option | Default | Meaning |
|---|---|---|
| `--scenario` | `Server` | `Server`, `Offline` or `SingleStream` |
| `--rate` | 100 | Server: target QPS. Offline: expected QPS |
| `--slo_ms` | 100 | latency target (Server, SingleStream) |
| `--percentile` | 99 | latency percentile checked against `--slo_ms` |
| `--duration` | 60 | minimum run length, seconds |
| `--min_queries` | 1 | minimum query count |

Output in `results/mlperf_<scenario>_<timestamp>/`:
- `mlperf_log_summary.txt`: the result (VALID/INVALID), throughput, latency percentiles, and why a run is INVALID
- `mlperf_log_detail.txt`, `mlperf_log_trace.json`: LoadGen's detailed logs
- `http.csv`: HTTP status counts. LoadGen only sees when a query finishes, not whether it succeeded, so check this for errors.

Tips:
- **Server:** run at least ~30 s. Shorter runs have too few queries for MLPerf's early-stopping check and come out INVALID.
- **Offline:** set `--rate` close to the server's real throughput. If the server is faster, the run ends before `--duration` and is INVALID.
- **Official MLPerf length:** pass the MLPerf minimum query count, for example `--min_queries 270336` for Server.

MLPerf has no uniform (fixed-interval) arrival scenario. For evenly spaced requests use `uniform_load.py`.
