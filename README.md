# ops-stats

Thin client for the dashboard's PostgreSQL stored functions
`ops.update_stats` and `ops.update_state`. Hides connection handling,
cursor management, type casting and commits behind plain function calls.

## Build

```bash
python -m build --wheel
```

## Install

```bash
pip install dist/ops_stats-5.5.0-py3-none-any.whl
```

## Configure the connection

`ops_stats` uses `psycopg`, which (same as `psql`) reads the standard
libpq environment variables when no connection details are passed. They
can also come from a `.env` file in the current directory or a parent:

```bash
PGHOST=localhost
PGPORT=5432
PGDATABASE=dashboard
PGUSER=postgres
PGPASSWORD=<password>
```

Never commit real `.env` files or passwords.

## Usage

### Scripts: one-shot calls

```python
from ops_stats import update_stats

update_stats(
    customer_name="Platform42",
    component_type="CHANNEL",
    component_name="WhatsApp",
    total_events=1000,
    total_errors=25,
    total_response_time_ms=312450.0,   # SUM over the batch, not an average
)
```

Connects, writes immediately, commits and closes; no aggregation. For
per-transaction reporting, use `OpsClient` (below).

### State

```python
from ops_stats import update_state

update_state("Platform42", "ORCHESTRATOR", "Orchestrator", available=True)

# Planned maintenance: shown gray instead of red (keyword-only flag)
update_state("Platform42", "ORCHESTRATOR", "Orchestrator",
             available=False, planned_shutdown=True)
```

`available=False` without `planned_shutdown=True` means an abnormal end
(red).

### Applications: report every transaction

Create one client at startup and report each transaction as it happens.
The client adds the numbers up per component and writes the totals once
per flush interval; the application never aggregates anything itself.

```python
from ops_stats import OpsClient

client = OpsClient(flush_interval_s=30)

# per transaction: 1 event, 0 errors, 12 ms
client.update_stats("Platform42", "CHANNEL", "WhatsApp", 1, 0, 12)

# state changes are written immediately
client.update_state("Platform42", "ORCHESTRATOR", "Orchestrator", True)

client.close()   # writes what is still buffered
```

Every `flush_interval_s` seconds a background thread calls
`ops.update_stats` with each component's totals and resets them to zero,
so counters never grow without bound. Keep `flush_interval_s` at most
1/10 of the server's 5-minute stats window: the server stamps a batch
with its arrival time, and the default of 30 s shifts only about 5% of
events into the next window.

- `client.update_stats` never waits on the database and never raises
  database errors. If the database is unreachable, the totals are kept
  and retried at the next flush. Batches the database rejects (e.g. an
  unknown component) are logged and dropped.
- `close()`, leaving a `with` block, and normal interpreter exit flush
  the totals. A hard crash loses at most one interval.
- Thread-safe within a process. Create one client per process, after
  forking (e.g. per gunicorn worker).
- `update_state` is not buffered.

### Overriding connection details

Every entry point accepts a conninfo string or `psycopg.connect()`
keyword arguments:

```python
OpsClient(conninfo="host=localhost dbname=dashboard user=postgres")
update_stats("Platform42", "CHANNEL", "WhatsApp", 1000, 25, 312450.0,
             host="localhost", dbname="dashboard")
```

Prefer environment variables or `.env` for passwords.
