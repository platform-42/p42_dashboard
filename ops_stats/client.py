"""
ops_stats.client
================

Thin client for the dashboard's `ops.update_stats` and `ops.update_state`
PostgreSQL stored functions.

Typical usage: a long-running application
-----------------------------------------
Create one client at startup and report every transaction as it
happens. The client adds the numbers up per component and writes the
totals to the database once per flush interval; the application never
aggregates anything itself.

    from ops_stats import OpsClient

    client = OpsClient(flush_interval_s=30)

    # once per transaction: 1 event, 0 errors, 12 ms
    client.update_stats("Platform42", "CHANNEL", "WhatsApp", 1, 0, 12)

    # state changes are written immediately
    client.update_state("Platform42", "ORCHESTRATOR", "Orchestrator", True)

    # planned maintenance: shown gray instead of red
    client.update_state("Platform42", "ORCHESTRATOR", "Orchestrator",
                        False, planned_shutdown=True)

    client.close()   # writes what is still buffered

Scripts: one-shot functions
---------------------------
update_stats() and update_state() at module level open a connection,
write immediately, and close. Use them for occasional calls; for
per-transaction reporting use OpsClient.

Connection parameters are picked up from a `.env` file (or the process
environment directly) using the standard libpq environment variable
names: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD. You can also pass
an explicit conninfo string, or psycopg.connect() kwargs, to override
the defaults.
"""

from __future__ import annotations

import atexit
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import psycopg
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_ENV_LOADED = False


def _ensure_env_loaded(dotenv_path: Optional[Union[str, Path]] = None) -> None:
    """
    Load a .env file into the process environment, once.

    By default this searches the current directory and its parents for a
    file named `.env` (same lookup behaviour as python-dotenv's default).
    Pass dotenv_path explicitly to point at a specific file instead.

    Values already set in the real environment are NOT overridden by the
    .env file - explicit env vars always win, same as libpq's own
    precedence rules.
    """
    global _ENV_LOADED
    if dotenv_path is not None:
        # Explicit path: always (re)load it, don't rely on the cached flag.
        load_dotenv(dotenv_path=dotenv_path, override=False)
        return
    if not _ENV_LOADED:
        load_dotenv(override=False)
        _ENV_LOADED = True

_UPDATE_STATS_SQL = """
    SELECT ops.update_stats(
        %s::text,     -- customer_name
        %s::text,     -- component_type
        %s::text,     -- component_name
        %s::integer,  -- total_events
        %s::integer,  -- total_errors
        %s::numeric   -- total_response_time_ms (SUM over the reported batch, not an average)
    )
"""

_UPDATE_STATE_SQL = """
    SELECT ops.update_state(
        %s::text,     -- customer_name
        %s::text,     -- component_type
        %s::text,     -- component_name
        %s::boolean,  -- available
        %s::boolean   -- planned_shutdown (only meaningful when available = false)
    )
"""


def _validate_state_args(available: bool, planned_shutdown: bool) -> None:
    """A planned shutdown only makes sense for a component that is down."""
    if available and planned_shutdown:
        raise ValueError(
            "planned_shutdown=True requires available=False "
            "(a running component cannot be in planned maintenance)"
        )


_StatsKey = tuple[str, str, str]  # (customer_name, component_type, component_name)


@dataclass
class _StatsBucket:
    """Running totals for one component since the last flush."""

    total_events: int = 0
    total_errors: int = 0
    total_response_time_ms: float = 0.0
    reports: int = 0  # number of update_stats() calls folded in

    def add(self, events: int, errors: int, response_ms: float) -> None:
        self.total_events += events
        self.total_errors += errors
        self.total_response_time_ms += response_ms
        self.reports += 1

    def merge(self, other: "_StatsBucket") -> None:
        """Fold a batch whose write failed back in, to retry next flush."""
        self.total_events += other.total_events
        self.total_errors += other.total_errors
        self.total_response_time_ms += other.total_response_time_ms
        self.reports += other.reports


class OpsClient:
    """
    Client for reporting stats and state from a long-running process.

    Stats are aggregated in memory
    ------------------------------
    update_stats() only adds the numbers to running totals for that
    component (customer, type, name) and returns at once; it never
    touches the database. A background thread writes all totals every
    flush_interval_s seconds through ops.update_stats() and then resets
    them to zero, so counters never grow without bound: each flush
    starts a fresh set, and a component that goes quiet disappears from
    memory until it reports again.

    Choosing flush_interval_s
    -------------------------
    The server stamps each batch with the time it ARRIVES, so a
    transaction can land in the next 5-minute stats window when a flush
    happens just after a window boundary. On average about
    (flush_interval_s / 2) / window of the events shift into the next
    window. Keep flush_interval_s at most 1/10 of the window: the default
    of 30 s shifts about 5%; 150 s would shift about 25%.

    Guarantees
    ----------
      - update_stats() never raises database errors and never waits on
        the database: all writes happen in the background thread.
      - Database unreachable: the totals are kept and retried at the
        next flush (summed, so memory stays one set per component).
      - Batch rejected by the database (e.g. unknown component): logged
        as an error and dropped, because retrying cannot succeed.
      - close(), leaving a `with` block, and normal interpreter exit
        write what is still buffered. A hard crash loses at most one
        interval of stats.
      - Thread-safe: application threads may share one client. Create one
        client per process, after forking (e.g. per gunicorn worker),
        because the background thread does not survive a fork.
      - update_state() is not buffered: state changes are written
        immediately, and database errors are raised.

    Usage:

        with OpsClient(flush_interval_s=30) as client:
            client.update_stats("Platform42", "CHANNEL", "WhatsApp", 1, 0, 12)
            client.update_state("Platform42", "ORCHESTRATOR", "Orchestrator", True)
    """

    def __init__(
        self,
        conninfo: Optional[str] = None,
        dotenv_path: Optional[Union[str, Path]] = None,
        *,
        flush_interval_s: Optional[float] = 30.0,
        **conn_kwargs,
    ):
        """
        Parameters
        ----------
        conninfo:
            Optional libpq connection string, e.g. "host=localhost dbname=dashboard".
        dotenv_path:
            Optional path to a specific .env file. If omitted, the default
            .env lookup (current directory and parents) is used.
        flush_interval_s:
            Seconds between writes of the aggregated stats. Default 30.
            None writes every update_stats() call immediately instead
            (used by the one-shot functions; raises database errors).
        conn_kwargs:
            Optional keyword args forwarded to psycopg.connect()
            (host=, port=, dbname=, user=, password=, ...).

        If neither conninfo nor conn_kwargs are given, connection details
        are read from PG* environment variables, loaded automatically
        from a `.env` file if one is found.
        """
        if flush_interval_s is not None and flush_interval_s <= 0:
            raise ValueError("flush_interval_s must be > 0 (or None for immediate writes)")

        _ensure_env_loaded(dotenv_path)
        self._conninfo = conninfo
        self._conn_kwargs = conn_kwargs
        self._conn: Optional[psycopg.Connection] = None

        # Serializes all use of the connection (application threads and
        # the flush thread share it).
        self._db_lock = threading.RLock()

        self._flush_interval_s = flush_interval_s
        self._buffer: dict[_StatsKey, _StatsBucket] = {}
        self._buffer_lock = threading.Lock()
        self._stop = threading.Event()
        self._flusher: Optional[threading.Thread] = None
        self._closed = False

        if self.buffered:
            self._flusher = threading.Thread(
                target=self._flush_loop, name="ops_stats-flusher", daemon=True
            )
            self._flusher.start()
            # Write what is buffered on normal interpreter exit; close()
            # unregisters this.
            atexit.register(self.close)

    @property
    def buffered(self) -> bool:
        """True when update_stats() aggregates in memory (the default)."""
        return self._flush_interval_s is not None

    def _connect(self) -> psycopg.Connection:
        if self._conninfo:
            return psycopg.connect(self._conninfo, **self._conn_kwargs)
        return psycopg.connect(**self._conn_kwargs)

    @property
    def connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn

    # -- stats --------------------------------------------------------------------

    def update_stats(
        self,
        customer_name: str,
        component_type: str,
        component_name: str,
        total_events: int,
        total_errors: int,
        total_response_time_ms: float,
    ) -> None:
        """
        Report stats for a component, typically one transaction:

            client.update_stats("Platform42", "CHANNEL", "WhatsApp", 1, 0, 12)

        The numbers are added to this component's running totals and
        written at the next flush (see the class docstring). Passing
        larger batches (e.g. 10 events, 1 error, 140 ms summed) works the
        same way; total_response_time_ms is always a SUM, never an
        average. The dashboard derives the average as
        total_response_time_ms / total_events.
        """
        if not self.buffered:
            self._write_stats(
                customer_name,
                component_type,
                component_name,
                total_events,
                total_errors,
                total_response_time_ms,
            )
            return

        if self._closed:
            raise RuntimeError("OpsClient is closed")

        key: _StatsKey = (customer_name, component_type, component_name)
        with self._buffer_lock:
            self._buffer.setdefault(key, _StatsBucket()).add(
                total_events, total_errors, total_response_time_ms
            )

    def flush(self) -> None:
        """Write all aggregated stats now, instead of waiting for the next interval."""
        if not self.buffered:
            return
        # Take the current totals and give the application fresh counters.
        with self._buffer_lock:
            batches = self._buffer
            self._buffer = {}

        for i, (key, bucket) in enumerate(batches.items()):
            if not self._write_batch(key, bucket):
                # Database unreachable: keep this and all remaining
                # batches for the next flush, without trying each one.
                self._keep(list(batches.items())[i:])
                return

    # -- internals ------------------------------------------------------------------

    def _flush_loop(self) -> None:
        """Background thread: flush every flush_interval_s until close()."""
        while not self._stop.wait(self._flush_interval_s):
            try:
                self.flush()
            except Exception:  # never let the flush thread die
                logger.exception("ops_stats: unexpected error in flush thread")

    def _keep(self, batches: list[tuple[_StatsKey, _StatsBucket]]) -> None:
        with self._buffer_lock:
            for key, bucket in batches:
                self._buffer.setdefault(key, _StatsBucket()).merge(bucket)

    def _write_batch(self, key: _StatsKey, bucket: _StatsBucket) -> bool:
        """
        Write one component's totals. Never raises.
        Returns False only when the database is unreachable (caller keeps
        the batch); a rejected batch is logged and counts as handled.
        """
        try:
            self._write_stats(
                *key,
                bucket.total_events,
                bucket.total_errors,
                bucket.total_response_time_ms,
                log_failure=False,
            )
            return True
        except psycopg.OperationalError as exc:
            logger.warning(
                "ops_stats: database unavailable (%s); keeping stats for the next "
                "flush in %.0f s",
                str(exc).splitlines()[0],
                self._flush_interval_s,
            )
            self._drop_connection()
            return False
        except Exception:
            logger.exception(
                "ops_stats: dropped stats (%d events from %d reports) for "
                "customer=%s type=%s name=%s",
                bucket.total_events,
                bucket.reports,
                *key,
            )
            return True

    def _write_stats(
        self,
        customer_name: str,
        component_type: str,
        component_name: str,
        total_events: int,
        total_errors: int,
        total_response_time_ms: float,
        *,
        log_failure: bool = True,
    ) -> None:
        """Call ops.update_stats(...) and commit. Raises on failure."""
        with self._db_lock:
            conn = self.connection
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        _UPDATE_STATS_SQL,
                        (
                            customer_name,
                            component_type,
                            component_name,
                            total_events,
                            total_errors,
                            total_response_time_ms,
                        ),
                    )
                conn.commit()
            except Exception:
                self._safe_rollback(conn)
                if log_failure:
                    logger.exception(
                        "ops.update_stats failed for customer=%s type=%s name=%s",
                        customer_name,
                        component_type,
                        component_name,
                    )
                raise

    @staticmethod
    def _safe_rollback(conn: psycopg.Connection) -> None:
        """Roll back, ignoring errors from a connection that is already broken."""
        if conn.closed:
            return
        try:
            conn.rollback()
        except Exception:
            pass

    def _drop_connection(self) -> None:
        with self._db_lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None

    # -- state ----------------------------------------------------------------------

    def update_state(
        self,
        customer_name: str,
        component_type: str,
        component_name: str,
        available: bool,
        *,
        planned_shutdown: bool = False,
    ) -> None:
        """
        Call ops.update_state(...) with the given values and commit.

        Resulting state stored server-side:

            available=True                          -> 'UP'           (green)
            available=False                         -> 'DOWN'         (red, ABEND)
            available=False, planned_shutdown=True  -> 'MAINTENANCE'  (gray)

        planned_shutdown defaults to False, so a component that goes down
        is treated as an abnormal end unless the caller explicitly says
        the shutdown was planned. It is keyword-only to keep call sites
        readable (no bare True/False pairs).

        Raises ValueError for available=True with planned_shutdown=True.
        """
        _validate_state_args(available, planned_shutdown)
        with self._db_lock:
            conn = self.connection
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        _UPDATE_STATE_SQL,
                        (
                            customer_name,
                            component_type,
                            component_name,
                            available,
                            planned_shutdown,
                        ),
                    )
                conn.commit()
            except Exception:
                self._safe_rollback(conn)
                logger.exception(
                    "ops.update_state failed for customer=%s type=%s name=%s",
                    customer_name,
                    component_type,
                    component_name,
                )
                raise

    # -- lifecycle ------------------------------------------------------------------

    def close(self) -> None:
        """
        Stop the flush thread, write what is still buffered, and close
        the connection. A buffered client cannot be used after close().
        Safe to call more than once.
        """
        if self.buffered and not self._closed:
            self._closed = True
            self._stop.set()
            if self._flusher is not None and self._flusher is not threading.current_thread():
                self._flusher.join(timeout=10)
            self.flush()
            with self._buffer_lock:
                lost = sum(b.total_events for b in self._buffer.values())
                self._buffer.clear()
            if lost:
                logger.error(
                    "ops_stats: %d event(s) could not be written before close", lost
                )
            atexit.unregister(self.close)
        with self._db_lock:
            if self._conn is not None and not self._conn.closed:
                self._conn.close()
            self._conn = None

    def __enter__(self) -> "OpsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def update_stats(
    customer_name: str,
    component_type: str,
    component_name: str,
    total_events: int,
    total_errors: int,
    total_response_time_ms: float,
    *,
    conninfo: Optional[str] = None,
    dotenv_path: Optional[Union[str, Path]] = None,
    **conn_kwargs,
) -> None:
    """
    One-shot convenience function for scripts: opens a connection,
    calls ops.update_stats(...) immediately, commits, and closes.
    Database errors are raised.

    No aggregation happens here: each call is one database write. For
    per-transaction reporting in a long-running process, use OpsClient,
    which aggregates in memory and writes once per flush interval.
    total_response_time_ms is the SUM over the reported events.
    """
    with OpsClient(
        conninfo=conninfo, dotenv_path=dotenv_path, flush_interval_s=None, **conn_kwargs
    ) as client:
        client.update_stats(
            customer_name,
            component_type,
            component_name,
            total_events,
            total_errors,
            total_response_time_ms,
        )


def update_state(
    customer_name: str,
    component_type: str,
    component_name: str,
    available: bool,
    *,
    planned_shutdown: bool = False,
    conninfo: Optional[str] = None,
    dotenv_path: Optional[Union[str, Path]] = None,
    **conn_kwargs,
) -> None:
    """
    One-shot convenience function: opens a connection, calls
    ops.update_state(...), commits, and closes.

    planned_shutdown=True marks a deliberate stop (planned maintenance),
    shown gray on the dashboard. The default (False) treats any
    available=False report as an abnormal end, shown red. See
    OpsClient.update_state for the full state mapping.

    This is the "just call it" entry point for scripts and simple
    reporting sites. If you're calling it repeatedly in a loop or a
    long-running service, prefer OpsClient so you reuse one
    connection instead of opening/closing one every time.
    """
    with OpsClient(
        conninfo=conninfo, dotenv_path=dotenv_path, flush_interval_s=None, **conn_kwargs
    ) as client:
        client.update_state(
            customer_name,
            component_type,
            component_name,
            available,
            planned_shutdown=planned_shutdown,
        )
