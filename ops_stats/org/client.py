"""
ops_stats.client
================

Thin wrapper around the `ops.update_stats` and `ops.update_state` PostgreSQL
stored functions.

Typical usage
--------------
    from ops_stats import update_stats, update_state

    update_stats(
        customer_name="Platform42",
        component_type="CHANNEL",
        component_name="WhatsApp",
        total_events=1000,
        total_errors=25,
        total_response_time_ms=312450.0,
    )

    update_state(
        customer_name="Platform42",
        component_type="CHANNEL",
        component_name="WhatsApp",
        available=True,
    )

Connection parameters are picked up from a `.env` file (or the process
environment directly) using the standard libpq environment variable
names: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD. You can also pass
an explicit conninfo string, or psycopg.connect() kwargs, to override
the defaults.
"""

from __future__ import annotations

import logging
import os
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
        %s::boolean   -- available
    )
"""


class OpsClient:
    """
    Reusable client that keeps a single connection open across multiple
    calls. Use this when reporting stats/state repeatedly in a
    long-running process (a daemon, a batch job, a service loop) so
    you're not paying the cost of a fresh connection on every call.

    Can be used as a context manager:

        with OpsClient() as client:
            client.update_stats("Platform42", "CHANNEL", "WhatsApp", 1000, 25, 312450.0)
            client.update_state("Platform42", "CHANNEL", "WhatsApp", True)
    """

    def __init__(
        self,
        conninfo: Optional[str] = None,
        dotenv_path: Optional[Union[str, Path]] = None,
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
        conn_kwargs:
            Optional keyword args forwarded to psycopg.connect()
            (host=, port=, dbname=, user=, password=, ...).

        If neither conninfo nor conn_kwargs are given, connection details
        are read from PG* environment variables - loaded automatically
        from a `.env` file if one is found (see the module docstring for
        the expected variable names). That's the recommended way to
        configure this: drop a `.env` file next to your script and call
        OpsClient() or update_stats() with no connection arguments
        at all.
        """
        _ensure_env_loaded(dotenv_path)
        self._conninfo = conninfo
        self._conn_kwargs = conn_kwargs
        self._conn: Optional[psycopg.Connection] = None

    def _connect(self) -> psycopg.Connection:
        if self._conninfo:
            return psycopg.connect(self._conninfo, **self._conn_kwargs)
        return psycopg.connect(**self._conn_kwargs)

    @property
    def connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn

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
        Call ops.update_stats(...) with the given values and commit.

        Parameters report totals for a batch of N events (the app-side
        "event hysteresis" window, e.g. 100-1000 events) - NOT a
        per-event average. total_response_time_ms is the SUM of response
        times across that batch.

        Server-side, ops.update_stats() aggregates these batch totals
        into fixed time windows (see ops.stats.window_start, typically
        5 minutes) and handles the reset-vs-accumulate decision itself -
        callers just report "here's what happened since I last reported"
        and don't need to think about resets. The true average response
        time per event is computed by the dashboard as
        total_response_time_ms / total_events, not here.
        """
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
            conn.rollback()
            logger.exception(
                "ops.update_stats failed for customer=%s type=%s name=%s",
                customer_name,
                component_type,
                component_name,
            )
            raise

    def update_state(
        self,
        customer_name: str,
        component_type: str,
        component_name: str,
        available: bool,
    ) -> None:
        """Call ops.update_state(...) with the given values and commit."""
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
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception(
                "ops.update_state failed for customer=%s type=%s name=%s",
                customer_name,
                component_type,
                component_name,
            )
            raise

    def close(self) -> None:
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
    One-shot convenience function: opens a connection, calls
    ops.update_stats(...), commits, and closes.

    Parameters report totals for a batch of N events (the app-side
    "event hysteresis" window, e.g. 100-1000 events) - NOT a per-event
    average. total_response_time_ms is the SUM of response times across
    that batch.

    Server-side, ops.update_stats() aggregates these batch totals into
    fixed time windows (see ops.stats.window_start, typically 5 minutes)
    and handles the reset-vs-accumulate decision itself - callers just
    report "here's what happened since I last reported" and don't need
    to think about resets. The true average response time per event is
    computed by the dashboard as total_response_time_ms / total_events,
    not here.

    This is the "just call it" entry point for scripts and simple
    reporting sites. If you're calling it repeatedly in a loop or a
    long-running service, prefer OpsClient so you reuse one
    connection instead of opening/closing one every time.
    """
    with OpsClient(conninfo=conninfo, dotenv_path=dotenv_path, **conn_kwargs) as client:
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
    conninfo: Optional[str] = None,
    dotenv_path: Optional[Union[str, Path]] = None,
    **conn_kwargs,
) -> None:
    """
    One-shot convenience function: opens a connection, calls
    ops.update_state(...), commits, and closes.

    This is the "just call it" entry point for scripts and simple
    reporting sites. If you're calling it repeatedly in a loop or a
    long-running service, prefer OpsClient so you reuse one
    connection instead of opening/closing one every time.
    """
    with OpsClient(conninfo=conninfo, dotenv_path=dotenv_path, **conn_kwargs) as client:
        client.update_state(
            customer_name,
            component_type,
            component_name,
            available,
        )
