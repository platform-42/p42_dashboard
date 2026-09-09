"""
ops_stats.client
================

Thin wrapper around the `ops.update_stats` PostgreSQL stored function.

Typical usage
--------------
    from ops_stats import update_stats

    update_stats(
        customer_name="Platform42",
        component_type="CHANNEL",
        component_name="WhatsApp",
        total_events=1000,
        total_errors=25,
        average_response_time_ms=312.450,
    )

Connection parameters are picked up the same way `psql` picks them up:
via the standard libpq environment variables (PGHOST, PGPORT, PGDATABASE,
PGUSER, PGPASSWORD) or a `~/.pgpass` file. You can also pass an explicit
conninfo string, or psycopg.connect() kwargs, to override the defaults.
"""

from __future__ import annotations

import logging
from typing import Optional

import psycopg

logger = logging.getLogger(__name__)

_UPDATE_STATS_SQL = """
    SELECT ops.update_stats(
        %s::text,     -- customer_name
        %s::text,     -- component_type
        %s::text,     -- component_name
        %s::integer,  -- total_events
        %s::integer,  -- total_errors
        %s::numeric   -- average_response_time_ms
    )
"""


class OpsStatsClient:
    """
    Reusable client that keeps a single connection open across multiple
    calls. Use this when reporting stats repeatedly in a long-running
    process (a daemon, a batch job, a service loop) so you're not paying
    the cost of a fresh connection on every call.

    Can be used as a context manager:

        with OpsStatsClient() as client:
            client.update_stats("Platform42", "CHANNEL", "WhatsApp", 1000, 25, 312.450)
            client.update_stats("Platform42", "CHANNEL", "SMS", 500, 2, 88.1)
    """

    def __init__(self, conninfo: Optional[str] = None, **conn_kwargs):
        """
        Parameters
        ----------
        conninfo:
            Optional libpq connection string, e.g. "host=localhost dbname=dashboard".
        conn_kwargs:
            Optional keyword args forwarded to psycopg.connect()
            (host=, port=, dbname=, user=, password=, ...).

        If neither conninfo nor conn_kwargs are given, psycopg falls back
        to the standard PG* environment variables / .pgpass, exactly like
        psql does. That's the recommended way to configure this in
        production: set PGHOST/PGDATABASE/PGUSER/PGPASSWORD (or use a
        .pgpass file) and just call OpsStatsClient() or update_stats()
        with no connection arguments at all.
        """
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
        average_response_time_ms: float,
    ) -> None:
        """Call ops.update_stats(...) with the given values and commit."""
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
                        average_response_time_ms,
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

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "OpsStatsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def update_stats(
    customer_name: str,
    component_type: str,
    component_name: str,
    total_events: int,
    total_errors: int,
    average_response_time_ms: float,
    *,
    conninfo: Optional[str] = None,
    **conn_kwargs,
) -> None:
    """
    One-shot convenience function: opens a connection, calls
    ops.update_stats(...), commits, and closes.

    This is the "just call it" entry point for scripts and simple
    reporting sites. If you're calling it repeatedly in a loop or a
    long-running service, prefer OpsStatsClient so you reuse one
    connection instead of opening/closing one every time.
    """
    with OpsStatsClient(conninfo=conninfo, **conn_kwargs) as client:
        client.update_stats(
            customer_name,
            component_type,
            component_name,
            total_events,
            total_errors,
            average_response_time_ms,
        )
