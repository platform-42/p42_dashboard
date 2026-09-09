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
        average_response_time_ms=312.450,
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
        %s::numeric   -- average_response_time_ms
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
            client.update_stats("Platform42", "CHANNEL", "WhatsApp", 1000, 25, 312.450)
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
