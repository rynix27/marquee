"""Shared ClickHouse client used across the backend (simulator, alert checker,
and direct-query agent tools). The agent's main querying path goes through the
official ClickHouse MCP server (see agent.py) — this module is the thin,
direct client used for writes (simulator) and for the threshold checker,
which needs tight control over query shape and timing.
"""
import os
from functools import lru_cache

import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()


@lru_cache(maxsize=1)
def get_client():
    """Returns a cached ClickHouse client built from environment variables."""
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", 8443)),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DATABASE", "default"),
        secure=os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true",
    )
