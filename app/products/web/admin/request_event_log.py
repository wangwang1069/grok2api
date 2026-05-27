"""Lightweight SQLite event log for per-request time-bucketed statistics.

Stores each request outcome as a single row so the admin stats dashboard
can query true per-period counts (1d / 3d / 7d / all).

Completely self-contained — no changes to existing backends required.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Any

_DB_PATH = os.path.join(os.getcwd(), "data", "request_events.db")
_RETENTION_DAYS = 30
_WAL_MODE_LOCK = threading.Lock()

_FEEDBACK_KIND_TO_ERROR: dict[str, str] = {
    "success":           "",
    "unauthorized":      "auth_failure",
    "forbidden":         "forbidden",
    "rate_limited":      "rate_limited",
    "server_error":      "server_error",
}


def _ensure_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, timeout=3)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS request_events ("
        "  id         INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  token      TEXT    NOT NULL,"
        "  success    INTEGER NOT NULL,"
        "  error_kind TEXT    DEFAULT '',"
        "  ts         INTEGER NOT NULL"
        ")"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_re_ts ON request_events(ts)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_re_token ON request_events(token)"
    )
    return conn


def init(db_path: str | None = None) -> None:
    """Initialise the database. Safe to call multiple times."""
    global _DB_PATH
    if db_path:
        _DB_PATH = db_path
    conn = _ensure_db()
    conn.close()


def record(
    token: str,
    success: bool,
    error_kind: str = "",
    ts_ms: int | None = None,
) -> None:
    """Insert a single request-outcome row."""
    ts = ts_ms or int(time.time() * 1000)
    tok = token[:200]
    try:
        conn = _ensure_db()
        conn.execute(
            "INSERT INTO request_events (token, success, error_kind, ts) "
            "VALUES (?, ?, ?, ?)",
            (tok, 1 if success else 0, error_kind, ts),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def query_stats(
    since_ms: int,
    until_ms: int,
) -> dict[str, Any]:
    """Return aggregated statistics for the given time window."""
    conn = _ensure_db()
    try:
        cur = conn.execute(
            "SELECT token, success, error_kind, COUNT(*) AS cnt "
            "FROM request_events "
            "WHERE ts >= ? AND ts <= ? "
            "GROUP BY token, success, error_kind",
            (since_ms, until_ms),
        )
        rows = cur.fetchall()

        per_account: dict[str, dict[str, Any]] = {}
        overall_success = 0
        overall_fail = 0
        error_dist: dict[str, int] = {}

        for token, success, error_kind, cnt in rows:
            if token not in per_account:
                per_account[token] = {"success": 0, "fail": 0, "last_fail_reason": ""}
            if success:
                per_account[token]["success"] += cnt
                overall_success += cnt
            else:
                per_account[token]["fail"] += cnt
                overall_fail += cnt
                if error_kind:
                    per_account[token]["last_fail_reason"] = error_kind
                    ek = error_kind
                    if ek not in error_dist:
                        error_dist[ek] = 0
                    error_dist[ek] += cnt

        per_account_list = sorted(
            [
                {"token": t, **v}
                for t, v in per_account.items()
            ],
            key=lambda x: -(x["success"] + x["fail"]),
        )

        total = overall_success + overall_fail
        return {
            "overall": {
                "total": total,
                "success": overall_success,
                "fail": overall_fail,
                "success_rate": round(overall_success / total, 3) if total > 0 else None,
                "fail_rate": round(overall_fail / total, 3) if total > 0 else None,
            },
            "per_account": per_account_list,
            "error_distribution": error_dist,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


def cleanup(retention_days: int = _RETENTION_DAYS) -> int:
    """Delete records older than *retention_days*."""
    cutoff_ms = int(time.time() * 1000) - retention_days * 86400 * 1000
    conn = _ensure_db()
    try:
        cur = conn.execute(
            "DELETE FROM request_events WHERE ts < ?", (cutoff_ms,)
        )
        conn.commit()
        return cur.rowcount
    finally:
        try:
            conn.close()
        except Exception:
            pass