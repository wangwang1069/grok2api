"""Request statistics API for the admin dashboard.

Provides aggregated request statistics across all accounts, including
success/failure rates, per-account breakdowns, and error-type distribution.

Uses a lightweight SQLite event log (``request_event_log``) for true
per-period counts, enriched with account metadata from the repository.
"""

from __future__ import annotations

import time
from typing import Any

import orjson
from fastapi import APIRouter, Query, Request, Response

from .tokens import get_repo, _serialize_record
from .request_event_log import query_stats as _query_event_stats
from app.control.account.commands import ListAccountsQuery

router = APIRouter(tags=["Admin - Statistics"])

_TAG = "Admin - Statistics"


def _json(data: Any) -> Response:
    return Response(content=orjson.dumps(data), media_type="application/json")


_PERIOD_SECONDS = {"1d": 86400, "3d": 259200, "7d": 604800, "all": 0}


@router.get("/stats/requests", tags=[_TAG])
async def request_stats(
    request: Request,
    period: str = Query("1d", description="Time period: 1d, 3d, 7d, all"),
) -> Response:
    now_ms = int(time.time() * 1000)
    p_sec = _PERIOD_SECONDS.get(period, 86400)
    cutoff_ms = (now_ms - p_sec * 1000) if p_sec > 0 else 0

    event_stats = _query_event_stats(since_ms=cutoff_ms, until_ms=now_ms)
    overall = event_stats["overall"]
    event_per_account: dict[str, dict[str, Any]] = {}
    for a in event_stats["per_account"]:
        event_per_account[a["token"]] = a

    repo = get_repo(request)
    all_records = []
    page_num = 1
    while True:
        page = await repo.list_accounts(
            ListAccountsQuery(page=page_num, page_size=2000)
        )
        all_records.extend(page.items)
        if page_num * 2000 >= page.total:
            break
        page_num += 1

    per_account: list[dict[str, Any]] = []
    for r in all_records:
        s = _serialize_record(r)
        full_token = s["token"]
        short_token = full_token[:12] + "..." if len(full_token) > 12 else full_token
        evt = event_per_account.pop(full_token, None)
        if evt:
            success = evt["success"]
            fail = evt["fail"]
            total_account = success + fail
            rate = round(success / total_account, 3) if total_account > 0 else None
            per_account.append({
                "token": short_token,
                "pool": s["pool"],
                "status": s["status"],
                "success": success,
                "fail": fail,
                "success_rate": rate,
                "last_used_at": s.get("last_used_at") or None,
                "last_fail_at": s.get("last_fail_at") or None,
                "last_fail_reason": evt.get("last_fail_reason") or None,
            })
        else:
            per_account.append({
                "token": short_token,
                "pool": s["pool"],
                "status": s["status"],
                "success": 0,
                "fail": 0,
                "success_rate": None,
                "last_used_at": s.get("last_used_at") or None,
                "last_fail_at": s.get("last_fail_at") or None,
                "last_fail_reason": None,
            })

    return _json({
        "period": period,
        "period_seconds": p_sec,
        "overall": overall,
        "per_account": per_account,
        "error_distribution": event_stats["error_distribution"],
    })