"""Request statistics API for the admin dashboard.

Provides aggregated request statistics across all accounts, including
success/failure rates, per-account breakdowns, and error-type distribution.

All data is derived from the existing ``AccountRepository`` — no new
persistence layer is required.
"""

from __future__ import annotations

import time
from typing import Any

import orjson
from fastapi import APIRouter, Depends, Query, Request, Response

from .tokens import get_repo, _serialize_record
from app.control.account.commands import ListAccountsQuery

router = APIRouter(tags=["Admin - Statistics"])

_TAG = "Admin - Statistics"


def _json(data: Any) -> Response:
    return Response(content=orjson.dumps(data), media_type="application/json")


@router.get("/stats/requests", tags=[_TAG])
async def request_stats(
    request: Request,
    period: str = Query("1d", description="Time period: 1d, 3d, 7d, all"),
) -> Response:
    """Return aggregated request statistics for the given period.

    Response shape::

        {
          "period": "1d",
          "period_seconds": 86400,
          "overall": {
            "total": 1200,
            "success": 1000,
            "fail": 200,
            "success_rate": 0.833,
            "fail_rate": 0.167
          },
          "per_account": [
            {
              "token": "eyJ0eXA...",
              "pool": "super",
              "status": "active",
              "success": 600,
              "fail": 80,
              "success_rate": 0.882,
              "last_used_at": 1716422400000,
              "last_fail_at": 1716422300000,
              "last_fail_reason": "rate_limited (mode=0)"
            },
            ...
          ],
          "error_distribution": {
            "rate_limited": 120,
            "forbidden": 40,
            "auth_failure": 20,
            "server_error": 15,
            "other": 5
          }
        }
    """
    repo = get_repo(request)
    now_ms = int(time.time() * 1000)

    period_seconds = {"1d": 86400, "3d": 259200, "7d": 604800, "all": 0}
    p_sec = period_seconds.get(period, 86400)
    cutoff_ms = (now_ms - p_sec * 1000) if p_sec > 0 else 0

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

    overall_success = 0
    overall_fail = 0
    per_account: list[dict[str, Any]] = []
    error_dist: dict[str, int] = {}

    for r in all_records:
        s = _serialize_record(r)
        use_count = s["use_count"]
        fail_count = s["fail_count"]
        last_used_at = s.get("last_used_at") or 0
        last_fail_at = s.get("last_fail_at") or 0
        last_fail_reason = getattr(r, "last_fail_reason", None) or ""

        if p_sec > 0:
            if last_used_at and last_used_at < cutoff_ms:
                use_count = 0
            if last_fail_at and last_fail_at < cutoff_ms:
                fail_count = 0
                last_fail_reason = ""

        overall_success += use_count
        overall_fail += fail_count

        if fail_count > 0 and last_fail_reason:
            reason = _classify_error(last_fail_reason)
            error_dist[reason] = error_dist.get(reason, 0) + fail_count

        total_account = use_count + fail_count
        per_account.append({
            "token": s["token"][:12] + "..." if len(s["token"]) > 12 else s["token"],
            "pool": s["pool"],
            "status": s["status"],
            "success": use_count,
            "fail": fail_count,
            "success_rate": round(use_count / total_account, 3) if total_account > 0 else None,
            "last_used_at": last_used_at or None,
            "last_fail_at": last_fail_at or None,
            "last_fail_reason": last_fail_reason or None,
        })

    total = overall_success + overall_fail

    return _json({
        "period": period,
        "period_seconds": p_sec,
        "overall": {
            "total": total,
            "success": overall_success,
            "fail": overall_fail,
            "success_rate": round(overall_success / total, 3) if total > 0 else None,
            "fail_rate": round(overall_fail / total, 3) if total > 0 else None,
        },
        "per_account": per_account,
        "error_distribution": error_dist,
    })


def _classify_error(reason: str) -> str:
    """Map a raw ``last_fail_reason`` into a stable category."""
    r = reason.lower()
    if "rate_limited" in r or "429" in r:
        return "rate_limited"
    if "forbidden" in r or "403" in r:
        return "forbidden"
    if "auth" in r or "401" in r or "invalid_credential" in r:
        return "auth_failure"
    if "server" in r or "500" in r or "502" in r or "503" in r:
        return "server_error"
    if "timeout" in r:
        return "timeout"
    if "quota" in r:
        return "quota_exhausted"
    return "other"
