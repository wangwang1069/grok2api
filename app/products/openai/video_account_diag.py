"""Video job account-selection diagnostics.

Wraps ``AccountDirectory.reserve_any`` with structured diagnostic logs so
operators can quickly answer three questions:

* **Why no accounts are available?** — every candidate is logged with the
  exact filter reason (status / quota / cooling / inflight).
* **Why was *this* account chosen?** — score breakdown (quota strategy) or
  surviving-filter summary (random strategy) for all candidates.
* **What does the account look like?** — token prefix, per-mode quota,
  health, inflight, fail-count, pool, status.

Usage (minimal change to *video.py*)::

    from .video_account_diag import reserve_and_diagnose

    acct = await reserve_and_diagnose(
        _acct_dir,
        pool_candidates=spec.pool_candidates(),
        spec=spec,
        job_id=job.id,
    )
    if acct is None:
        raise RateLimitError("No available accounts for video generation")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from app.platform.logging.logger import logger
from app.dataplane.account.selector import (
    _RECENT_WINDOW_S,
    _W_FAIL,
    _W_HEALTH,
    _W_INFLIGHT,
    _W_QUOTA,
    _W_RECENT,
    _best_no_quota,
    _pool_union,
    current_strategy,
)
from app.dataplane.shared.enums import ModeId, POOL_ID_TO_STR, StatusId

if TYPE_CHECKING:
    from app.control.model.spec import ModelSpec
    from app.dataplane.account import AccountDirectory
    from app.dataplane.account.table import AccountRuntimeTable

_MODE_NAMES: dict[int, str] = {
    int(ModeId.AUTO): "auto",
    int(ModeId.FAST): "fast",
    int(ModeId.EXPERT): "expert",
    int(ModeId.HEAVY): "heavy",
    int(ModeId.GROK_4_3): "grok_4_3",
}

_STATUS_NAMES: dict[int, str] = {
    int(StatusId.ACTIVE): "ACTIVE",
    int(StatusId.COOLING): "COOLING",
    int(StatusId.EXPIRED): "EXPIRED",
    int(StatusId.DISABLED): "DISABLED",
    int(StatusId.DELETED): "DELETED",
}


def _mask_token(token: str) -> str:
    if len(token) <= 12:
        return token[:4] + "..."
    return token[:8] + "..." + token[-4:]


def _pool_name(pool_id: int) -> str:
    return POOL_ID_TO_STR.get(pool_id, f"pool({pool_id})")


def _mode_name(mode_id: int) -> str:
    return _MODE_NAMES.get(mode_id, f"mode({mode_id})")


def _status_name(status_id: int) -> str:
    return _STATUS_NAMES.get(status_id, f"status({status_id})")


def _snapshot_account(
    table: AccountRuntimeTable, idx: int, mode_id: int, now_s: int
) -> dict[str, Any]:
    token = table.get_token(idx)
    quota = table.quota_for(idx, mode_id)
    total = int(table._total_col(mode_id)[idx])
    window = int(table._window_col(mode_id)[idx])
    reset_at = int(table._reset_col(mode_id)[idx])
    health = float(table.health_by_idx[idx])
    inflight = int(table.inflight_by_idx[idx])
    fails = int(table.fail_count_by_idx[idx])
    status = int(table.status_by_idx[idx])
    pool_id = table.get_pool_id(idx)
    last_use = int(table.last_use_at_by_idx[idx])
    last_fail = int(table.last_fail_at_by_idx[idx])
    cooling_until = int(table.cooling_until_s_by_idx[idx])

    all_quotas = {}
    for mid in (0, 1, 2, 3, 4):
        q = table.quota_for(idx, mid)
        t = int(table._total_col(mid)[idx])
        if q >= 0 or t > 0:
            all_quotas[_mode_name(mid)] = {"remaining": q, "total": t}

    return {
        "idx": idx,
        "token": _mask_token(token),
        "pool": _pool_name(pool_id),
        "status": _status_name(status),
        "target_mode_quota": quota,
        "target_mode_total": total,
        "window_sec": window,
        "reset_at_epoch": reset_at,
        "health": round(health, 3),
        "inflight": inflight,
        "fail_count": fails,
        "last_use_ago_s": (now_s - last_use) if last_use > 0 else -1,
        "last_fail_ago_s": (now_s - last_fail) if last_fail > 0 else -1,
        "cooling_until_s": cooling_until,
        "cooling_remaining_s": max(0, cooling_until - now_s),
        "all_quotas": all_quotas,
    }


def _score_breakdown(
    info: dict[str, Any], now_s: int
) -> dict[str, Any]:
    health = info["health"]
    inflight = info["inflight"]
    fails = min(info["fail_count"], 10)
    last_use_ago = info["last_use_ago_s"]

    s_health = health * _W_HEALTH
    s_inflight = -(inflight * _W_INFLIGHT)
    s_fails = -(fails * _W_FAIL)
    s_recent = 0.0
    if last_use_ago >= 0 and last_use_ago < _RECENT_WINDOW_S:
        s_recent = -((1.0 - last_use_ago / _RECENT_WINDOW_S) * _W_RECENT)

    total = s_health + s_inflight + s_fails + s_recent
    return {
        "total": round(total, 2),
        "health_part": round(s_health, 2),
        "inflight_penalty": round(s_inflight, 2),
        "fail_penalty": round(s_fails, 2),
        "recent_penalty": round(s_recent, 2),
        "detail": (
            f"health={health:.3f}*{_W_HEALTH}={s_health:.1f} "
            f"-inflight={inflight}*{_W_INFLIGHT}={s_inflight:.1f} "
            f"-fails={fails}*{_W_FAIL}={s_fails:.1f}"
            + (f" -recent={s_recent:.1f}" if s_recent != 0 else "")
        ),
    }


def _format_candidates(
    accounts: list[dict[str, Any]],
    selected_idx: int | None,
    now_s: int,
    strategy: str,
) -> str:
    lines: list[str] = []
    lines.append(f"{'idx':>4s} {'token':>16s} {'pool':>6s} {'status':>10s} {'quota':>6s} {'health':>7s} {'in':>4s} {'fail':>4s}")
    lines.append("-" * 72)

    for acc in accounts:
        idx = acc["idx"]
        marker = " >>>" if idx == selected_idx else "    "
        q = acc["target_mode_quota"]
        quota_str = str(q) if q >= 0 else "?"
        lines.append(
            f"{marker}{idx:>4d} {acc['token']:>16s} {acc['pool']:>6s} {acc['status']:>10s} {quota_str:>6s} {acc['health']:>7.3f} {acc['inflight']:>4d} {acc['fail_count']:>4d}"
        )

    if selected_idx is not None and strategy == "quota":
        sel = next((a for a in accounts if a["idx"] == selected_idx), None)
        if sel:
            sb = _score_breakdown(sel, now_s)
            lines.append("")
            lines.append(
                f"Selected #{selected_idx} score breakdown: {sb['detail']}"
            )
            others = [a for a in accounts if a["idx"] != selected_idx]
            if others:
                lines.append("Other candidates:")
                for o in others:
                    osb = _score_breakdown(o, now_s)
                    diff = round(sb["total"] - osb["total"], 2)
                    sign = "+" if diff > 0 else ""
                    lines.append(
                        f"  #{o['idx']} {o['token']}: score={osb['total']:.1f} ({sign}{diff:+.1f} vs selected)"
                    )

    return "\n".join(lines)


async def reserve_and_diagnose(
    directory: AccountDirectory,
    *,
    pool_candidates: tuple[int, ...],
    spec: ModelSpec,
    job_id: str,
    exclude_tokens: list[str] | None = None,
) -> object:
    """Call ``reserve_any`` and emit detailed diagnostics.

    Parameters
    ----------
    directory:
        The global :class:`AccountDirectory`.
    pool_candidates:
        Pool IDs tried in priority order (from ``spec.pool_candidates()``).
    spec:
        The resolved :class:`ModelSpec` for the current request.
    job_id:
        Video job ID for log correlation.
    exclude_tokens:
        Tokens that have already been tried and failed (excluded from selection).

    Returns
    -------
    AccountLease | None
        Same as ``directory.reserve_any(...)``.
    """
    mode_id = int(spec.mode_id)
    now_s_val = 0
    try:
        from app.platform.runtime.clock import now_s as _now_s

        now_s_val = _now_s()
    except Exception:
        pass

    table = directory._table
    strategy = current_strategy()

    pools_tried: list[int] = list(pool_candidates)
    pool_names = ", ".join(_pool_name(p) for p in pools_tried)

    logger.info(
        "video acct select: job={} ENTER model={} mode={} pools=[{}] strategy={} excluded_tokens={}",
        job_id,
        spec.model_name,
        _mode_name(mode_id),
        pool_names,
        strategy,
        [_mask_token(t) for t in (exclude_tokens or [])],
    )

    if table is None or table.size == 0:
        logger.error(
            "video acct select: job={} EARLY_RETURN table_is_none={} table_size=0={}",
            job_id,
            table is None,
            table.size == 0 if table else True,
        )
        result = await directory.reserve_any(
            pool_candidates=pool_candidates,
            now_s_override=now_s_val,
        )
        logger.info(
            "video acct select: job={} RESERVE_ANY_RESULT (empty_table) result_is_none={}",
            job_id,
            result is None,
        )
        return result

    logger.info(
        "video acct select: job={} table_size={} scanning_pools={}",
        job_id,
        table.size,
        pool_names,
    )

    all_candidates: list[dict[str, Any]] = []
    reject_reasons: dict[int, list[str]] = {}
    pool_candidate_counts: dict[int, int] = {}

    for pool_id in pools_tried:
        cands = _pool_union(table, pool_id)
        pool_candidate_counts[pool_id] = len(cands)

        if not cands:
            reason = f"pool={_pool_name(pool_id)}: no active accounts in mode_available"
            reject_reasons.setdefault(-1, []).append(reason)
            logger.warning(
                "video acct select: job={} pool={} EMPTY no_candidates_in_mode_available",
                job_id,
                _pool_name(pool_id),
            )
            continue

        logger.info(
            "video acct select: job={} pool={} raw_candidates={}",
            job_id,
            _pool_name(pool_id),
            len(cands),
        )

        for idx in cands:
            snap = _snapshot_account(table, idx, mode_id, now_s_val)
            all_candidates.append(snap)
            reasons: list[str] = []

            status = int(table.status_by_idx[idx])
            if status != int(StatusId.ACTIVE):
                reasons.append(f"status={snap['status']}")
                logger.info(
                    "video acct select: job={} pool={} #{} token={} REJECT status={}",
                    job_id,
                    _pool_name(pool_id),
                    idx,
                    snap["token"],
                    snap["status"],
                )

            if strategy == "random":
                max_inflight = 8
                try:
                    from app.platform.config.snapshot import get_config as _gc

                    max_inflight = int(_gc("account.selection.max_inflight", 8))
                except Exception:
                    pass
                cooling_until = int(table.cooling_until_s_by_idx[idx])
                inflight = int(table.inflight_by_idx[idx])
                if cooling_until > now_s_val:
                    reasons.append(
                        f"cooling (remaining {snap['cooling_remaining_s']}s)"
                    )
                    logger.info(
                        "video acct select: job={} pool={} #{} token={} REJECT cooling remaining={}s until_epoch={}",
                        job_id,
                        _pool_name(pool_id),
                        idx,
                        snap["token"],
                        snap["cooling_remaining_s"],
                        snap["cooling_until_s"],
                    )
                if inflight >= max_inflight:
                    reasons.append(f"inflight={inflight}>={max_inflight}")
                    logger.info(
                        "video acct select: job={} pool={} #{} token={} REJECT inflight={}/{} max={}",
                        job_id,
                        _pool_name(pool_id),
                        idx,
                        snap["token"],
                        inflight,
                        max_inflight,
                    )
            else:
                quota = int(table._quota_col(mode_id)[idx])
                if quota <= 0:
                    reasons.append(f"quota={quota}")
                    logger.info(
                        "video acct select: job={} pool={} #{} token={} DIAG_QUOTA_ZERO quota={} "
                        "(note: reserve_any uses _best_no_quota which ignores per-mode quota)",
                        job_id,
                        _pool_name(pool_id),
                        idx,
                        snap["token"],
                        quota,
                    )

            if not reasons:
                logger.info(
                    "video acct select: job={} pool={} #{} token={} PASS all_filters "
                    "status={} health={} inflight={} fail_count={}",
                    job_id,
                    _pool_name(pool_id),
                    idx,
                    snap["token"],
                    snap["status"],
                    snap["health"],
                    snap["inflight"],
                    snap["fail_count"],
                )

            if reasons:
                reject_reasons.setdefault(idx, []).extend(reasons)

    logger.info(
        "video acct select: job={} SCAN_COMPLETE total_candidates={} reject_accounts={} reject_pools={} calling_reserve_any",
        job_id,
        len(all_candidates),
        sum(1 for k in reject_reasons if k != -1),
        len(reject_reasons.get(-1, [])),
    )

    result = await directory.reserve_any(
        pool_candidates=pool_candidates,
        now_s_override=now_s_val,
        exclude_tokens=exclude_tokens,
    )

    if result is not None:
        selected_idx = result.idx
        logger.info(
            "video acct select: job={} SELECTED #{} token={} pool={} mode_quota={} "
            "after_scanning {} candidates from {} pools",
            job_id,
            selected_idx,
            _mask_token(result.token),
            _pool_name(result.pool_id),
            table.quota_for(selected_idx, mode_id),
            len(all_candidates),
            len(pools_tried),
        )
        cand_table = _format_candidates(
            all_candidates, selected_idx, now_s_val, strategy
        )
        for line in cand_table.split("\n"):
            logger.info("video acct select: job={} | {}", job_id, line)

        sel_snap = _snapshot_account(table, selected_idx, mode_id, now_s_val)
        logger.info(
            "video acct select: job={} detail={} ",
            job_id,
            sel_snap,
        )
    else:
        logger.warning(
            "video acct select: job={} NO_CANDIDATE pools=[{}] strategy={} "
            "total_scanned={} rejected_accounts={} rejected_pools={}",
            job_id,
            pool_names,
            strategy,
            len(all_candidates),
            sum(1 for k in reject_reasons if k != -1),
            len(reject_reasons.get(-1, [])),
        )
        if all_candidates:
            cand_table = _format_candidates(all_candidates, None, now_s_val, strategy)
            for line in cand_table.split("\n"):
                logger.warning("video acct select: job={} | {}", job_id, line)
        if reject_reasons:
            for idx_or_pool, reasons in reject_reasons.items():
                if idx_or_pool == -1:
                    for r in reasons:
                        logger.warning(
                            "video acct select: job={} POOL_REJECT: {}",
                            job_id,
                            r,
                        )
                else:
                    tok = ""
                    for a in all_candidates:
                        if a["idx"] == idx_or_pool:
                            tok = a["token"]
                            break
                    logger.warning(
                        "video acct select: job={} REJECT #{} {}: {}",
                        job_id,
                        idx_or_pool,
                        tok,
                        "; ".join(reasons),
                    )

    logger.info(
        "video acct select: job={} EXIT result_is_none={} returning_to_caller",
        job_id,
        result is None,
    )
    return result


__all__ = ["reserve_and_diagnose"]
