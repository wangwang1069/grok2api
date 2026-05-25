"""Account-retry executor for video (and other long-running) jobs.

Wraps a single-attempt ``reserve → execute → release/feedback`` cycle into
a loop that automatically retries on **retryable** errors (429, 403) by
excluding the failed token and selecting a different account.

Usage in *video.py*::

    from .video_retry import execute_with_retry

    artifact, token = await execute_with_retry(
        directory=_acct_dir,
        reserve_fn=video_account_diag.reserve_and_diagnose,
        execute_fn=_execute_video,
        job_id=job.id,
        pool_candidates=spec.pool_candidates(),
        spec=spec,
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable, Callable

from app.control.account.enums import FeedbackKind
from app.platform.errors import RateLimitError
from app.platform.logging.logger import logger

if TYPE_CHECKING:
    from app.control.model.spec import ModelSpec
    from app.dataplane.account import AccountDirectory

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_MAX_RETRIES = 3

_RETRYABLE_KINDS: frozenset[FeedbackKind] = frozenset({
    FeedbackKind.RATE_LIMITED,
    FeedbackKind.FORBIDDEN,
})


def _mask_token(token: str) -> str:
    # if len(token) > 12:
        # return f"{token[:12]}..."
    # return f"{token[:6]}..."
    return token


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def execute_with_retry(
    *,
    directory: AccountDirectory,
    reserve_fn: Callable[..., Awaitable[Any]],
    execute_fn: Callable[..., Awaitable[Any]],
    feedback_kind_fn: Callable[[BaseException], FeedbackKind],
    release_fn: Callable[[Any], Awaitable[None]],
    feedback_fn: Callable[[str, FeedbackKind, int], Awaitable[None]],
    fail_sync_fn: Callable[[str, int, BaseException | None], Awaitable[None]],
    quota_sync_fn: Callable[[str, int], Awaitable[None]],
    job_id: str,
    pool_candidates: tuple[int, ...],
    spec: ModelSpec,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    **reserve_kwargs: Any,
) -> tuple[Any, str]:
    """Execute *execute_fn* with automatic account retry on 429 / 403.

    Parameters
    ----------
    directory :
        The global :class:`AccountDirectory`.
    reserve_fn :
        Async callable that selects an account.  Must accept at least
        ``directory``, ``pool_candidates``, ``spec``, ``job_id`` and an
        optional ``exclude_tokens`` keyword argument.  Returns an
        ``AccountLease`` or ``None``.
    execute_fn :
        Async callable that performs the upstream work using the selected
        token.  Must return the result artifact on success.
    feedback_kind_fn :
        Maps an exception to a :class:`FeedbackKind`.
    release_fn, feedback_fn, fail_sync_fn, quota_sync_fn :
        Lifecycle hooks called in order after each attempt.
    job_id :
        Correlation ID for log messages.
    pool_candidates, spec :
        Forwarded to *reserve_fn*.
    max_retries :
        Maximum number of account attempts (default 3).
    **reserve_kwargs :
        Extra keyword arguments forwarded to *reserve_fn*.

    Returns
    -------
    tuple[result, token]
        The return value of *execute_fn* and the token that succeeded.

    Raises
    ------
    RateLimitError
        When all attempts are exhausted or no accounts are available.
    Exception
        Re-raises the last non-retryable exception.
    """
    mode_id = int(spec.mode_id)
    tried_tokens: list[str] = []
    last_exc: BaseException | None = None

    logger.info(
        "retry executor: job={} start max_retries={} mode_id={} pools={}",
        job_id,
        max_retries,
        mode_id,
        list(pool_candidates),
    )

    for attempt in range(max_retries):
        exclude_count = len(tried_tokens)
        excluded_preview = (
            [_mask_token(t) for t in tried_tokens[-3:]]
            if tried_tokens
            else []
        )

        logger.info(
            "retry executor: job={} attempt={}/{} start exclude_count={} excluded={}",
            job_id,
            attempt + 1,
            max_retries,
            exclude_count,
            excluded_preview,
        )

        lease = await _call_reserve(
            reserve_fn,
            directory=directory,
            pool_candidates=pool_candidates,
            spec=spec,
            job_id=job_id,
            exclude_tokens=tried_tokens or None,
            **reserve_kwargs,
        )

        if lease is None:
            logger.warning(
                "retry executor: job={} attempt={}/{} RESERVE_FAILED "
                "no_account_available tried_tokens={} excluded={}",
                job_id,
                attempt + 1,
                max_retries,
                len(tried_tokens),
                [_mask_token(t) for t in tried_tokens],
            )
            raise RateLimitError(
                f"No available accounts (tried {len(tried_tokens)})"
            )

        token = lease.token
        tried_tokens.append(token)

        logger.info(
            "retry executor: job={} attempt={}/{} RESERVED token={} pool={} idx={}",
            job_id,
            attempt + 1,
            max_retries,
            _mask_token(token),
            lease.pool_id,
            lease.idx,
        )

        try:
            result = await execute_fn(token=token)

            await release_fn(lease)
            await feedback_fn(token, FeedbackKind.SUCCESS, mode_id)
            _spawn(quota_sync_fn, token, mode_id)

            logger.info(
                "retry executor: job={} SUCCESS attempt={}/{} token={} "
                "total_tried={} releasing_feedback=SUCCESS",
                job_id,
                attempt + 1,
                max_retries,
                _mask_token(token),
                len(tried_tokens),
            )
            return result, token

        except BaseException as exc:
            last_exc = exc
            kind = feedback_kind_fn(exc)
            exc_type = type(exc).__name__
            exc_msg = str(exc)[:200]

            logger.info(
                "retry executor: job={} attempt={}/{} FAILED token={} "
                "exc_type={} kind={} msg={} will_release_and_feedback",
                job_id,
                attempt + 1,
                max_retries,
                _mask_token(token),
                exc_type,
                kind.name,
                exc_msg,
            )

            await release_fn(lease)
            await feedback_fn(token, kind, mode_id)
            _spawn(fail_sync_fn, token, mode_id, exc)

            is_retryable = kind in _RETRYABLE_KINDS
            has_more = attempt < max_retries - 1

            if is_retryable and has_more:
                logger.info(
                    "retry executor: job={} WILL_RETRY attempt={}/{} token={} "
                    "kind={} retryable={} remaining_attempts={} "
                    "next_exclude_count={}",
                    job_id,
                    attempt + 1,
                    max_retries,
                    _mask_token(token),
                    kind.name,
                    is_retryable,
                    max_retries - attempt - 1,
                    len(tried_tokens),
                )
                continue

            logger.warning(
                "retry executor: job={} EXHAUSTED final_attempt={}/{} "
                "last_token={}... last_kind={} last_exc={} retryable={} "
                "total_tried={}",
                job_id,
                attempt + 1,
                max_retries,
                _mask_token(token),
                kind.name,
                exc_type,
                is_retryable,
                len(tried_tokens),
            )
            raise

    logger.error(
        "retry executor: job={} UNREACHABLE fell_through_loop max_retries={}",
        job_id,
        max_retries,
    )
    raise RateLimitError(
        f"Failed after {max_retries} attempts"
    ) from last_exc


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _call_reserve(fn, **kw):
    return await fn(**kw)


def _spawn(fn, *args):
    import asyncio
    asyncio.create_task(fn(*args))


__all__ = ["execute_with_retry"]
