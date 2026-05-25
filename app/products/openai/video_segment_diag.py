"""Video segment collection diagnostics.

Wraps ``_collect_video_segment`` with structured diagnostic logs so operators
can quickly answer:

* **What stream events were received?** — every SSE data item is logged with
  its event classification (data / done / other) and parse status.
* **Why did the stream fail?** — stream errors from ``raise_for_stream_error``
  are captured with the full upstream response object.
* **Why was no video URL produced?** — when the function raises
  ``UpstreamError("Video generation returned no final video URL")``, all
  accumulated stream data is dumped for post-mortem analysis.
* **What progress was observed?** — progress milestones are logged so operators
  can tell whether generation stalled at a specific percentage.

Usage (minimal change to *video.py*)::

    from .video_segment_diag import collect_video_segment_diag

    artifact = await collect_video_segment_diag(
        token=token,
        payload=payload,
        referer=referer,
        timeout_s=timeout_s,
        progress_cb=progress_cb,
    )
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Awaitable, Callable

import orjson

from app.platform.errors import UpstreamError
from app.platform.logging.logger import logger
from app.dataplane.proxy import get_proxy_runtime
from app.dataplane.proxy.adapters.headers import build_http_headers
from app.dataplane.proxy.adapters.session import ResettableSession, build_session_kwargs
from app.dataplane.reverse.protocol.xai_chat import classify_line, raise_for_stream_error
from app.dataplane.reverse.protocol.xai_assets import (
    resolve_asset_reference,
    resolve_download_url,
)
from app.dataplane.reverse.runtime.endpoint_table import CHAT


_DIAG_MAX_STREAM_DUMP = 60
_DIAG_MAX_RAW_LEN = 500


@dataclass(slots=True)
class _VideoArtifact:
    video_url: str = ""
    video_post_id: str = ""
    asset_id: str = ""
    thumbnail_url: str = ""
    remixed_from_video_id: str | None = None


def _truncate(value: str, max_len: int = _DIAG_MAX_RAW_LEN) -> str:
    if len(value) <= max_len:
        return value
    return value[:max_len] + f"...({len(value)} chars)"


def _safe_json_parse(raw: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        obj = orjson.loads(raw)
        if isinstance(obj, dict):
            return obj, None
        return None, f"parsed to {type(obj).__name__}, expected dict"
    except orjson.JSONDecodeError as e:
        return None, f"JSON decode error at pos {e.pos}: {e.msg}"


def _extract_streaming_video_response(data: dict[str, Any]) -> dict[str, Any] | None:
    result = data.get("result")
    if not isinstance(result, dict):
        return None
    response = result.get("response")
    if not isinstance(response, dict):
        return None
    stream = response.get("streamingVideoGenerationResponse")
    return stream if isinstance(stream, dict) else None


def _extract_model_response_file_attachments(data: dict[str, Any]) -> list[str]:
    result = data.get("result")
    if not isinstance(result, dict):
        return []
    response = result.get("response")
    if not isinstance(response, dict):
        return []
    model_response = response.get("modelResponse")
    if not isinstance(model_response, dict):
        return []
    attachments = model_response.get("fileAttachments")
    if not isinstance(attachments, list):
        return []
    return [item for item in attachments if isinstance(item, str) and item]


def _absolutize_video_url(url: str) -> str:
    full_url, _, _ = resolve_download_url(url)
    return full_url


async def _stream_video_request(
    token: str,
    payload: dict[str, Any],
    *,
    referer: str,
    timeout_s: float,
) -> AsyncGenerator[str, None]:
    proxy = await get_proxy_runtime()
    lease = await proxy.acquire()
    headers = build_http_headers(
        token,
        content_type="application/json",
        origin="https://grok.com",
        referer=referer,
        lease=lease,
    )
    kwargs = build_session_kwargs(lease=lease)

    async with ResettableSession(**kwargs) as session:
        response = await session.post(
            CHAT,
            headers=headers,
            data=orjson.dumps(payload),
            timeout=timeout_s,
            stream=True,
        )
        if response.status_code != 200:
            body = response.content.decode("utf-8", "replace")[:300]
            raise UpstreamError(
                f"Video upstream returned {response.status_code}",
                status=response.status_code,
                body=body,
            )
        async for line in response.aiter_lines():
            yield line


def _extract_stream_summary(obj: dict[str, Any]) -> dict[str, Any]:
    result = obj.get("result")
    if not isinstance(result, dict):
        return {"has_result": False}
    response = result.get("response")
    if not isinstance(response, dict):
        return {"has_result": True, "has_response": False}

    stream = response.get("streamingVideoGenerationResponse")
    if isinstance(stream, dict):
        progress = stream.get("progress")
        video_url = bool(stream.get("videoUrl"))
        asset_id = bool(stream.get("assetId"))
        moderated = stream.get("moderated")
        video_post_id = stream.get("videoPostId") or stream.get("videoId")
        return {
            "has_result": True,
            "has_response": True,
            "has_stream": True,
            "progress": progress,
            "has_video_url": video_url,
            "has_asset_id": asset_id,
            "moderated": moderated,
            "video_post_id": str(video_post_id or "")[:40],
        }

    model_response = response.get("modelResponse")
    if isinstance(model_response, dict):
        attachments = model_response.get("fileAttachments")
        return {
            "has_result": True,
            "has_response": True,
            "has_stream": False,
            "has_model_response": True,
            "file_attachments_count": len(attachments) if isinstance(attachments, list) else 0,
        }

    return {"has_result": True, "has_response": True, "has_stream": False}


async def collect_video_segment_diag(
    *,
    token: str,
    payload: dict[str, Any],
    referer: str,
    timeout_s: float,
    progress_cb: Callable[[int], Awaitable[None]] | None = None,
) -> _VideoArtifact:
    """Drop-in replacement for ``_collect_video_segment`` with diagnostic logging.

    All internal logic is identical to the original function; the only
    difference is structured log output at key decision points.
    """
    _mask = token[:8] + "..." + token[-4:] if len(token) > 12 else token[:4] + "..."

    t0 = time.monotonic()
    logger.info(
        "video segment diag: token={} ENTER referer={} timeout={}s",
        _mask, referer, timeout_s,
    )

    final_url = ""
    final_asset_id = ""
    final_thumbnail = ""
    video_post_id = ""
    stream_data_items: list[str] = []

    line_count = 0
    data_count = 0
    parse_fail_count = 0
    stream_error_count = 0
    progress_milestones: list[int] = []
    last_progress = -1

    async for line in _stream_video_request(
        token,
        payload,
        referer=referer,
        timeout_s=timeout_s,
    ):
        line_count += 1
        event_type, data = classify_line(line)

        if event_type == "done":
            logger.info(
                "video segment diag: token={} line#{} DONE event received after {} data items",
                _mask, line_count, data_count,
            )
            break

        if event_type != "data" or not data:
            if line_count <= 5 or line_count % 200 == 0:
                logger.debug(
                    "video segment diag: token={} line#{} non-data event={} raw={}",
                    _mask, line_count, event_type, _truncate(line),
                )
            continue

        data_count += 1
        stream_data_items.append(data)

        obj, parse_err = _safe_json_parse(data)
        if parse_err is not None:
            parse_fail_count += 1
            if parse_fail_count <= 3:
                logger.warning(
                    "video segment diag: token={} data#{} PARSE_FAIL {} raw={}",
                    _mask, data_count, parse_err, _truncate(data),
                )
            continue

        try:
            raise_for_stream_error(obj)
        except UpstreamError as exc:
            stream_error_count += 1
            logger.error(
                "video segment diag: token={} data#{} STREAM_ERROR error={} summary={}",
                _mask, data_count, exc, _extract_stream_summary(obj),
            )
            raise

        stream = _extract_streaming_video_response(obj)
        if stream:
            try:
                progress = int(stream.get("progress") or 0)
            except (TypeError, ValueError):
                progress = 0

            if progress != last_progress:
                is_milestone = (
                    progress in (0, 10, 25, 50, 75, 90, 100)
                    or progress - last_progress >= 20
                )
                last_progress = progress
                if is_milestone:
                    progress_milestones.append(progress)
                    logger.info(
                        "video segment diag: token={} PROGRESS={}%",
                        _mask, progress,
                    )

            if progress_cb is not None:
                await progress_cb(progress)

            video_post_id = str(
                stream.get("videoPostId")
                or stream.get("videoId")
                or video_post_id
                or ""
            ).strip()

            if progress >= 100 and not stream.get("moderated"):
                raw_url = stream.get("videoUrl")
                asset_id = stream.get("assetId")
                thumbnail = stream.get("thumbnailImageUrl")
                if isinstance(raw_url, str) and raw_url:
                    final_url = _absolutize_video_url(raw_url)
                    logger.info(
                        "video segment diag: token={} GOT_VIDEO_URL len={} asset_id={}",
                        _mask, len(raw_url), bool(asset_id),
                    )
                if isinstance(asset_id, str) and asset_id:
                    final_asset_id = asset_id
                if isinstance(thumbnail, str) and thumbnail:
                    final_thumbnail = _absolutize_video_url(thumbnail)
            elif progress >= 100 and stream.get("moderated"):
                logger.warning(
                    "video segment diag: token={} PROGRESS=100% BUT MODERATED asset_id={}",
                    _mask, stream.get("assetId"),
                )

        attachments = _extract_model_response_file_attachments(obj)
        if attachments and not final_asset_id:
            final_asset_id = attachments[0]
            logger.info(
                "video segment diag: token={} GOT_ASSET_ID_FROM_ATTACHMENT asset_id={}",
                _mask, final_asset_id,
            )

    elapsed = time.monotonic() - t0

    logger.info(
        "video segment diag: token={} STREAM_COMPLETE lines={} data_items={} "
        "parse_fails={} stream_errors={} progress_milestones={} elapsed={:.1f}s "
        "final_url={} final_asset_id={} video_post_id={}",
        _mask, line_count, data_count, parse_fail_count, stream_error_count,
        progress_milestones, elapsed,
        bool(final_url), bool(final_asset_id), bool(video_post_id),
    )

    if not final_url and final_asset_id:
        logger.info(
            "video segment diag: token={} RESOLVING_ASSET asset_id={}",
            _mask, final_asset_id,
        )
        final_url = resolve_asset_reference(final_asset_id, "", user_id=None) or ""
        if not final_url:
            logger.error(
                "video segment diag: token={} ASSET_RESOLVE_FAILED asset_id={}",
                _mask, final_asset_id,
            )

    if not final_url and final_asset_id:
        dump = stream_data_items[-_DIAG_MAX_STREAM_DUMP:]
        logger.error(
            "video segment diag: token={} FAIL asset_id_without_url asset_id={} "
            "data_items={} last_{}_items={}",
            _mask, final_asset_id, len(stream_data_items), len(dump),
            [_truncate(d, 200) for d in dump],
        )
        raise UpstreamError(
            "Video segment returned only assetId without a resolvable URL",
            body="\n".join(stream_data_items),
        )

    if not final_url:
        dump = stream_data_items[-_DIAG_MAX_STREAM_DUMP:]
        logger.error(
            "video segment diag: token={} FAIL no_final_url "
            "data_items={} parse_fails={} progress_milestones={} "
            "video_post_id={} final_asset_id={} last_{}_items={}",
            _mask, len(stream_data_items), parse_fail_count,
            progress_milestones, video_post_id, final_asset_id,
            len(dump),
            [_truncate(d, 200) for d in dump],
        )
        raise UpstreamError(
            "Video generation returned no final video URL",
            body="\n".join(stream_data_items),
        )

    logger.info(
        "video segment diag: token={} SUCCESS url={} asset_id={} thumbnail={} elapsed={:.1f}s",
        _mask, _truncate(final_url, 80), final_asset_id, bool(final_thumbnail), elapsed,
    )

    return _VideoArtifact(
        video_url=final_url,
        video_post_id=video_post_id or final_asset_id,
        asset_id=final_asset_id,
        thumbnail_url=final_thumbnail,
    )


__all__ = ["collect_video_segment_diag"]
