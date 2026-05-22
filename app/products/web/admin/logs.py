"""Runtime log viewer — file list, search, and real-time tail (SSE)."""

import asyncio
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from app.platform.paths import log_dir

router = APIRouter(prefix="/logs", tags=["Admin - Logs"])

_LOG_LEVELS = {"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}
_LOG_PATTERN = re.compile(
    r"^(?P<time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s*\|\s*(?P<level>\w+)\s*\|\s*(?P<name>[^:]+):(?P<function>[^:]+):(?P<line>\d+)\s*-\s*(?P<message>.*)$",
)

_MAX_TAIL_LINES = 500
_MAX_SEARCH_LINES = 1000


def _get_log_files() -> list[dict[str, Any]]:
    """Return sorted list of available log files with metadata."""
    d = log_dir()
    if not d.exists():
        return []
    files = []
    for f in sorted(d.glob("app_*.log"), key=lambda x: x.name, reverse=True):
        try:
            st = f.stat()
            files.append({
                "name": f.name,
                "size_bytes": st.st_size,
                "modified_at": st.st_mtime,
            })
        except OSError:
            continue
    return files


def _parse_log_line(line: str) -> dict[str, Any] | None:
    """Parse a single log line into structured data."""
    line = line.strip()
    if not line:
        return None
    m = _LOG_PATTERN.match(line)
    if not m:
        return {
            "raw": line,
            "time": None,
            "level": None,
            "logger": None,
            "message": line,
        }
    g = m.groupdict()
    try:
        dt = datetime.strptime(g["time"], "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        dt = None
    level = g["level"].strip().upper()
    if level not in _LOG_LEVELS:
        level = None
    return {
        "raw": line,
        "time": dt.isoformat() if dt else g["time"],
        "level": level,
        "logger": g["name"].strip(),
        "location": f"{g['function']}:{g['line']}",
        "message": g["message"],
    }


def _read_lines(
    filename: str,
    offset: int = 0,
    limit: int = 100,
) -> tuple[list[dict[str, Any]], int]:
    """Read and parse log lines from file with pagination."""
    d = log_dir()
    filepath = d / filename
    if not filepath.exists() or not filepath.is_file():
        return [], 0

    lines = []
    total_lines = 0
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            for raw_line in f:
                total_lines += 1
                if total_lines <= offset:
                    continue
                if len(lines) >= limit:
                    break
                parsed = _parse_log_line(raw_line)
                if parsed:
                    parsed["_line_number"] = total_lines
                    lines.append(parsed)
    except OSError:
        return [], 0

    return lines, total_lines


def _search_logs(
    filename: str,
    keyword: str = "",
    level: str = "",
    start_time: str = "",
    end_time: str = "",
    offset: int = 0,
    limit: int = 100,
) -> tuple[list[dict[str, Any]], int]:
    """Search logs with filters and pagination."""
    d = log_dir()
    filepath = d / filename
    if not filepath.exists() or not filepath.is_file():
        return [], 0

    keyword_lower = keyword.strip().lower() if keyword else ""
    level_upper = level.strip().upper() if level else ""

    def _time_match(dt_str: str) -> bool:
        if not start_time and not end_time:
            return True
        try:
            dt = datetime.fromisoformat(dt_str)
        except (ValueError, TypeError):
            return True
        if start_time:
            try:
                st = datetime.fromisoformat(start_time)
                if dt < st:
                    return False
            except ValueError:
                pass
        if end_time:
            try:
                et = datetime.fromisoformat(end_time)
                if dt > et:
                    return False
            except ValueError:
                pass
        return True

    matched = []
    total_matched = 0
    skip_count = offset

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            for raw_line in f:
                parsed = _parse_log_line(raw_line)
                if not parsed:
                    continue

                if level_upper and parsed.get("level") != level_upper:
                    continue
                if keyword_lower:
                    msg = (parsed.get("message") or "").lower()
                    logger_name = (parsed.get("logger") or "").lower()
                    if keyword_lower not in msg and keyword_lower not in logger_name:
                        continue
                time_val = parsed.get("time")
                if time_val and not _time_match(time_val):
                    continue

                total_matched += 1
                if skip_count > 0:
                    skip_count -= 1
                    continue
                if len(matched) >= limit:
                    break
                matched.append(parsed)
    except OSError:
        return [], 0

    return matched, total_matched


async def _tail_stream(filename: str, lines: int = 50):
    """Generator for SSE real-time log streaming."""
    import json as _json
    import time as _time

    d = log_dir()
    filepath = d / filename
    if not filepath.exists():
        yield f"data: {_json.dumps({'error': 'Log file not found'})}\n\n"
        return

    initial_lines = []
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            f.seek(0, 2)
            file_size = f.tell()
            if file_size > 0:
                f.seek(max(0, file_size - 100 * 1024))
                all_lines = f.readlines()
            else:
                all_lines = []
            tail_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
            for line in tail_lines:
                parsed = _parse_log_line(line)
                if parsed:
                    initial_lines.append(parsed)
            last_pos = f.tell()
    except OSError as e:
        yield f"data: {_json.dumps({'error': str(e)})}\n\n"
        return

    yield f"data: {_json.dumps({'type': 'init', 'lines': initial_lines})}\n\n"

    heartbeat_counter = 0
    while True:
        await asyncio.sleep(0.5)
        heartbeat_counter += 1

        try:
            current_size = filepath.stat().st_size

            if current_size > last_pos:
                try:
                    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(last_pos)
                        new_content = f.read()
                    last_pos = f.tell()
                except OSError:
                    await asyncio.sleep(0.3)
                    continue

                new_lines = new_content.splitlines()
                parsed_lines = []
                for line in new_lines:
                    parsed = _parse_log_line(line)
                    if parsed:
                        parsed_lines.append(parsed)

                if parsed_lines:
                    yield f"data: {_json.dumps({'type': 'append', 'lines': parsed_lines})}\n\n"
                    heartbeat_counter = 0

            elif heartbeat_counter >= 20:
                yield ": heartbeat\n\n"
                heartbeat_counter = 0

        except OSError:
            yield f"data: {_json.dumps({'type': 'error', 'message': 'File access error'})}\n\n"
            break


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/files")
async def list_log_files():
    """List available log files."""
    files = _get_log_files()
    return {"status": "success", "files": files}


@router.get("/read")
async def read_logs(
    filename: str = Query(..., description="Log file name"),
    offset: int = Query(0, ge=0, description="Line offset"),
    limit: int = Query(100, ge=1, le=1000, description="Lines per page"),
):
    """Read log lines with pagination."""
    lines, total = _read_lines(filename, offset, limit)
    return {
        "status": "success",
        "filename": filename,
        "total": total,
        "offset": offset,
        "limit": limit,
        "lines": lines,
    }


@router.get("/search")
async def search_logs(
    filename: str = Query(..., description="Log file name"),
    keyword: str = Query("", description="Search keyword"),
    level: str = Query("", description="Filter by log level"),
    start_time: str = Query("", description="Start time (ISO format)"),
    end_time: str = Query("", description="End time (ISO format)"),
    offset: int = Query(0, ge=0, description="Result offset"),
    limit: int = Query(100, ge=1, le=_MAX_SEARCH_LINES, description="Results per page"),
):
    """Search logs with filters."""
    lines, total = _search_logs(
        filename, keyword, level, start_time, end_time, offset, limit
    )
    return {
        "status": "success",
        "filename": filename,
        "total": total,
        "offset": offset,
        "limit": limit,
        "keyword": keyword,
        "level": level,
        "lines": lines,
    }


@router.get("/tail")
async def tail_logs(
    filename: str = Query(..., description="Log file name"),
    lines: int = Query(50, ge=10, le=_MAX_TAIL_LINES, description="Initial tail lines"),
):
    """Real-time log streaming via SSE."""
    return StreamingResponse(
        _tail_stream(filename, lines),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
