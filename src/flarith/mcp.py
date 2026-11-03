"""Minimal Agent-facing MCP entrypoint; never retain signed source URLs."""
from __future__ import annotations

from importlib.metadata import version

import json
import re
import threading
import time

from fastmcp import FastMCP
from fastmcp.tools import ToolResult
from pydantic import BaseModel, ConfigDict, Field

from flarith.cache import (
    ProfileCache, ProfileInUseError, ProfileNotFoundError, ResourceLimitError,
)
from flarith.core import InvalidProfileError
from flarith.download import DownloadError, download_https, spark_url

mcp = FastMCP("Flarith", version=version("flarith"))
cache = ProfileCache()
_import_lock = threading.Lock()
SHA256 = re.compile(r"[a-f0-9]{64}\Z")


class OpenAIFile(BaseModel):
    model_config = ConfigDict(extra="forbid")
    download_url: str
    file_id: str = Field(min_length=1)
    mime_type: str | None = None
    file_name: str | None = None


def _error(code: str) -> dict:
    result = {"ok": False, "error": {"code": code, "message": code.replace("_", " ").lower()}}
    if code == "PROFILE_NOT_FOUND":
        result["error"]["message"] = "The requested profile is not available."
        result["error"]["recovery_methods"] = ["import_chatgpt_file", "open_profile"]
    return result


def _load_url(url: str, *, native: bool, expected_sha256: str | None = None) -> dict:
    if expected_sha256 is not None and not SHA256.fullmatch(expected_sha256):
        return _error("INVALID_QUERY")
    if not _import_lock.acquire(blocking=False):
        return _error("RESOURCE_LIMIT")
    try:
        result = download_https(url, cache, native_file=native)
        if expected_sha256 is not None and result.transport_sha256 != expected_sha256:
            return _error("CONTENT_MISMATCH")
        before = set(cache.references())
        started = time.perf_counter()
        reference = cache.load(result.payload)
        with cache.borrow(reference) as loaded:
            profile = loaded.profile
            response = {
                "ok": True,
                "profile_ref": reference,
                "transport_sha256": result.transport_sha256,
                "transport_bytes": len(result.payload),
                "protobuf_bytes": len(loaded.raw),
                "compressed": result.payload.startswith(b"\x1f\x8b"),
                "threads": len(profile.threads),
                "nodes": profile.node_count,
                "time_windows": len(profile.message.time_windows),
                "cache_hit": reference in before,
                "download_ms": round(result.elapsed_ms, 2),
                "load_ms": round((time.perf_counter() - started) * 1000, 2),
            }
        return response
    except DownloadError as exc:
        return _error(exc.code)
    except ResourceLimitError:
        return _error("RESOURCE_LIMIT")
    except (InvalidProfileError, ValueError):
        return _error("PROFILE_UNSUPPORTED")
    except (MemoryError, RecursionError):
        return _error("RESOURCE_LIMIT")
    except Exception:
        # An exception may embed the temporary signed URL: do not expose it.
        return _error("SOURCE_UNAVAILABLE")
    finally:
        _import_lock.release()


@mcp.tool(meta={"openai/fileParams": ["file"]})
def import_chatgpt_file(file: OpenAIFile, expected_sha256: str | None = None) -> dict:
    """Import a ChatGPT-authorized Spark Profile attachment."""
    if not file.file_id:
        return _error("INVALID_QUERY")
    return _load_url(file.download_url, native=True, expected_sha256=expected_sha256)


@mcp.tool()
def open_profile(profile: str) -> dict:
    """Load a Spark Profile by ID or official spark.lucko.me URL."""
    try:
        url = spark_url(profile)
    except DownloadError as exc:
        return _error(exc.code)
    return _load_url(url, native=False)


@mcp.tool()
def describe_profile(profile_ref: str) -> dict:
    """Read the directory of a currently cached Spark Profile."""
    try:
        with cache.borrow(profile_ref) as entry:
            obj = entry.profile
            meta = obj.message.metadata
            return {
                "ok": True,
                "profile_ref": profile_ref,
                "type": "SamplerData",
                "threads": [
                    {"index": i, "name": graph.thread.name,
                     "nodes": len(graph.thread.children), "roots": len(graph.roots)}
                    for i, graph in enumerate(obj.threads)
                ],
                "node_count": obj.node_count,
                "time_windows": list(obj.message.time_windows),
                "start_time": str(meta.start_time),
                "end_time": str(meta.end_time),
                "sampler_mode": meta.SamplerMode.Name(meta.sampler_mode),
                "sampler_engine": meta.SamplerEngine.Name(meta.sampler_engine),
            }
    except ProfileNotFoundError:
        return _error("PROFILE_NOT_FOUND")


@mcp.tool()
def release_profile(profile_ref: str) -> dict:
    """Release an inactive cached Profile; LRU also reclaims automatically."""
    try:
        if not cache.release(profile_ref):
            return _error("PROFILE_NOT_FOUND")
        return {"ok": True, "released": True}
    except ProfileInUseError:
        return _error("RESOURCE_LIMIT")

# Query tools operate on leased cache entries so eviction cannot invalidate
# in-flight reads. No tree cloning or unbounded JSON serialization.
from flarith import query as _query


def _read_query(profile_ref: str, operation, *args, raw: bool = False, **kwargs) -> dict:
    try:
        with cache.borrow(profile_ref) as entry:
            if raw:
                result = operation(entry.raw, *args, **kwargs)
            else:
                result = operation(entry.profile, *args, **kwargs)
            return {"ok": True, **result}
    except ProfileNotFoundError:
        return _error("PROFILE_NOT_FOUND")
    except _query.QueryError as exc:
        return _error(exc.code)
    except (IndexError, TypeError, ValueError):
        return _error("INVALID_QUERY")
    except MemoryError:
        return _error("RESOURCE_LIMIT")


@mcp.tool()
def describe_schema(message_type: str = "spark.SamplerData") -> dict:
    """Describe available Spark Protobuf fields and message types."""
    try:
        return {"ok": True, **_query.describe_schema(message_type)}
    except _query.QueryError as exc:
        return _error(exc.code)


@mcp.tool()
def read_fields(profile_ref: str, path: list[str | int], offset: int = 0,
                limit: int = 50, fields: list[str] | None = None) -> dict:
    """Read a Protobuf path, optionally projecting shallow scalar/map fields."""
    return _read_query(profile_ref, _query.read_fields, path, offset, limit, fields)


@mcp.tool()
def query_entity_distribution(profile_ref: str, entity_type: str,
                              world_index: int | None = None,
                              offset: int = 0, limit: int = 10) -> dict:
    """Rank per-chunk entity counts from the recorded world statistics snapshot."""
    return _read_query(profile_ref, _query.query_entity_distribution,
                       entity_type, world_index, offset, limit)


# Match Freedo's single-content ToolResult and conservative MCP envelope budget.
MAX_MCP_PAGE_BYTES = 4 * 1024 * 1024
MCP_FRAMING_ALLOWANCE = 8192


def _compact_json(value: dict) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _framed_size(payload: dict) -> int:
    """Count a serialized JSON-RPC tool result, allowing for a 64-byte request ID."""
    text = _compact_json(payload)
    envelope = {
        "jsonrpc": "2.0", "id": "0" * 64,
        "result": {"content": [{"type": "text", "text": text}], "isError": False},
    }
    return len(_compact_json(envelope).encode("utf-8"))


def _budgeted_read_raw(raw: bytes, profile_ref: str, offset: int, limit: int) -> dict:
    """Preserve the requested limit while fitting the actual page in one MCP text item."""
    first = _query.read_raw(raw, offset, limit)
    requested = min(limit, max(0, len(raw) - offset))
    budget = MAX_MCP_PAGE_BYTES - MCP_FRAMING_ALLOWANCE

    def candidate(page: dict, count: int) -> dict:
        adjusted = count < requested
        result = {
            **page,
            "requested_bytes": limit,
            "returned_bytes": count,
            "size_limit_adjusted": adjusted,
            "mcp_response_bytes": 0,
            "next_request": (
                {"profile_ref": profile_ref, "offset": page["next_offset"], "limit": limit}
                if page["has_more"] else None
            ),
        }
        if adjusted:
            result["size_limit_reason"] = "mcp_response_budget"
        # Account for the decimal width of the size field itself.
        for _ in range(4):
            size = _framed_size({"ok": True, **result})
            if result["mcp_response_bytes"] == size:
                break
            result["mcp_response_bytes"] = size
        return result

    result = candidate(first, requested)
    size = _framed_size({"ok": True, **result})
    while size >= budget:
        count = result["returned_bytes"]
        if count <= 1:
            raise _query.QueryError("RESOURCE_LIMIT")
        # Base64 grows by 4 bytes per 3 source bytes; avoid repeated binary searches.
        reduction = max(3, 3 * ((size - budget + 4) // 4))
        count = max(1, count - reduction)
        result = candidate(_query.read_raw(raw, offset, count), min(count, requested))
        size = _framed_size({"ok": True, **result})
    return result


@mcp.tool()
def read_raw(profile_ref: str, offset: int = 0,
             limit: int = 2 * 1024 * 1024) -> ToolResult:
    """Read raw Protobuf as one Base64 text result under 4 MiB; follow next_request to continue."""
    result = _read_query(profile_ref, _budgeted_read_raw, profile_ref, offset, limit, raw=True)
    return ToolResult(content=_compact_json(result))


@mcp.tool()
def search_symbols(profile_ref: str, text: str, mode: str = "substring",
                   thread_index: int | None = None, offset: int = 0,
                   limit: int = 50) -> dict:
    """Find frames by full symbol, prefix or substring; return stable node indices."""
    return _read_query(profile_ref, _query.search_symbols,
                       text, mode, thread_index, offset, limit)


@mcp.tool()
def get_call_tree(profile_ref: str, thread_index: int,
                  node_index: int | None = None, offset: int = 0,
                  limit: int = 50, window_index: int | None = None,
                  order_by: str = "original") -> dict:
    """Read direct children in original or descending Self/Inclusive order."""
    return _read_query(profile_ref, _query.get_call_tree,
                       thread_index, node_index, offset, limit, window_index, order_by)


@mcp.tool()
def query_frames(profile_ref: str, thread_index: int,
                 sort_by: str = "self", window_index: int | None = None,
                 text: str | None = None, offset: int = 0,
                 limit: int = 50, group_by: str = "node",
                 start_window: int | None = None,
                 end_window: int | None = None) -> dict:
    """Rank nodes, symbols or classes, optionally over a window range."""
    return _read_query(profile_ref, _query.query_frames,
                       thread_index, sort_by, window_index, text, offset, limit,
                       group_by, start_window, end_window)


@mcp.tool()
def query_subtree_hotspots(profile_ref: str, thread_index: int, node_index: int,
                           group_by: str = "symbol", window_index: int | None = None,
                           offset: int = 0, limit: int = 50,
                           start_window: int | None = None,
                           end_window: int | None = None) -> dict:
    """Rank Self hotspots inside one call subtree without summing Inclusive values."""
    return _read_query(profile_ref, _query.query_subtree_hotspots,
                       thread_index, node_index, group_by, window_index, offset, limit,
                       start_window, end_window)


@mcp.tool()
def compare_time_ranges(profile_ref: str, thread_index: int,
                        baseline_start: int, baseline_end: int,
                        target_start: int, target_end: int,
                        offset: int = 0, limit: int = 50) -> dict:
    """Compare complete per-symbol Self shares between two time ranges."""
    return _read_query(profile_ref, _query.compare_time_ranges,
                       thread_index, baseline_start, baseline_end,
                       target_start, target_end, offset, limit)


@mcp.tool()
def query_metrics(profile_ref: str, series: str = "windows",
                  offset: int = 0, limit: int = 50) -> dict:
    """Read actual window statistics or sampled metric series without interpolation."""
    return _read_query(profile_ref, _query.query_metrics, series, offset, limit)


@mcp.tool()
def compare_profiles(left_profile_ref: str, right_profile_ref: str,
                     thread_name: str, offset: int = 0, limit: int = 50) -> dict:
    """Compare complete per-symbol Self fractions for the same named thread."""
    try:
        with cache.borrow(left_profile_ref) as left:
            with cache.borrow(right_profile_ref) as right:
                return {"ok": True, **_query.compare_profiles(
                    left.profile, right.profile, thread_name, offset, limit)}
    except ProfileNotFoundError:
        return _error("PROFILE_NOT_FOUND")
    except _query.QueryError as exc:
        return _error(exc.code)
    except (TypeError, ValueError):
        return _error("INVALID_QUERY")
    except MemoryError:
        return _error("RESOURCE_LIMIT")


@mcp.tool()
def compare_results(left: dict, right: dict,
                    offset: int = 0, limit: int = 50) -> dict:
    """Compare supplied symbol pages without treating omitted Top-K rows as zeros."""
    try:
        return {"ok": True, **_query.compare_results(left, right, offset, limit)}
    except _query.QueryError as exc:
        return _error(exc.code)
    except (TypeError, ValueError):
        return _error("INVALID_QUERY")
    except MemoryError:
        return _error("RESOURCE_LIMIT")
