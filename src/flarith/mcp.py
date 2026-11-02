"""Minimal Agent-facing MCP entrypoint; never retain signed source URLs."""
from __future__ import annotations

import re
import threading
import time

from fastmcp import FastMCP
from pydantic import BaseModel, ConfigDict, Field

from flarith.cache import (
    ProfileCache, ProfileInUseError, ProfileNotFoundError, ResourceLimitError,
)
from flarith.core import InvalidProfileError
from flarith.download import DownloadError, download_https, spark_url

mcp = FastMCP("Flarith")
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
