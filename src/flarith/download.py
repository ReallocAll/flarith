"""Bounded HTTPS retrieval for ChatGPT file references and Spark IDs.

Signed URLs are bearer capabilities: never log them or include them in errors.
Connect to a validated public DNS answer; authenticate TLS against the
original hostname, not the numeric IP. No redirects or proxy handling.
"""
from __future__ import annotations

import hashlib
import http.client
import ipaddress
import re
import socket
import ssl
import tempfile
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

from flarith.cache import ProfileCache, ResourceLimitError

DOWNLOAD_DEADLINE_SECONDS = 120
SOCKET_TIMEOUT_SECONDS = 12
SPARK_ID = re.compile(r"[A-Za-z0-9]{10}\Z")


class DownloadError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class Downloaded:
    payload: bytes
    transport_sha256: str
    elapsed_ms: float


def _validated_url(url: str) -> tuple[str, str]:
    if (not isinstance(url, str) or not 1 <= len(url) <= 8192
            or any(ord(c) <= 32 or ord(c) == 127 for c in url)):
        raise DownloadError("INVALID_QUERY")
    try:
        parsed = urlsplit(url)
        host = parsed.hostname
        if (parsed.scheme != "https" or not host or parsed.port is not None
                or parsed.username is not None or parsed.password is not None
                or parsed.fragment or not parsed.path.startswith("/")
                or parsed.netloc != host or not host.isascii()):
            raise DownloadError("INVALID_QUERY")
        host.encode("ascii")
    except (ValueError, UnicodeError) as exc:
        raise DownloadError("INVALID_QUERY") from exc
    target = parsed.path + (("?" + parsed.query) if parsed.query else "")
    return host, target


def spark_url(value: str) -> str:
    """Accept only an official Spark Viewer URL or a ten-character ID."""
    if not isinstance(value, str):
        raise DownloadError("INVALID_QUERY")
    if SPARK_ID.fullmatch(value):
        profile_id = value
    else:
        host, target = _validated_url(value)
        if host not in {"spark.lucko.me", "spark-usercontent.lucko.me"}:
            raise DownloadError("INVALID_QUERY")
        if "?" in target or not target.startswith("/") or not SPARK_ID.fullmatch(target[1:]):
            raise DownloadError("INVALID_QUERY")
        profile_id = target[1:]
    return "https://spark-usercontent.lucko.me/" + profile_id


def _public_addresses(hostname: str) -> list[tuple]:
    try:
        addresses = socket.getaddrinfo(hostname, 443, type=socket.SOCK_STREAM)
        if not addresses:
            raise DownloadError("SOURCE_UNAVAILABLE")
        # Reject the entire response if even one DNS answer is non-global.
        if any(not ipaddress.ip_address(info[4][0]).is_global for info in addresses):
            raise DownloadError("UNSAFE_RESOLVED_ADDRESS")
        return addresses
    except (OSError, ValueError, IndexError) as exc:
        raise DownloadError("SOURCE_UNAVAILABLE") from exc


class _PinnedHTTPS(http.client.HTTPConnection):
    """Override the documented connect() hook; no private stdlib overrides."""

    def __init__(self, host: str, addresses: list[tuple], timeout: float):
        super().__init__(host, 443, timeout=timeout)
        self._addresses = addresses
        self._ssl_context = ssl.create_default_context()

    def connect(self) -> None:
        last_error = None
        for family, kind, protocol, _, address in self._addresses:
            raw = socket.socket(family, kind, protocol)
            try:
                raw.settimeout(self.timeout)
                raw.connect(address)
                self.sock = self._ssl_context.wrap_socket(raw, server_hostname=self.host)
                return
            except (OSError, ssl.SSLError) as exc:
                raw.close()
                last_error = exc
        raise OSError("No validated public address reachable") from last_error


def download_https(
    url: str, cache: ProfileCache, *, native_file: bool
) -> Downloaded:
    """Download through bounded disk-backed buffering, then reserve input RSS."""
    host, target = _validated_url(url)
    addresses = _public_addresses(host)
    deadline = time.monotonic() + DOWNLOAD_DEADLINE_SECONDS
    conn = _PinnedHTTPS(host, addresses, SOCKET_TIMEOUT_SECONDS)
    max_bytes = cache.limits.max_input_mib * 1024 * 1024
    count = 0
    digest = hashlib.sha256()
    start = time.perf_counter()
    try:
        conn.request("GET", target, headers={
            "Accept-Encoding": "identity",
            "Connection": "close",
            "User-Agent": "Flarith/0.1",
        })
        response = conn.getresponse()
        if 300 <= response.status < 400:
            raise DownloadError("SOURCE_UNAVAILABLE")
        if response.status in (401, 403, 404, 410) and native_file:
            raise DownloadError("FILE_AUTHORIZATION_EXPIRED")
        if response.status != 200:
            raise DownloadError("SOURCE_UNAVAILABLE")
        length = response.getheader("Content-Length")
        if length is not None and (not length.isascii() or not length.isdecimal()):
            raise DownloadError("SOURCE_UNAVAILABLE")
        if length is not None and int(length) > max_bytes:
            raise ResourceLimitError("download exceeds configured input limit")
        with tempfile.TemporaryFile() as output:
            while True:
                if time.monotonic() >= deadline:
                    raise DownloadError("SOURCE_UNAVAILABLE")
                block = response.read(min(65536, max_bytes + 1 - count))
                if not block:
                    break
                count += len(block)
                if count > max_bytes:
                    raise ResourceLimitError("download exceeds configured input limit")
                output.write(block)
                digest.update(block)
            if length is not None and int(length) != count:
                raise DownloadError("SOURCE_UNAVAILABLE")
            if not count:
                raise DownloadError("SOURCE_UNAVAILABLE")
            cache.reserve_input(count)
            output.seek(0)
            payload = output.read()
        return Downloaded(payload, digest.hexdigest(), (time.perf_counter() - start) * 1000)
    except (OSError, TimeoutError, http.client.HTTPException, ssl.SSLError) as exc:
        raise DownloadError("SOURCE_UNAVAILABLE") from exc
    finally:
        conn.close()
