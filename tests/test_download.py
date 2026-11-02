"""Security and byte-boundary regressions for remote HTTPS imports."""
import hashlib
import socket

import pytest

from flarith.cache import Limits, ProfileCache, ResourceLimitError
from flarith.download import DownloadError, _PinnedHTTPS, _public_addresses, _validated_url, download_https, spark_url


@pytest.mark.parametrize("value", [
    "http://spark.lucko.me/abcdefghij",
    "https://spark.lucko.me:443/abcdefghij",
    "https://spark.lucko.me/abcdefghij?anything=1",
    "https://spark.lucko.me/abcdefghij#fragment",
    "https://spark.lucko.me@evil.org/abcdefghij",
    "https://evil.org/abcdefghij",
    "https://spark.lucko.me/short",
    "https://spark.lucko.me/abcdefghij/extra",
])
def test_spark_sources_restricted(value):
    with pytest.raises(DownloadError):
        spark_url(value)


def test_spark_id_normalizes_to_single_allowed_origin():
    assert spark_url("abcdefghij") == "https://spark-usercontent.lucko.me/abcdefghij"
    assert spark_url("https://spark.lucko.me/abcdefghij") == spark_url("abcdefghij")
    assert spark_url("https://spark-usercontent.lucko.me/abcdefghij") == spark_url("abcdefghij")


def test_native_capability_urls_are_not_fixed_domain_whitelisted():
    host, target = _validated_url(
        "https://oaisdmntprkoreacentral.blob.core.windows.net/files/a?sig=redacted")
    assert host == "oaisdmntprkoreacentral.blob.core.windows.net"
    assert target == "/files/a?sig=redacted"


def test_reject_mixed_dns_addresses(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *_args, **_kwargs: [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443)),
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)),
    ])
    with pytest.raises(DownloadError, match="UNSAFE_RESOLVED_ADDRESS"):
        _public_addresses("untrusted.example")


def test_pinned_connection_preserves_tls_host_and_numeric_target(monkeypatch):
    import flarith.download as module
    seen = {}

    class FakeSocket:
        def settimeout(self, timeout):
            seen["timeout"] = timeout

        def connect(self, endpoint):
            seen["endpoint"] = endpoint

        def close(self):
            seen["closed"] = True

    class FakeContext:
        def wrap_socket(self, sock, server_hostname):
            seen["tls_server_hostname"] = server_hostname
            return sock

    monkeypatch.setattr(module.socket, "socket", lambda *_a, **_kw: FakeSocket())
    monkeypatch.setattr(module.ssl, "create_default_context", lambda: FakeContext())
    conn = _PinnedHTTPS("files.example", [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.4.4", 443)),
    ], 12)
    conn.connect()
    assert seen["endpoint"] == ("8.8.4.4", 443)
    assert seen["tls_server_hostname"] == "files.example"
    conn.close()


class FakeResponse:
    def __init__(self, data=b"abc", status=200, length=None):
        self.data, self.status, self.length = data, status, length
        self.position = 0

    def getheader(self, key):
        return self.length if key == "Content-Length" else None

    def read(self, n):
        block = self.data[self.position:self.position + n]
        self.position += len(block)
        return block


def _patch_connection(monkeypatch, response):
    import flarith.download as module
    connections = []

    class FakeConnection:
        def __init__(self, host, _addresses, _timeout):
            assert host == "files.example"
            self.closed = False
            connections.append(self)

        def request(self, method, target, headers):
            assert method == "GET"
            assert target == "/p?sig=secret"
            assert headers["Accept-Encoding"] == "identity"

        def getresponse(self):
            return response

        def close(self):
            self.closed = True

    monkeypatch.setattr(module, "_public_addresses", lambda _host: [])
    monkeypatch.setattr(module, "_PinnedHTTPS", FakeConnection)
    return connections


def test_hashed_download_and_cleanup(monkeypatch):
    fake = _patch_connection(monkeypatch, FakeResponse(b"abc", length="3"))
    cache = ProfileCache(rss_reader=lambda: 20 * 1024 * 1024)
    result = download_https("https://files.example/p?sig=secret", cache, native_file=True)
    assert result.payload == b"abc"
    assert result.transport_sha256 == hashlib.sha256(b"abc").hexdigest()
    assert fake[0].closed


@pytest.mark.parametrize("response,code", [
    (FakeResponse(status=302), "SOURCE_UNAVAILABLE"),
    (FakeResponse(status=403), "FILE_AUTHORIZATION_EXPIRED"),
    (FakeResponse(status=410), "FILE_AUTHORIZATION_EXPIRED"),
    (FakeResponse(length="invalid"), "SOURCE_UNAVAILABLE"),
    (FakeResponse(b"abc", length="4"), "SOURCE_UNAVAILABLE"),
])
def test_failures_no_bearer_url_disclosure(monkeypatch, response, code):
    connections = _patch_connection(monkeypatch, response)
    cache = ProfileCache(rss_reader=lambda: 20 * 1024 * 1024)
    with pytest.raises(DownloadError) as info:
        download_https("https://files.example/p?sig=secret", cache, native_file=True)
    assert info.value.code == code
    assert "secret" not in str(info.value)
    assert connections[0].closed


def test_oversize_rejection_with_and_without_content_length(monkeypatch):
    cache = ProfileCache(
        Limits(max_input_mib=1, max_profile_memory_mib=20, max_process_rss_mib=100),
        rss_reader=lambda: 20 * 1024 * 1024,
    )
    for response in (FakeResponse(length=str(2 * 1024 * 1024)),
                     FakeResponse(b"x" * (1024 * 1024 + 1))):
        connection = _patch_connection(monkeypatch, response)
        with pytest.raises(ResourceLimitError):
            download_https("https://files.example/p?sig=secret", cache, native_file=False)
        assert connection[0].closed
