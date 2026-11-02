"""In-process MCP contract: attachment binding, cached data and recovery."""
import hashlib
import importlib

import pytest
from fastmcp import Client

from flarith.cache import ProfileCache
from flarith.download import Downloaded
from flarith.proto.spark import spark_sampler_pb2

server = importlib.import_module("flarith.mcp")
FILE = {
    "download_url": "https://public.example/files/a?sig=secret",
    "file_id": "sediment://file_example",
}


def sample() -> bytes:
    obj = spark_sampler_pb2.SamplerData()
    obj.metadata.start_time = 100
    obj.metadata.end_time = 200
    obj.threads.add().name = "main"
    return obj.SerializeToString()


@pytest.fixture(autouse=True)
def fresh_cache(monkeypatch):
    monkeypatch.setattr(server, "cache", ProfileCache(rss_reader=lambda: 25 * 1048576))


async def test_fileparams_metadata_and_mcp_roundtrip(monkeypatch):
    raw = sample()
    digest = hashlib.sha256(raw).hexdigest()
    calls = []

    def fake_download(url, _cache, *, native_file):
        calls.append((url, native_file))
        return Downloaded(raw, digest, 1.0)

    monkeypatch.setattr(server, "download_https", fake_download)
    async with Client(server.mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}
        assert {"import_chatgpt_file", "open_profile", "describe_profile",
                "release_profile", "describe_schema", "read_fields", "read_raw",
                "search_symbols", "get_call_tree", "query_frames",
                "query_metrics"} <= set(tools)
        raw_tool = tools["import_chatgpt_file"].model_dump(by_alias=True)
        meta = raw_tool.get("_meta") or raw_tool.get("meta")
        assert meta["openai/fileParams"] == ["file"]
        schema = raw_tool["inputSchema"]
        assert "file" in schema["required"]
        props = schema["properties"]["file"]
        if "$ref" in props:
            props = schema["$defs"][props["$ref"].rsplit("/", 1)[1]]
        assert {"download_url", "file_id"} <= set(props["required"])

        loaded = await client.call_tool("import_chatgpt_file", {
            "file": FILE, "expected_sha256": digest,
        })
        assert not loaded.is_error
        assert loaded.data["ok"] and loaded.data["transport_sha256"] == digest
        reference = loaded.data["profile_ref"]
        assert reference == "sha256:" + digest

        details = await client.call_tool("describe_profile", {"profile_ref": reference})
        assert details.data["ok"]
        assert details.data["threads"][0]["name"] == "main"
        removed = await client.call_tool("release_profile", {"profile_ref": reference})
        assert removed.data["ok"]
        absent = await client.call_tool("describe_profile", {"profile_ref": reference})
        assert absent.data["error"]["code"] == "PROFILE_NOT_FOUND"
        assert absent.data["error"]["recovery_methods"] == [
            "import_chatgpt_file", "open_profile"
        ]
        recovered = await client.call_tool("import_chatgpt_file", {"file": FILE})
        assert recovered.data["profile_ref"] == reference
        assert calls == [(FILE["download_url"], True)] * 2


async def test_spark_url_native_sha_mismatch_and_no_leaks(monkeypatch):
    raw = sample()
    digest = hashlib.sha256(raw).hexdigest()

    def fake_download(url, _cache, *, native_file):
        assert url == "https://spark-usercontent.lucko.me/abcdefghij" or native_file
        return Downloaded(raw, digest, 2.0)

    monkeypatch.setattr(server, "download_https", fake_download)
    async with Client(server.mcp) as client:
        mismatch = await client.call_tool("import_chatgpt_file", {
            "file": FILE, "expected_sha256": "0" * 64,
        })
        assert mismatch.data["error"]["code"] == "CONTENT_MISMATCH"
        opened = await client.call_tool("open_profile", {"profile": "abcdefghij"})
        assert opened.data["ok"]
        invalid = await client.call_tool("open_profile", {
            "profile": "https://127.0.0.1:443/abcdefghij"
        })
        assert invalid.data["error"]["code"] == "INVALID_QUERY"
        assert "secret" not in str(invalid.data)


async def test_real_streamable_http_entrypoint():
    """Run the shipped server entrypoint over HTTP, not an in-process client."""
    import asyncio
    import os
    from pathlib import Path
    import socket
    import subprocess
    import sys
    import tempfile

    root = Path(__file__).resolve().parents[1]
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    with tempfile.TemporaryFile() as output:
        process = subprocess.Popen(
            [sys.executable, "mcp_server.py"],
            cwd=root,
            env={**os.environ, "PORT": str(port)},
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        try:
            ready = False
            for _ in range(80):
                if process.poll() is not None:
                    break
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        ready = True
                        break
                except OSError:
                    await asyncio.sleep(0.1)
            if not ready:
                output.seek(0)
                pytest.fail("HTTP server did not start: " + output.read(2048).decode(errors="replace"))

            async with Client(f"http://127.0.0.1:{port}/mcp") as client:
                definitions = {t.name: t for t in await client.list_tools()}
                assert len(definitions) == 14
                meta = definitions["import_chatgpt_file"].model_dump(by_alias=True)
                assert (meta.get("_meta") or meta.get("meta"))["openai/fileParams"] == ["file"]
                assert "query_entity_distribution" in definitions
                assert "fields" in definitions["read_fields"].input_schema["properties"]
                result = await client.call_tool(
                    "open_profile", {"profile": "http://127.0.0.1/private"}
                )
                assert not result.is_error
                assert result.data["error"]["code"] == "INVALID_QUERY"
                missing = await client.call_tool(
                    "describe_profile", {"profile_ref": "sha256:" + "0" * 64}
                )
                assert missing.data["error"]["code"] == "PROFILE_NOT_FOUND"
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
