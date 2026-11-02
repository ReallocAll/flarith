"""R3 contract checks: raw descriptor access, node-pool identity and statistics."""
import base64
import hashlib
import importlib

import pytest
from fastmcp import Client

from flarith import Profile
from flarith.cache import ProfileCache
from flarith.proto.spark import spark_sampler_pb2
from flarith.query import (
    QueryError, describe_schema, get_call_tree, query_frames, query_metrics,
    read_fields, read_raw, search_symbols,
)

server = importlib.import_module("flarith.mcp")


def sample():
    obj = spark_sampler_pb2.SamplerData()
    obj.metadata.start_time = 2**60 + 17
    obj.metadata.end_time = 2**60 + 100
    obj.metadata.interval = 4000
    obj.metadata.number_of_ticks = 9
    obj.time_windows.extend([7, 8])
    obj.time_window_statistics[7].tps = 18.0
    obj.time_window_statistics[7].start_time = 2**60
    obj.class_sources["world"] = "Plugin"
    thread = obj.threads.add()
    thread.name = "main"
    thread.times.extend([10, 20])
    thread.children_refs.append(0)
    root = thread.children.add()
    root.class_name = "Level"
    root.method_name = "tick"
    root.times.extend([10, 20])
    root.children_refs.extend([1, 2])
    leaf = thread.children.add()
    leaf.class_name = "Entity"
    leaf.method_name = "tick"
    leaf.times.extend([7, 13])
    leaf2 = thread.children.add()
    leaf2.class_name = "Goal"
    leaf2.method_name = "run"
    leaf2.times.extend([1, 2])
    tps = obj.metadata.metrics.tps
    tps.start_timestamp_ms = 2**60
    tps.timestamp_deltas_ms.extend([0, 1000])
    tps.values.extend([18.0, 19.0])
    tick_duration = obj.metadata.metrics.tick_duration
    tick_duration.start_timestamp_ms = 2**60
    tick_duration.timestamp_deltas_ms.append(0)
    tick_duration.values.add().median = 62.5
    return obj


def test_full_schema_and_presence_contract():
    profile = Profile.from_bytes(sample().SerializeToString())
    schema = describe_schema()
    assert any(f["name"] == "threads" and f["kind"] == "repeated" for f in schema["fields"])
    assert describe_schema("spark.WindowStatistics")["message_type"] == "spark.WindowStatistics"
    assert read_fields(profile, ["metadata", "start_time"])["value"] == str(2**60 + 17)
    assert read_fields(profile, ["time_window_statistics", 7, "start_time"])["value"] == str(2**60)
    assert read_fields(profile, ["threads", 0, "children", 0, "method_name"])["value"] == "tick"
    assert read_fields(profile, ["class_sources"])["items"] == [{"key": "world", "value": "Plugin"}]
    with pytest.raises(QueryError, match="FIELD_NOT_FOUND"):
        read_fields(profile, ["time_window_statistics", 9])
    with pytest.raises(QueryError, match="FIELD_NOT_FOUND"):
        read_fields(profile, ["metadata", "creator"])  # absent message
    assert len(profile.message.time_window_statistics) == 1


def test_call_tree_explicit_node_pool_and_viewer_weights():
    profile = Profile.from_bytes(sample().SerializeToString())
    roots = get_call_tree(profile, 0)
    assert [node["node_index"] for node in roots["children"]] == [0]
    root = get_call_tree(profile, 0, 0, limit=1)
    assert root["node"]["inclusive"] == 30
    assert root["node"]["self"] == 7
    assert root["total_children"] == 2 and root["has_more"]
    next_page = get_call_tree(profile, 0, 0, offset=1)
    assert [node["node_index"] for node in next_page["children"]] == [2]
    leaf = get_call_tree(profile, 0, 1, window_index=1)
    assert leaf["ancestors"] == [0]
    assert leaf["node"]["self"] == 13.0
    top = query_frames(profile, 0, sort_by="self", window_index=0)
    assert [i["method_name"] for i in top["items"]] == ["tick", "tick", "run"]
    assert top["items"][0]["self"] == 7.0
    assert top["items"][0]["thread_fraction"] == 0.7
    by_total = query_frames(profile, 0, sort_by="inclusive")
    assert by_total["items"][0]["node_index"] == 0
    assert "nested" in by_total["note"]


def test_symbols_and_metrics_pagination():
    profile = Profile.from_bytes(sample().SerializeToString())
    symbols = search_symbols(profile, "tick", limit=1)
    assert symbols["total_matches"] == 2 and symbols["has_more"]
    other = search_symbols(profile, "tick", offset=symbols["next_offset"])
    assert other["items"][0]["node_index"] == 1
    assert search_symbols(profile, "Entity.tick", mode="exact")["items"][0]["node_index"] == 1
    stats = query_metrics(profile, "windows", limit=1)
    assert stats["items"][0]["tps"] == 18.0 and stats["has_more"]
    assert query_metrics(profile, "windows", offset=1)["items"][0]["available"] is False
    series = query_metrics(profile, "tps")
    assert series["start_timestamp_ms"] == str(2**60)
    assert [x["value"] for x in series["items"]] == [18.0, 19.0]
    detail = query_metrics(profile, "tick_duration")
    assert detail["items"][0]["value"]["median"] == 62.5


def test_raw_pagination_supports_offsets_beyond_10k():
    data = b"x" * 20000
    result = read_raw(data, offset=12000, limit=8000)
    assert base64.b64decode(result["data_base64"]) == b"x" * 8000
    assert not result["has_more"]


def test_shared_node_does_not_invent_a_unique_ancestor():
    obj = sample()
    obj.threads[0].children[2].children_refs.append(1)
    profile = Profile.from_bytes(obj.SerializeToString())
    shared = get_call_tree(profile, 0, 1)
    assert shared["ambiguous_path"]
    assert shared["ancestors"] == []


@pytest.mark.parametrize("invalid", [
    lambda p: get_call_tree(p, 0, 3),
    lambda p: read_fields(p, ["threads", -1]),
    lambda p: search_symbols(p, "x", limit=1000),
    lambda p: query_metrics(p, "invalid"),
    lambda p: describe_schema("google.protobuf.FileOptions"),
])
def test_invalid_queries_have_codes(invalid):
    with pytest.raises(QueryError):
        invalid(Profile.from_bytes(sample().SerializeToString()))


async def test_mcp_query_roundtrip(monkeypatch):
    monkeypatch.setattr(server, "cache", ProfileCache(rss_reader=lambda: 30 * 1048576))
    payload = sample().SerializeToString()
    ref = server.cache.load(payload)
    async with Client(server.mcp) as client:
        schema = await client.call_tool("describe_schema", {"message_type": "spark.SamplerData"})
        assert not schema.is_error and schema.data["ok"]
        field = await client.call_tool("read_fields", {"profile_ref": ref, "path": ["metadata", "start_time"]})
        assert field.data["value"] == str(2**60 + 17)
        root = await client.call_tool("get_call_tree", {"profile_ref": ref, "thread_index": 0, "node_index": 0})
        assert root.data["node"]["self"] == 7.0
        found = await client.call_tool("search_symbols", {"profile_ref": ref, "text": "Entity.tick"})
        assert found.data["items"][0]["node_index"] == 1
        metrics = await client.call_tool("query_metrics", {"profile_ref": ref, "series": "tps"})
        assert metrics.data["items"][0]["value"] == 18.0
        raw = await client.call_tool("read_raw", {"profile_ref": ref, "offset": 0, "limit": 256})
        assert base64.b64decode(raw.data["data_base64"]) == payload[:256]
        assert server.cache.release(ref)
        lost = await client.call_tool("get_call_tree", {"profile_ref": ref, "thread_index": 0})
        assert lost.data["error"]["code"] == "PROFILE_NOT_FOUND"


def modified_profile():
    """Same sample mode and thread, different Self distribution."""
    obj = sample()
    thread = obj.threads[0]
    thread.times[0], thread.times[1] = 20.0, 40.0
    root, leaf, goal = thread.children
    root.times[0], root.times[1] = 20.0, 40.0
    leaf.times[0], leaf.times[1] = 8.0, 16.0
    goal.times[0], goal.times[1] = 2.0, 4.0
    return Profile.from_bytes(obj.SerializeToString())


def test_recursive_symbol_aggregation_is_self_only():
    from flarith.query import query_frames
    obj = sample()
    leaf = obj.threads[0].children[1]
    leaf.class_name = "Level"
    leaf.method_name = "tick"
    profile = Profile.from_bytes(obj.SerializeToString())
    result = query_frames(profile, 0, group_by="symbol", limit=1)
    assert result["group_by"] == "symbol"
    assert result["total_groups"] == 2 and not result["complete"]
    assert result["items"][0]["self"] == 27.0  # 7 + 20, NOT 30 + 20
    assert result["items"][0]["occurrences"] == 2
    full = query_frames(profile, 0, group_by="symbol", limit=50)
    assert full["complete"] and not full["has_more"]
    assert sum(row["self"] for row in full["items"]) == 30.0
    with pytest.raises(QueryError, match="INVALID_QUERY"):
        query_frames(profile, 0, group_by="symbol", sort_by="inclusive")


def test_full_profile_comparison_is_normalized_and_deterministic():
    from flarith.query import compare_profiles
    left = Profile.from_bytes(sample().SerializeToString())
    right = modified_profile()
    result = compare_profiles(left, right, "main")
    assert result["complete"] and result["comparison_basis"] == "self_fraction_of_thread"
    assert result["left_thread_total"] == 30
    assert result["right_thread_total"] == 60
    assert result["sampling_parameters_match"]
    by_symbol = {(row["class_name"], row["method_name"]): row for row in result["items"]}
    level = by_symbol["Level", "tick"]
    assert level["left_self"] == 7
    assert level["right_self"] == 30
    assert level["delta_fraction"] == pytest.approx(30 / 60 - 7 / 30)
    assert by_symbol["Goal", "run"]["delta_fraction"] == pytest.approx(0.0)
    assert result["items"][0]["delta_fraction"] is not None
    assert compare_profiles(left, right, "main", limit=1)["has_more"]


def test_partial_topk_comparison_preserves_unknown_missing_values():
    from flarith.query import compare_profiles, compare_results, query_frames
    left = Profile.from_bytes(sample().SerializeToString())
    right = modified_profile()
    a = query_frames(left, 0, group_by="symbol", limit=1)
    b = query_frames(right, 0, group_by="symbol", limit=1)
    assert not a["complete"] and not b["complete"]
    partial = compare_results(a, b)
    assert not partial["complete"]
    assert partial["total_symbols"] >= 2
    assert all(row["delta_fraction"] is None for row in partial["items"])
    assert any(row["left_self"] is None or row["right_self"] is None
               for row in partial["items"])

    complete_a = query_frames(left, 0, group_by="symbol")
    complete_b = query_frames(right, 0, group_by="symbol")
    full = compare_results(complete_a, complete_b)
    direct = compare_profiles(left, right, "main")
    assert full["complete"]
    a_by_key = {(r["class_name"], r["method_name"]): r["delta_fraction"]
                for r in full["items"]}
    d_by_key = {(r["class_name"], r["method_name"]): r["delta_fraction"]
                for r in direct["items"]}
    assert a_by_key == pytest.approx(d_by_key)


def test_ambiguous_graph_or_incompatible_modes_rejected():
    from flarith.query import compare_profiles, query_frames
    obj = sample()
    obj.threads[0].children[0].children_refs.append(1)
    shared = Profile.from_bytes(obj.SerializeToString())
    with pytest.raises(QueryError, match="PROFILE_UNSUPPORTED"):
        query_frames(shared, 0, group_by="symbol")

    left = Profile.from_bytes(sample().SerializeToString())
    obj = sample()
    obj.metadata.sampler_mode = 1  # ALLOCATION
    allocation = Profile.from_bytes(obj.SerializeToString())
    with pytest.raises(QueryError, match="PROFILE_UNSUPPORTED"):
        compare_profiles(left, allocation, "main")
    with pytest.raises(QueryError, match="INVALID_QUERY"):
        compare_profiles(left, modified_profile(), "missing")
    with pytest.raises(QueryError, match="INVALID_QUERY"):
        compare_profiles(left, modified_profile(), "main", limit=101)


async def test_mcp_comparison_roundtrip(monkeypatch):
    from flarith.cache import ProfileCache
    from flarith.query import query_frames
    monkeypatch.setattr(server, "cache", ProfileCache(rss_reader=lambda: 30 * 1048576))
    left = server.cache.load(sample().SerializeToString())
    right = server.cache.load(modified_profile().message.SerializeToString())
    async with Client(server.mcp) as client:
        tools = {t.name for t in await client.list_tools()}
        assert {"compare_profiles", "compare_results"} <= tools
        direct = await client.call_tool("compare_profiles", {
            "left_profile_ref": left,
            "right_profile_ref": right,
            "thread_name": "main",
        })
        assert not direct.is_error and direct.data["ok"]
        assert direct.data["complete"]
        with server.cache.borrow(left) as a, server.cache.borrow(right) as b:
            a_page = query_frames(a.profile, 0, group_by="symbol", limit=1)
            b_page = query_frames(b.profile, 0, group_by="symbol", limit=1)
        supplied = await client.call_tool("compare_results", {
            "left": a_page, "right": b_page,
        })
        assert not supplied.is_error and supplied.data["ok"]
        assert not supplied.data["complete"]
        unavailable = await client.call_tool("compare_profiles", {
            "left_profile_ref": "sha256:" + "0" * 64,
            "right_profile_ref": right,
            "thread_name": "main",
        })
        assert unavailable.data["error"]["code"] == "PROFILE_NOT_FOUND"
