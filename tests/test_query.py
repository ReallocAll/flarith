"""R3 contract checks: raw descriptor access, node-pool identity and statistics."""
import base64
import hashlib
import importlib
import json

import pytest
from fastmcp import Client

from flarith import Profile
from flarith.cache import ProfileCache
from flarith.proto.spark import spark_sampler_pb2
from flarith.query import (
    QueryError, describe_schema, get_call_tree, query_entity_distribution,
    query_frames, query_metrics, read_fields, read_raw, search_symbols,
    compare_time_ranges, query_subtree_hotspots,
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




def test_raw_large_pages_and_validation():
    mib = 1024 * 1024
    data = b"x" * (4 * mib + 7)

    default_page = read_raw(data)
    assert len(base64.b64decode(default_page["data_base64"])) == 2 * mib
    assert default_page["next_offset"] == 2 * mib
    assert default_page["has_more"]

    max_page = read_raw(data, limit=4 * mib)
    assert len(base64.b64decode(max_page["data_base64"])) == 4 * mib
    assert max_page["next_offset"] == 4 * mib
    assert max_page["has_more"]

    tail = read_raw(data, offset=max_page["next_offset"])
    assert base64.b64decode(tail["data_base64"]) == b"x" * 7
    assert not tail["has_more"] and tail["next_offset"] is None

    for invalid in (0, 4 * mib + 1, True):
        with pytest.raises(QueryError, match="INVALID_QUERY"):
            read_raw(data, limit=invalid)


def test_repeated_fields_paginate_past_ten_thousand():
    obj = sample()
    obj.time_windows.extend(range(10002))
    profile = Profile.from_bytes(obj.SerializeToString())
    page = read_fields(profile, ["time_windows"], offset=10001, limit=3)
    assert page["items"] == [9999, 10000, 10001]
    assert page["total"] == 10004
    assert not page["has_more"]
    assert page["next_offset"] is None

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
        tools = {tool.name: tool for tool in await client.list_tools()}
        assert tools["read_raw"].input_schema["properties"]["limit"]["default"] == 2 * 1024 * 1024
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
        assert raw.structured_content is None
        assert base64.b64decode(json.loads(raw.content[0].text)["data_base64"]) == payload[:256]
        assert server.cache.release(ref)
        lost = await client.call_tool("get_call_tree", {"profile_ref": ref, "thread_index": 0})
        assert lost.data["error"]["code"] == "PROFILE_NOT_FOUND"



def world_sample():
    obj = sample()
    stats = obj.metadata.platform_statistics.world
    stats.total_entities = 47
    stats.entity_counts["minecraft:villager_v2"] = 47
    overworld = stats.worlds.add()
    overworld.name = "Overworld"
    overworld.total_entities = 41
    first = overworld.regions.add()
    for x, z, villagers in [(-4, 7, 17), (0, 0, 14)]:
        chunk = first.chunks.add()
        chunk.x, chunk.z = x, z
        chunk.total_entities = villagers
        chunk.entity_counts["minecraft:villager_v2"] = villagers
    second = overworld.regions.add()
    chunk = second.chunks.add()
    chunk.x, chunk.z = 2, 3
    chunk.total_entities = 10
    chunk.entity_counts["minecraft:villager_v2"] = 10
    nether = stats.worlds.add()
    nether.name = "Nether"
    nether.total_entities = 6
    chunk = nether.regions.add().chunks.add()
    chunk.x, chunk.z = -4, 7
    chunk.total_entities = 6
    chunk.entity_counts["minecraft:villager_v2"] = 6
    # Empty region for zero-match and empty collection projection.
    nether.regions.add()
    return obj


def test_entity_distribution_world_identity_totals_and_pagination():
    profile = Profile.from_bytes(world_sample().SerializeToString())
    full = query_entity_distribution(profile, "minecraft:villager_v2")
    assert full["global_entity_count"] == 47
    assert full["observed_chunk_entity_count"] == 47
    assert (full["scanned_worlds"], full["scanned_regions"], full["scanned_chunks"]) == (2, 4, 4)
    assert full["total_matching_chunks"] == 4 and full["duplicate_chunk_records"] == 0
    assert [(row["world_index"], row["chunk_x"], row["chunk_z"], row["count"])
            for row in full["items"]] == [
                (0, -4, 7, 17), (0, 0, 0, 14), (0, 2, 3, 10), (1, -4, 7, 6)]
    first = query_entity_distribution(profile, "minecraft:villager_v2", limit=2)
    second = query_entity_distribution(profile, "minecraft:villager_v2", offset=first["next_offset"])
    assert first["has_more"] and [row["count"] for row in second["items"]] == [10, 6]
    assert not second["has_more"] and second["next_offset"] is None
    selected = query_entity_distribution(profile, "minecraft:villager_v2", world_index=1)
    assert selected["global_entity_count"] == 47  # still all-world global count
    assert selected["observed_chunk_entity_count"] == 6
    assert selected["items"][0]["world_name"] == "Nether"
    absent = query_entity_distribution(profile, "minecraft:does_not_exist")
    assert absent["global_entity_count"] == 0
    assert absent["observed_chunk_entity_count"] == 0
    assert absent["total_matching_chunks"] == 0 and absent["items"] == []
    with pytest.raises(QueryError, match="INVALID_QUERY"):
        query_entity_distribution(profile, "minecraft:villager_v2", world_index=2)
    with pytest.raises(QueryError, match="INVALID_QUERY"):
        query_entity_distribution(profile, "", limit=10)
    with pytest.raises(QueryError, match="INVALID_QUERY"):
        query_entity_distribution(profile, "minecraft:villager_v2", world_index=True)
    with pytest.raises(QueryError, match="FIELD_NOT_FOUND"):
        query_entity_distribution(Profile.from_bytes(sample().SerializeToString()), "minecraft:zombie")


def test_shallow_projection_defaults_maps_schema_and_response_budget():
    obj = world_sample()
    profile = Profile.from_bytes(obj.SerializeToString())
    chunks = ["metadata", "platform_statistics", "world", "worlds", 0, "regions", 0, "chunks"]
    selected = read_fields(profile, chunks, fields=["x", "z", "total_entities", "entity_counts"])
    assert selected["items"] == [
        {"x": -4, "z": 7, "total_entities": 17,
         "entity_counts": {"minecraft:villager_v2": 17}},
        {"x": 0, "z": 0, "total_entities": 14,
         "entity_counts": {"minecraft:villager_v2": 14}},
    ]
    assert selected["total"] == 2 and not selected["has_more"]
    assert read_fields(profile, chunks, limit=1)["items"][0]["message_type"] == "spark.WorldStatistics.Chunk"
    assert read_fields(profile, chunks, fields=[])["items"][0]["message_type"] == "spark.WorldStatistics.Chunk"
    assert read_fields(profile, chunks, offset=1, limit=1, fields=["x"])["items"] == [{"x": 0}]
    empty = read_fields(profile, ["metadata", "platform_statistics", "world",
                                  "worlds", 1, "regions", 1, "chunks"], fields=["x"])
    assert empty["items"] == [] and empty["total"] == 0
    region = read_fields(profile, chunks[:-1], fields=["total_entities"])
    assert region["type"] == "message" and region["value"] == {"total_entities": 0}
    for fields in (["unknown"], ["chunks"], ["x", "x"], ["x"] * 9, ["x", 7]):
        with pytest.raises(QueryError):
            read_fields(profile, chunks, fields=fields)
    with pytest.raises(QueryError, match="INVALID_QUERY"):
        read_fields(profile, ["metadata", "number_of_ticks"], fields=["x"])
    with pytest.raises(QueryError, match="INVALID_QUERY"):
        read_fields(profile, chunks[:-1], offset=1, fields=["total_entities"])
    # 100 projected objects can easily exceed MCP response budgets.
    for index in range(100):
        obj.metadata.platform_statistics.world.worlds.add().name = "x" * 3000
    large = Profile.from_bytes(obj.SerializeToString())
    with pytest.raises(QueryError, match="RESOURCE_LIMIT"):
        read_fields(large, ["metadata", "platform_statistics", "world", "worlds"],
                    limit=100, fields=["name"])


async def test_entity_distribution_mcp_roundtrip(monkeypatch):
    monkeypatch.setattr(server, "cache", ProfileCache(rss_reader=lambda: 30 * 1048576))
    ref = server.cache.load(world_sample().SerializeToString())
    async with Client(server.mcp) as client:
        tools = {tool.name: tool for tool in await client.list_tools()}
        assert "query_entity_distribution" in tools
        assert "fields" in tools["read_fields"].input_schema["properties"]
        result = await client.call_tool("query_entity_distribution", {
            "profile_ref": ref, "entity_type": "minecraft:villager_v2", "limit": 2,
        })
        assert not result.is_error
        assert result.data["ok"] and result.data["global_entity_count"] == 47
        assert [row["count"] for row in result.data["items"]] == [17, 14]
        projected = await client.call_tool("read_fields", {
            "profile_ref": ref,
            "path": ["metadata", "platform_statistics", "world", "worlds",
                     0, "regions", 0, "chunks"],
            "fields": ["x", "z", "entity_counts"],
        })
        assert not projected.is_error
        assert projected.data["items"][0]["entity_counts"]["minecraft:villager_v2"] == 17


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


def test_supplied_comparison_uses_each_side_completeness():
    from flarith.query import compare_results, query_frames
    left = Profile.from_bytes(sample().SerializeToString())
    obj = sample()
    obj.threads[0].children[1].class_name = "Novel"
    obj.threads[0].children[1].method_name = "run"
    right = Profile.from_bytes(obj.SerializeToString())
    full_left = query_frames(left, 0, group_by="symbol", limit=50)
    top_right = query_frames(right, 0, group_by="symbol", limit=1)
    comparison = compare_results(full_left, top_right)
    assert not comparison["complete"]
    novel = next(row for row in comparison["items"] if row["class_name"] == "Novel")
    assert novel["left_self"] == 0.0  # known absent: left page is complete
    assert novel["right_self"] == 20.0
    assert novel["delta_fraction"] == pytest.approx(20.0 / 30.0)
    other = next(row for row in comparison["items"] if row["class_name"] == "Entity")
    assert other["right_self"] is None  # unknown absent: right page truncated
    assert other["delta_fraction"] is None

    filtered = query_frames(left, 0, group_by="symbol", text="Entity")
    with pytest.raises(QueryError, match="PROFILE_UNSUPPORTED"):
        compare_results(filtered, top_right)
    a = query_frames(left, 0, group_by="symbol", window_index=0)
    b = query_frames(left, 0, group_by="symbol", window_index=1)
    windows = compare_results(a, b)
    assert windows["left_window_index"] == 0
    assert windows["right_window_index"] == 1
    assert query_frames(left, 0, group_by="symbol", offset=20000)["items"] == []

    
def test_window_range_aggregation_matches_individual_windows():
    profile = Profile.from_bytes(sample().SerializeToString())
    both = query_frames(profile, 0, group_by="symbol", start_window=0, end_window=2)
    one = query_frames(profile, 0, group_by="symbol", window_index=0)
    two = query_frames(profile, 0, group_by="symbol", window_index=1)
    by_name = lambda result: {row["class_name"]: row["self"] for row in result["items"]}
    all_weights = by_name(both)
    assert both["thread_total"] == 30
    assert both["start_window_id"] == 7 and both["last_window_id"] == 8
    assert one["window_id"] == 7
    for key, weight in all_weights.items():
        assert weight == pytest.approx(by_name(one)[key] + by_name(two)[key])
    assert both["complete"]
    assert sum(all_weights.values()) == pytest.approx(both["thread_total"])

    by_class = query_frames(profile, 0, group_by="class", start_window=0, end_window=2)
    assert by_class["complete"]
    assert by_class["total_groups"] == 3
    assert {r["class_name"]: r["self"] for r in by_class["items"]} == pytest.approx(all_weights)

    node = query_frames(profile, 0, group_by="node", start_window=0, end_window=2)
    root = next(row for row in node["items"] if row["node_index"] == 0)
    assert root["inclusive"] == 30
    assert root["self"] == 7
    for kwargs in ({"start_window": 0}, {"end_window": 2},
                   {"start_window": 1, "end_window": 1},
                   {"start_window": 0, "end_window": 3},
                   {"start_window": True, "end_window": 2},
                   {"window_index": 0, "start_window": 0, "end_window": 1}):
        with pytest.raises(QueryError, match="INVALID_QUERY"):
            query_frames(profile, 0, group_by="symbol", **kwargs)


def test_time_range_comparison_is_full_not_topk_subtraction():
    profile = Profile.from_bytes(sample().SerializeToString())
    result = compare_time_ranges(profile, 0, 0, 1, 1, 2, limit=1)
    assert result["complete"]
    assert result["baseline_thread_total"] == 10
    assert result["target_thread_total"] == 20
    assert result["baseline_first_window_id"] == 7
    assert result["target_first_window_id"] == 8
    assert result["has_more"]
    all_rows = compare_time_ranges(profile, 0, 0, 1, 1, 2, limit=50)
    by_name = {r["class_name"]: r for r in all_rows["items"]}
    assert by_name["Level"]["left_fraction"] == pytest.approx(0.2)
    assert by_name["Level"]["right_fraction"] == pytest.approx(0.25)
    assert by_name["Level"]["delta_fraction"] == pytest.approx(0.05)
    assert by_name["Entity"]["delta_fraction"] == pytest.approx(-0.05)
    assert by_name["Goal"]["delta_fraction"] == pytest.approx(0)
    assert not all_rows["has_more"]
    with pytest.raises(QueryError, match="INVALID_QUERY"):
        compare_time_ranges(profile, 0, 1, 1, 1, 2)


def test_subtree_self_and_sorted_children():
    obj = sample()
    # Original reference order places the lighter child first.
    obj.threads[0].children[0].children_refs[:] = [2, 1]
    profile = Profile.from_bytes(obj.SerializeToString())
    original = get_call_tree(profile, 0, 0)
    assert [r["node_index"] for r in original["children"]] == [2, 1]
    heavy = get_call_tree(profile, 0, 0, order_by="inclusive", limit=1)
    assert [r["node_index"] for r in heavy["children"]] == [1]
    assert heavy["next_offset"] == 1
    assert get_call_tree(profile, 0, 0, order_by="self", offset=1)["children"][0]["node_index"] == 2

    subtree = query_subtree_hotspots(profile, 0, 0, start_window=0, end_window=2)
    assert subtree["subtree_inclusive"] == 30
    assert subtree["complete"]
    assert all("subtree_fraction" in row and "thread_fraction" not in row
               for row in subtree["items"])
    assert sum(row["self"] for row in subtree["items"]) == pytest.approx(30)
    assert {row["class_name"]: row["example_node_index"] for row in subtree["items"]} == {
        "Level": 0, "Entity": 1, "Goal": 2,
    }
    leaf = query_subtree_hotspots(profile, 0, 1, window_index=0)
    assert leaf["subtree_inclusive"] == 7
    assert len(leaf["items"]) == 1 and leaf["items"][0]["self"] == 7
    assert leaf["window_id"] == 7
    by_class = query_subtree_hotspots(profile, 0, 0, group_by="class", limit=1)
    assert by_class["total_groups"] == 3 and by_class["has_more"]
    for invalid in (-1, 99, True):
        with pytest.raises(QueryError):
            query_subtree_hotspots(profile, 0, invalid)
    with pytest.raises(QueryError, match="INVALID_QUERY"):
        get_call_tree(profile, 0, 0, order_by="invalid")


def test_recursive_self_and_shared_node_aggregation():
    obj = sample()
    obj.threads[0].children[1].class_name = "Level"
    obj.threads[0].children[1].method_name = "tick"
    profile = Profile.from_bytes(obj.SerializeToString())
    rows = query_subtree_hotspots(profile, 0, 0, window_index=0)
    by_name = {r["class_name"]: r for r in rows["items"]}
    assert by_name["Level"]["self"] == 9
    assert by_name["Level"]["occurrences"] == 2
    assert sum(r["self"] for r in rows["items"]) == pytest.approx(10)

    # Shared nodes are not counted once per path by accident.
    obj.threads[0].children[2].children_refs.append(1)
    shared = Profile.from_bytes(obj.SerializeToString())
    with pytest.raises(QueryError, match="PROFILE_UNSUPPORTED"):
        query_subtree_hotspots(shared, 0, 0)
    with pytest.raises(QueryError, match="PROFILE_UNSUPPORTED"):
        compare_time_ranges(shared, 0, 0, 1, 1, 2)


async def test_new_aggregation_tools_mcp_roundtrip(monkeypatch):
    monkeypatch.setattr(server, "cache", ProfileCache(rss_reader=lambda: 30 * 1048576))
    ref = server.cache.load(sample().SerializeToString())
    async with Client(server.mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}
        assert {"compare_time_ranges", "query_subtree_hotspots", "query_frames"} <= tools.keys()
        assert "start_window" in tools["query_frames"].input_schema["properties"]
        group = await client.call_tool("query_frames", {
            "profile_ref": ref, "thread_index": 0, "group_by": "class",
            "start_window": 0, "end_window": 2,
        })
        assert group.data["ok"] and group.data["thread_total"] == 30
        comparison = await client.call_tool("compare_time_ranges", {
            "profile_ref": ref, "thread_index": 0, "baseline_start": 0,
            "baseline_end": 1, "target_start": 1, "target_end": 2,
        })
        assert comparison.data["ok"] and comparison.data["total_symbols"] == 3
        subtree = await client.call_tool("query_subtree_hotspots", {
            "profile_ref": ref, "thread_index": 0, "node_index": 0,
        })
        assert subtree.data["ok"] and subtree.data["subtree_inclusive"] == 30
