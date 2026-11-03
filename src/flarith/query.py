"""Bounded, read-only Spark Protobuf queries over the R0 node pool.

All values come from the encoded SamplerData. No deobfuscation or extrapolated
causality. Offset pagination is scoped to an immutable cached Profile.
"""
from __future__ import annotations

import base64
import heapq
import json
import math

from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message
from google.protobuf import descriptor_pool

from flarith.core import AmbiguousPathError, InvalidProfileError, Profile


class QueryError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _bounds(offset: int, limit: int, maximum: int = 100, max_offset: int = 10000) -> None:
    if (type(offset) is not int or type(limit) is not int
            or offset < 0 or offset > max_offset or not 1 <= limit <= maximum):
        raise QueryError("INVALID_QUERY")


def _number(value):
    if isinstance(value, float) and not math.isfinite(value):
        raise QueryError("PROFILE_UNSUPPORTED")
    return str(value) if isinstance(value, int) and abs(value) > (1 << 53) - 1 else value


def _is_map(field: FieldDescriptor) -> bool:
    return bool(field.message_type and field.message_type.GetOptions().map_entry)


def _encode(value, field: FieldDescriptor | None = None):
    if isinstance(value, Message):
        return {
            "message_type": value.DESCRIPTOR.full_name,
            "present_fields": [f.name for f, _ in value.ListFields()],
        }
    if isinstance(value, bytes):
        if len(value) > 65536:
            raise QueryError("RESOURCE_LIMIT")
        return {"base64": base64.b64encode(value).decode("ascii"), "byte_count": len(value)}
    if isinstance(value, str) and len(value.encode("utf-8")) > 65536:
        raise QueryError("RESOURCE_LIMIT")
    if field is not None and field.type in (3, 4, 6, 16, 18):
        return str(value)
    return _number(value)


def describe_schema(message_type: str = "spark.SamplerData") -> dict:
    if not message_type.startswith("spark.") or len(message_type) > 200:
        raise QueryError("INVALID_QUERY")
    try:
        descriptor = descriptor_pool.Default().FindMessageTypeByName(message_type)
    except KeyError as exc:
        raise QueryError("FIELD_NOT_FOUND") from exc
    fields = []
    for f in descriptor.fields:
        fields.append({
            "name": f.name,
            "number": f.number,
            "kind": "map" if _is_map(f) else "repeated" if f.is_repeated else "message" if f.message_type else "enum" if f.enum_type else "scalar",
            "type": f.message_type.full_name if f.message_type else f.enum_type.full_name if f.enum_type else next((attr[5:].lower() for attr in dir(FieldDescriptor) if attr.startswith("TYPE_") and getattr(FieldDescriptor, attr) == f.type), str(f.type)),
            "has_presence": f.has_presence,
        })
    return {"message_type": descriptor.full_name, "fields": fields}



# Projection is deliberately shallow: scalar fields and scalar-valued maps only.
# Charge the serialized rows against a fixed response budget before returning.
PROJECTION_BUDGET = 256 * 1024


def _projection_fields(descriptor, fields: list[str]):
    if (len(fields) > 8 or any(type(name) is not str or not name for name in fields)
            or len(set(fields)) != len(fields)):
        raise QueryError("INVALID_QUERY")
    selected = []
    for name in fields:
        field = descriptor.fields_by_name.get(name)
        if field is None:
            raise QueryError("FIELD_NOT_FOUND")
        if field.is_repeated and not _is_map(field):
            raise QueryError("INVALID_QUERY")
        if field.message_type and not _is_map(field):
            raise QueryError("INVALID_QUERY")
        if _is_map(field) and field.message_type.fields_by_name["value"].message_type:
            raise QueryError("INVALID_QUERY")
        selected.append(field)
    return selected


def _project_message(message: Message, selected) -> dict:
    projected = {}
    for field in selected:
        if field.has_presence and not message.HasField(field.name):
            projected[field.name] = None
            continue
        value = getattr(message, field.name)
        if _is_map(field):
            if len(value) > 256:
                raise QueryError("RESOURCE_LIMIT")
            key_field = field.message_type.fields_by_name["key"]
            value_field = field.message_type.fields_by_name["value"]
            projected[field.name] = {
                str(_encode(key, key_field)): _encode(value[key], value_field)
                for key in sorted(value)
            }
        else:
            projected[field.name] = _encode(value, field)
    return projected


def _projected_read(value, field, path, offset: int, limit: int, fields: list[str]) -> dict:
    if isinstance(value, Message):
        if offset:
            raise QueryError("INVALID_QUERY")
        selected = _projection_fields(value.DESCRIPTOR, fields)
        result = {"path": path, "type": "message", "fields": fields,
                  "value": _project_message(value, selected)}
        if len(json.dumps(result, ensure_ascii=False).encode("utf-8")) > PROJECTION_BUDGET:
            raise QueryError("RESOURCE_LIMIT")
        return result
    if (field is None or not field.is_repeated or _is_map(field)
            or field.message_type is None):
        raise QueryError("INVALID_QUERY")
    selected = _projection_fields(field.message_type, fields)
    count = len(value)
    items = []
    used = 256
    for message in value[offset:offset + limit]:
        row = _project_message(message, selected)
        used += len(json.dumps(row, ensure_ascii=False).encode("utf-8"))
        if used > PROJECTION_BUDGET:
            raise QueryError("RESOURCE_LIMIT")
        items.append(row)
    return {"path": path, "type": "collection", "fields": fields, "total": count,
            "offset": offset, "items": items,
            "has_more": offset + len(items) < count,
            "next_offset": offset + len(items) if offset + len(items) < count else None}


def read_fields(profile: Profile, path: list[str | int], offset: int = 0, limit: int = 50,
                fields: list[str] | None = None) -> dict:
    _bounds(offset, limit, max_offset=268435456)
    if not isinstance(path, list) or len(path) > 20:
        raise QueryError("INVALID_QUERY")
    if fields is not None and (type(fields) is not list or len(fields) > 8):
        raise QueryError("INVALID_QUERY")
    value = profile.message
    field = None
    for token in path:
        if isinstance(value, Message):
            if not isinstance(token, str) or token not in value.DESCRIPTOR.fields_by_name:
                raise QueryError("FIELD_NOT_FOUND")
            field = value.DESCRIPTOR.fields_by_name[token]
            if field.has_presence and not value.HasField(token):
                raise QueryError("FIELD_NOT_FOUND")
            value = getattr(value, token)
        elif hasattr(value, "keys"):  # Protobuf map container
            if token not in value:
                raise QueryError("FIELD_NOT_FOUND")
            value = value[token]
        elif not isinstance(value, (str, bytes)) and hasattr(value, "__len__") and hasattr(value, "__getitem__"):
            if type(token) is not int or not 0 <= token < len(value):
                raise QueryError("FIELD_NOT_FOUND")
            value = value[token]
        else:
            raise QueryError("FIELD_NOT_FOUND")
    if fields:
        return _projected_read(value, field, path, offset, limit, fields)
    if isinstance(value, Message):
        return {"path": path, "type": "message", "value": _encode(value)}
    if hasattr(value, "keys"):
        # Explicit stable ordering; never mutate Protobuf maps by indexing a
        # missing key. For large maps, caller selects an individual key.
        count = len(value)
        if count > 200000:
            raise QueryError("RESOURCE_LIMIT")
        keys = sorted(value)
        selected = keys[offset:offset + limit]
        values = [{"key": _encode(key, field.message_type.fields_by_name["key"]), "value": _encode(value[key], field.message_type.fields_by_name["value"])} for key in selected]
    elif not isinstance(value, (str, bytes)) and hasattr(value, "__len__") and hasattr(value, "__getitem__"):
        count = len(value)
        values = [_encode(v, field) for v in value[offset:offset + limit]]
    else:
        if offset:
            raise QueryError("INVALID_QUERY")
        return {"path": path, "type": "scalar", "value": _encode(value, field)}
    return {"path": path, "type": "collection", "total": count, "offset": offset,
            "items": values, "has_more": offset + len(values) < count,
            "next_offset": offset + len(values) if offset + len(values) < count else None}



def query_entity_distribution(profile: Profile, entity_type: str,
                              world_index: int | None = None, offset: int = 0,
                              limit: int = 10) -> dict:
    """Rank entity-count snapshots by world and chunk, not by sampled time window."""
    _bounds(offset, limit, max_offset=268435456)
    if (type(entity_type) is not str or not 1 <= len(entity_type) <= 256
            or type(world_index) not in (int, type(None))):
        raise QueryError("INVALID_QUERY")
    stats = profile.message.metadata.platform_statistics
    if not stats.HasField("world"):
        raise QueryError("FIELD_NOT_FOUND")
    world_stats = stats.world
    worlds = world_stats.worlds
    if world_index is not None and not 0 <= world_index < len(worlds):
        raise QueryError("INVALID_QUERY")

    counts = {}
    seen = set()
    scanned_regions = scanned_chunks = duplicate_chunk_records = 0
    observed = 0
    indices = range(len(worlds)) if world_index is None else (world_index,)
    for index in indices:
        world = worlds[index]
        for region in world.regions:
            scanned_regions += 1
            for chunk in region.chunks:
                scanned_chunks += 1
                key = (index, chunk.x, chunk.z)
                if key in seen:
                    duplicate_chunk_records += 1
                seen.add(key)
                count = chunk.entity_counts.get(entity_type, 0)
                if count < 0:
                    raise QueryError("PROFILE_UNSUPPORTED")
                if not count:
                    continue
                observed += count
                counts[key] = counts.get(key, 0) + count

    ranked = sorted(counts.items(), key=lambda item: (
        -item[1], item[0][0], item[0][1], item[0][2]))
    page = ranked[offset:offset + limit]
    return {
        "profile_ref": profile.profile_ref,
        "entity_type": entity_type,
        "world_index": world_index,
        "global_entity_count": world_stats.entity_counts.get(entity_type, 0),
        "observed_chunk_entity_count": observed,
        "scanned_worlds": len(indices),
        "scanned_regions": scanned_regions,
        "scanned_chunks": scanned_chunks,
        "duplicate_chunk_records": duplicate_chunk_records,
        "total_matching_chunks": len(ranked),
        "items": [
            {"world_index": key[0], "world_name": worlds[key[0]].name,
             "chunk_x": key[1], "chunk_z": key[2], "count": count}
            for key, count in page
        ],
        "offset": offset,
        "has_more": offset + len(page) < len(ranked),
        "next_offset": offset + len(page) if offset + len(page) < len(ranked) else None,
        "note": "World statistics snapshot; global count covers all worlds, even if world_index is selected.",
    }


def read_raw(raw: bytes, offset: int = 0, limit: int = 2 * 1024 * 1024) -> dict:
    _bounds(offset, limit, maximum=4 * 1024 * 1024, max_offset=268435456)
    length = len(raw)
    chunk = raw[offset:offset + limit]
    return {"offset": offset, "total_bytes": length,
            "data_base64": base64.b64encode(chunk).decode("ascii"),
            "has_more": offset + len(chunk) < length,
            "next_offset": offset + len(chunk) if offset + len(chunk) < length else None}


def _weights(values, index: int | None,
             window_range: tuple[int, int] | None = None) -> float:
    if window_range is not None:
        start, end = window_range
        if len(values) < end:
            raise QueryError("PROFILE_UNSUPPORTED")
        result = math.fsum(values[start:end])
    elif index is None:
        result = math.fsum(values)
    else:
        if not 0 <= index < len(values):
            raise QueryError("INVALID_QUERY")
        result = values[index]
    if not math.isfinite(result):
        raise QueryError("PROFILE_UNSUPPORTED")
    return result


def _window(profile: Profile, window_index: int | None):
    if window_index is None:
        return
    if type(window_index) is not int or not 0 <= window_index < len(profile.message.time_windows):
        raise QueryError("INVALID_QUERY")


def _window_range(profile: Profile, window_index: int | None,
                  start_window: int | None, end_window: int | None) -> tuple[int, int] | None:
    _window(profile, window_index)
    if start_window is None and end_window is None:
        return None
    if (window_index is not None or type(start_window) is not int
            or type(end_window) is not int
            or not 0 <= start_window < end_window <= len(profile.message.time_windows)):
        raise QueryError("INVALID_QUERY")
    return start_window, end_window


def _frame(graph, index: int, window_index: int | None,
           window_range: tuple[int, int] | None = None):
    node = graph.node(index)
    inclusive = _weights(node.times, window_index, window_range)
    child_total = math.fsum(_weights(graph.node(c).times, window_index, window_range)
                            for c in node.children_refs)
    self_weight = inclusive - child_total
    return {
        "thread_index": graph.thread_index, "node_index": index,
        "class_name": node.class_name, "method_name": node.method_name,
        "method_desc": node.method_desc, "line_number": node.line_number,
        "inclusive": inclusive, "self": self_weight,
    }


def get_call_tree(profile: Profile, thread_index: int, node_index: int | None = None,
                  offset: int = 0, limit: int = 50,
                  window_index: int | None = None, order_by: str = "original") -> dict:
    _bounds(offset, limit)
    _window(profile, window_index)
    if (type(thread_index) is not int or not 0 <= thread_index < len(profile.threads)
            or order_by not in ("original", "self", "inclusive")):
        raise QueryError("INVALID_QUERY")
    graph = profile.threads[thread_index]
    if node_index is None:
        refs = graph.roots
        record = {"type": "thread", "name": graph.thread.name,
                  "inclusive": _weights(graph.thread.times, window_index)}
        ancestors = []
        ambiguous = False
    else:
        if type(node_index) is not int or not 0 <= node_index < len(graph.thread.children):
            raise QueryError("INVALID_NODE_INDEX")
        refs = graph.children(node_index)
        record = {"type": "frame", **_frame(graph, node_index, window_index)}
        try:
            ancestors = list(graph.path_to(node_index))[:-1]
            ambiguous = False
        except (AmbiguousPathError, InvalidProfileError):
            ancestors, ambiguous = [], True
    if order_by != "original":
        ranked = [_frame(graph, index, window_index) for index in refs]
        ranked.sort(key=lambda row: (-row[order_by], row["node_index"]))
        children = ranked[offset:offset + limit]
    else:
        children = [_frame(graph, index, window_index) for index in refs[offset:offset + limit]]
    return {"profile_ref": profile.profile_ref, "node": record, "ancestors": ancestors,
            "ambiguous_path": ambiguous, "window_index": window_index,
            "window_id": profile.message.time_windows[window_index] if window_index is not None else None,
            "order_by": order_by, "children": children, "total_children": len(refs),
            "has_more": offset + len(children) < len(refs),
            "next_offset": offset + len(children) if offset + len(children) < len(refs) else None,
            "weight_unit": _unit(profile)}


def search_symbols(profile: Profile, text: str, mode: str = "substring",
                   thread_index: int | None = None, offset: int = 0,
                   limit: int = 50) -> dict:
    _bounds(offset, limit)
    if not 1 <= len(text) <= 256 or mode not in ("exact", "prefix", "substring"):
        raise QueryError("INVALID_QUERY")
    if thread_index is not None and (type(thread_index) is not int or not 0 <= thread_index < len(profile.threads)):
        raise QueryError("INVALID_QUERY")
    matches, seen = [], 0
    graphs = profile.threads if thread_index is None else (profile.threads[thread_index],)
    for graph in graphs:
        for i, node in enumerate(graph.thread.children):
            symbol = node.class_name + "." + node.method_name
            matched = symbol == text if mode == "exact" else symbol.startswith(text) if mode == "prefix" else text in symbol
            if not matched:
                continue
            if seen >= offset and len(matches) < limit:
                matches.append({"thread_index": graph.thread_index, "node_index": i, "symbol": symbol})
            seen += 1
    return {"profile_ref": profile.profile_ref, "items": matches, "total_matches": seen,
            "offset": offset, "has_more": offset + len(matches) < seen,
            "next_offset": offset + len(matches) if offset + len(matches) < seen else None}


def _group_page(groups: dict, group_by: str, total_weight: float,
                offset: int, limit: int, text: str | None,
                fraction_field: str = "thread_fraction") -> dict:
    if text is not None:
        groups = {key: value for key, value in groups.items()
                  if text in ".".join(key)}
    top = heapq.nsmallest(offset + limit + 1, groups.items(),
                          key=lambda kv: (-kv[1][0], kv[0]))
    items = []
    for key, value in top[offset:offset + limit]:
        row = {"class_name": key[0], "self": value[0],
               "occurrences": value[1], "example_node_index": value[2],
               fraction_field: value[0] / total_weight if total_weight else None}
        if group_by == "symbol":
            row.update(method_name=key[1], method_desc=key[2])
        items.append(row)
    more = len(top) > offset + limit
    return {"items": items, "total_groups": len(groups), "offset": offset,
            "has_more": more, "next_offset": offset + len(items) if more else None,
            "complete": offset == 0 and not more and len(items) == len(groups)}


def query_frames(profile: Profile, thread_index: int, sort_by: str = "self",
                 window_index: int | None = None, text: str | None = None,
                 offset: int = 0, limit: int = 50, group_by: str = "node",
                 start_window: int | None = None, end_window: int | None = None) -> dict:
    _bounds(offset, limit, max_offset=200000 if group_by != "node" else 10000)
    selected = _window_range(profile, window_index, start_window, end_window)
    if (type(thread_index) is not int or not 0 <= thread_index < len(profile.threads)
            or sort_by not in ("self", "inclusive")
            or group_by not in ("node", "symbol", "class")
            or (group_by != "node" and sort_by != "self")):
        raise QueryError("INVALID_QUERY")
    if text is not None and (not isinstance(text, str) or not 1 <= len(text) <= 256):
        raise QueryError("INVALID_QUERY")
    graph = profile.threads[thread_index]
    thread_total = _weights(graph.thread.times, window_index, selected)
    span = {"window_index": window_index,
            "window_id": profile.message.time_windows[window_index] if window_index is not None else None,
            "start_window": start_window, "end_window": end_window,
            "start_window_id": profile.message.time_windows[start_window] if selected else None,
            "last_window_id": profile.message.time_windows[end_window - 1] if selected else None}
    if group_by != "node":
        groups = _aggregate_symbol_self(graph, window_index, selected,
                                        group_by=group_by)
        page = _group_page(groups, group_by, thread_total, offset, limit, text)
        return {"profile_ref": profile.profile_ref, "thread_index": thread_index,
                "thread_name": graph.thread.name, **span,
                "sampler_mode": profile.message.metadata.sampler_mode,
                "sampler_engine": profile.message.metadata.sampler_engine,
                "aggregator_type": profile.message.metadata.data_aggregator.type,
                "sort_by": sort_by, "group_by": group_by,
                "text_filter": text, "thread_total": thread_total,
                "weight_unit": _unit(profile), **page}
    records = (_frame(graph, i, window_index, selected)
               for i, node in enumerate(graph.thread.children)
               if text is None or text in node.class_name + "." + node.method_name)
    top = heapq.nlargest(offset + limit + 1, records,
                         key=lambda row: (row[sort_by], -row["node_index"]))
    picked = top[offset:offset + limit]
    for item in picked:
        item["thread_fraction"] = item[sort_by] / thread_total if thread_total else None
    more = len(top) > offset + limit
    return {"profile_ref": profile.profile_ref, "thread_index": thread_index,
            **span, "sort_by": sort_by, "group_by": "node",
            "thread_total": thread_total, "weight_unit": _unit(profile),
            "items": picked, "offset": offset, "has_more": more,
            "next_offset": offset + len(picked) if more else None,
            "note": "Nodes may be nested; inclusive values must not be summed."}


def query_subtree_hotspots(profile: Profile, thread_index: int, node_index: int,
                           group_by: str = "symbol", window_index: int | None = None,
                           offset: int = 0, limit: int = 50,
                           start_window: int | None = None,
                           end_window: int | None = None) -> dict:
    _bounds(offset, limit, max_offset=200000)
    selected = _window_range(profile, window_index, start_window, end_window)
    if (type(thread_index) is not int or not 0 <= thread_index < len(profile.threads)
            or type(node_index) is not int
            or group_by not in ("symbol", "class")):
        raise QueryError("INVALID_QUERY")
    graph = profile.threads[thread_index]
    if not 0 <= node_index < len(graph.thread.children):
        raise QueryError("INVALID_NODE_INDEX")
    subtree_total = _weights(graph.node(node_index).times, window_index, selected)
    groups = _aggregate_symbol_self(graph, window_index, selected,
                                    roots=(node_index,), group_by=group_by)
    page = _group_page(groups, group_by, subtree_total, offset, limit, None,
                       fraction_field="subtree_fraction")
    return {"profile_ref": profile.profile_ref, "thread_index": thread_index,
            "root_node_index": node_index, "group_by": group_by,
            "window_index": window_index,
            "window_id": profile.message.time_windows[window_index] if window_index is not None else None,
            "start_window": start_window, "end_window": end_window,
            "subtree_inclusive": subtree_total, "weight_unit": _unit(profile),
            "fraction_basis": "subtree_inclusive",
            **page}


def query_metrics(profile: Profile, series: str = "windows",
                  offset: int = 0, limit: int = 50) -> dict:
    _bounds(offset, limit)
    data = profile.message
    if series == "windows":
        windows = data.time_windows
        indices = windows[offset:offset + limit]
        rows = []
        for key in indices:
            if key not in data.time_window_statistics:
                rows.append({"window_id": key, "available": False})
                continue
            stat = data.time_window_statistics[key]
            rows.append({"window_id": key, "available": True,
                         **{field.name: _encode(v, field) for field, v in stat.ListFields()}})
        total = len(windows)
    else:
        metrics = data.metadata.metrics
        if series not in metrics.DESCRIPTOR.fields_by_name:
            raise QueryError("FIELD_NOT_FOUND")
        field = metrics.DESCRIPTOR.fields_by_name[series]
        if field.message_type is None or not metrics.HasField(series):
            raise QueryError("FIELD_NOT_FOUND")
        source = getattr(metrics, series)
        values = source.values
        deltas = source.timestamp_deltas_ms
        if len(deltas) not in (len(values), len(values) - 1):
            raise QueryError("PROFILE_UNSUPPORTED")
        total = len(values)
        def metric_value(item):
            if isinstance(item, Message):
                # Spark metric value messages are small, flat structs such
                # as RollingAverageValues and MemoryUsage.
                return {f.name: _encode(v, f) for f, v in item.ListFields()}
            return _encode(item)

        rows = [{"index": i, "value": metric_value(values[i]),
                 "timestamp_delta_ms": deltas[i] if i < len(deltas) else None}
                for i in range(offset, min(offset + limit, total))]
    return {"profile_ref": profile.profile_ref, "series": series,
            "start_timestamp_ms": str(getattr(data.metadata.metrics, series).start_timestamp_ms) if series != "windows" else None,
            "total": total, "offset": offset, "items": rows,
            "has_more": offset + len(rows) < total,
            "next_offset": offset + len(rows) if offset + len(rows) < total else None}

def _unit(profile: Profile) -> str:
    return "bytes" if profile.message.metadata.sampler_mode == 1 else "milliseconds"


def _aggregate_symbol_self(graph, window_index: int | None,
                           window_range: tuple[int, int] | None = None,
                           *, roots: tuple[int, ...] | None = None,
                           group_by: str = "symbol") -> dict:
    """Sum each reachable node's Self once, including recursive symbols.

    Shared or multiply rooted nodes have ambiguous attribution and are not
    silently counted more than once. Inclusive weights are never summed.
    """
    nodes = graph.thread.children
    if len(nodes) > 200000:
        raise QueryError("RESOURCE_LIMIT")
    groups = {}
    visited = set()
    pending = list(graph.roots if roots is None else roots)
    while pending:
        index = pending.pop()
        if index in visited or len(graph.parents[index]) != 1:
            raise QueryError("PROFILE_UNSUPPORTED")
        visited.add(index)
        node = nodes[index]
        pending.extend(node.children_refs)
        record = _frame(graph, index, window_index, window_range)
        key = ((node.class_name, node.method_name, node.method_desc)
               if group_by == "symbol" else (node.class_name,))
        if key in groups:
            weight, count, example = groups[key]
            groups[key] = (weight + record["self"], count + 1, example)
        else:
            groups[key] = (record["self"], 1, index)
    return groups


def _thread_by_name(profile: Profile, name: str):
    if not isinstance(name, str) or not 1 <= len(name) <= 256:
        raise QueryError("INVALID_QUERY")
    matches = [g for g in profile.threads if g.thread.name == name]
    if len(matches) != 1:
        raise QueryError("INVALID_QUERY")
    return matches[0]


def _comparison_compatible(left: Profile, right: Profile) -> None:
    a, b = left.message.metadata, right.message.metadata
    if (a.sampler_mode != b.sampler_mode
            or a.sampler_engine != b.sampler_engine
            or a.data_aggregator.type != b.data_aggregator.type):
        raise QueryError("PROFILE_UNSUPPORTED")


def _symbol_rows(left, right, left_total: float, right_total: float,
                 offset: int, limit: int, *, left_complete: bool,
                 right_complete: bool) -> dict:
    keys = set(left) | set(right)
    if len(keys) > 200000:
        raise QueryError("RESOURCE_LIMIT")
    # Rank changes by absolute share delta; unknown omitted rows never
    # masquerade as zeros in partial-result comparisons.
    rows = []
    for key in keys:
        lv, rv = left.get(key), right.get(key)
        left_weight = lv[0] if lv is not None else (0.0 if left_complete else None)
        right_weight = rv[0] if rv is not None else (0.0 if right_complete else None)
        a = left_weight / left_total if left_weight is not None and left_total else None
        b = right_weight / right_total if right_weight is not None and right_total else None
        delta = b - a if a is not None and b is not None else None
        rows.append({"class_name": key[0], "method_name": key[1],
                     "method_desc": key[2], "left_self": left_weight,
                     "right_self": right_weight,
                     "left_fraction": a, "right_fraction": b,
                     "delta_fraction": delta,
                     "observed_in_both": lv is not None and rv is not None})
    rows.sort(key=lambda r: (-abs(r["delta_fraction"]) if r["delta_fraction"] is not None else float("inf"),
                             r["class_name"], r["method_name"], r["method_desc"]))
    selected = rows[offset:offset + limit]
    more = offset + len(selected) < len(rows)
    return {"items": selected, "total_symbols": len(rows),
            "offset": offset, "has_more": more,
            "next_offset": offset + len(selected) if more else None}


def compare_time_ranges(profile: Profile, thread_index: int,
                        baseline_start: int, baseline_end: int,
                        target_start: int, target_end: int,
                        offset: int = 0, limit: int = 50) -> dict:
    """Compare full symbol Self shares across two ranges of one thread."""
    _bounds(offset, limit, max_offset=200000)
    if type(thread_index) is not int or not 0 <= thread_index < len(profile.threads):
        raise QueryError("INVALID_QUERY")
    baseline = _window_range(profile, None, baseline_start, baseline_end)
    target = _window_range(profile, None, target_start, target_end)
    graph = profile.threads[thread_index]
    left_total = _weights(graph.thread.times, None, baseline)
    right_total = _weights(graph.thread.times, None, target)
    left = _aggregate_symbol_self(graph, None, baseline)
    right = _aggregate_symbol_self(graph, None, target)
    page = _symbol_rows(left, right, left_total, right_total, offset, limit,
                        left_complete=True, right_complete=True)
    return {
        "profile_ref": profile.profile_ref, "thread_index": thread_index,
        "thread_name": graph.thread.name,
        "baseline_start": baseline_start, "baseline_end": baseline_end,
        "target_start": target_start, "target_end": target_end,
        "baseline_first_window_id": profile.message.time_windows[baseline_start],
        "baseline_last_window_id": profile.message.time_windows[baseline_end - 1],
        "target_first_window_id": profile.message.time_windows[target_start],
        "target_last_window_id": profile.message.time_windows[target_end - 1],
        "baseline_thread_total": left_total, "target_thread_total": right_total,
        "weight_unit": _unit(profile), "complete": True,
        "comparison_basis": "self_fraction_of_thread",
        "note": "Descriptive sampled Self shares, not evidence of a causal spike.",
        **page,
    }


def compare_profiles(left: Profile, right: Profile, thread_name: str,
                     offset: int = 0, limit: int = 50) -> dict:
    """Compare complete self-weight shares for a uniquely named thread."""
    _bounds(offset, limit, max_offset=200000)
    _comparison_compatible(left, right)
    a = _thread_by_name(left, thread_name)
    b = _thread_by_name(right, thread_name)
    left_total = _weights(a.thread.times, None)
    right_total = _weights(b.thread.times, None)
    la = _aggregate_symbol_self(a, None)
    rb = _aggregate_symbol_self(b, None)
    result = _symbol_rows(la, rb, left_total, right_total, offset, limit,
                          left_complete=True, right_complete=True)
    ma, mb = left.message.metadata, right.message.metadata
    return {"left_profile_ref": left.profile_ref, "right_profile_ref": right.profile_ref,
            "thread_name": thread_name, "sampler_mode": ma.sampler_mode,
            "weight_unit": _unit(left),
            "left_capture_duration_ms": str(ma.end_time - ma.start_time),
            "right_capture_duration_ms": str(mb.end_time - mb.start_time),
            "left_ticks": ma.number_of_ticks, "right_ticks": mb.number_of_ticks,
            "left_thread_total": left_total, "right_thread_total": right_total,
            "sampling_parameters_match": ma.interval == mb.interval
            and ma.sampler_engine_version == mb.sampler_engine_version,
            "complete": True, "comparison_basis": "self_fraction_of_thread",
            "note": "Descriptive shares; capture durations and workloads may differ.",
            **result}


def compare_results(left: dict, right: dict, offset: int = 0,
                    limit: int = 50) -> dict:
    """Compare supplied symbol pages, preserving unknown values for Top-K."""
    _bounds(offset, limit, max_offset=200000)
    if not isinstance(left, dict) or not isinstance(right, dict):
        raise QueryError("INVALID_QUERY")
    required = ("group_by", "sort_by", "thread_name", "sampler_mode",
                "sampler_engine", "aggregator_type", "weight_unit",
                "thread_total", "items", "offset", "has_more", "total_groups",
                "window_index", "text_filter")
    if any(k not in obj for obj in (left, right) for k in required):
        raise QueryError("INVALID_QUERY")
    if (any(obj["group_by"] != "symbol" or obj["sort_by"] != "self" for obj in (left, right))
            or any(left[k] != right[k] for k in
                   ("thread_name", "sampler_mode", "sampler_engine",
                    "aggregator_type", "weight_unit", "text_filter"))):
        raise QueryError("PROFILE_UNSUPPORTED")
    def supplied(obj):
        items = obj["items"]
        if (type(obj["offset"]) is not int or obj["offset"] < 0
                or type(obj["has_more"]) is not bool
                or type(obj["total_groups"]) is not int or obj["total_groups"] < len(items)
                or not isinstance(items, list) or len(items) > 100
                or not isinstance(obj["thread_total"], (int, float))
                or not math.isfinite(obj["thread_total"]) or obj["thread_total"] < 0):
            raise QueryError("INVALID_QUERY")
        values = {}
        for item in items:
            if not isinstance(item, dict):
                raise QueryError("INVALID_QUERY")
            key = tuple(item.get(k) for k in ("class_name", "method_name", "method_desc"))
            weight = item.get("self")
            if (not all(isinstance(k, str) for k in key)
                    or not isinstance(weight, (int, float)) or not math.isfinite(weight)
                    or key in values):
                raise QueryError("INVALID_QUERY")
            values[key] = (weight, item.get("occurrences"))
        full = obj["offset"] == 0 and not obj["has_more"] and len(values) == obj["total_groups"]
        return values, full
    a, afull = supplied(left)
    b, bfull = supplied(right)
    # Missing entries in an incomplete Top-K are unknown, never zero.
    full = afull and bfull
    result = _symbol_rows(a, b, left["thread_total"], right["thread_total"],
                          offset, limit,
                          left_complete=afull, right_complete=bfull)
    return {"left_profile_ref": left.get("profile_ref"),
            "right_profile_ref": right.get("profile_ref"),
            "thread_name": left["thread_name"],
            "text_filter": left["text_filter"],
            "left_window_index": left["window_index"],
            "right_window_index": right["window_index"],
            "complete": full, "left_page_complete": afull,
            "right_page_complete": bfull,
            "comparison_basis": "supplied_self_fraction",
            "note": "Caller-supplied pages are not independently verified; absent Top-K rows are unknown.",
            **result}
