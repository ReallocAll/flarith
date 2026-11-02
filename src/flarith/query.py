"""Bounded, read-only Spark Protobuf queries over the R0 node pool.

All values come from the encoded SamplerData. No deobfuscation or extrapolated
causality. Offset pagination is scoped to an immutable cached Profile.
"""
from __future__ import annotations

import base64
import heapq
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


def read_fields(profile: Profile, path: list[str | int], offset: int = 0, limit: int = 50) -> dict:
    _bounds(offset, limit, max_offset=268435456)
    if not isinstance(path, list) or len(path) > 20:
        raise QueryError("INVALID_QUERY")
    value = profile.message
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


def read_raw(raw: bytes, offset: int = 0, limit: int = 16384) -> dict:
    _bounds(offset, limit, maximum=65536, max_offset=268435456)
    length = len(raw)
    chunk = raw[offset:offset + limit]
    return {"offset": offset, "total_bytes": length,
            "data_base64": base64.b64encode(chunk).decode("ascii"),
            "has_more": offset + len(chunk) < length,
            "next_offset": offset + len(chunk) if offset + len(chunk) < length else None}


def _weights(values, index: int | None) -> float:
    if index is None:
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


def _frame(graph, index: int, window_index: int | None):
    node = graph.node(index)
    inclusive = _weights(node.times, window_index)
    child_total = math.fsum(_weights(graph.node(c).times, window_index) for c in node.children_refs)
    self_weight = inclusive - child_total
    return {
        "thread_index": graph.thread_index, "node_index": index,
        "class_name": node.class_name, "method_name": node.method_name,
        "method_desc": node.method_desc, "line_number": node.line_number,
        "inclusive": inclusive, "self": self_weight,
    }


def get_call_tree(profile: Profile, thread_index: int, node_index: int | None = None,
                  offset: int = 0, limit: int = 50,
                  window_index: int | None = None) -> dict:
    _bounds(offset, limit)
    _window(profile, window_index)
    if type(thread_index) is not int or not 0 <= thread_index < len(profile.threads):
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
    selected = refs[offset:offset + limit]
    children = [_frame(graph, c, window_index) for c in selected]
    return {"profile_ref": profile.profile_ref, "node": record, "ancestors": ancestors,
            "ambiguous_path": ambiguous, "window_index": window_index,
            "children": children, "total_children": len(refs),
            "has_more": offset + len(children) < len(refs),
            "next_offset": offset + len(children) if offset + len(children) < len(refs) else None,
            "weight_unit": "bytes" if profile.message.metadata.sampler_mode == 1 else "milliseconds"}


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


def query_frames(profile: Profile, thread_index: int, sort_by: str = "self",
                 window_index: int | None = None, text: str | None = None,
                 offset: int = 0, limit: int = 50, group_by: str = "node") -> dict:
    _bounds(offset, limit, max_offset=200000 if group_by == "symbol" else 10000)
    _window(profile, window_index)
    if (type(thread_index) is not int or not 0 <= thread_index < len(profile.threads)
            or sort_by not in ("self", "inclusive")
            or group_by not in ("node", "symbol")):
        raise QueryError("INVALID_QUERY")
    if text is not None and (not isinstance(text, str) or not 1 <= len(text) <= 256):
        raise QueryError("INVALID_QUERY")
    graph = profile.threads[thread_index]
    thread_total = _weights(graph.thread.times, window_index)
    if group_by == "symbol":
        if sort_by != "self":
            # A symbol may recurse; summing nested inclusive values is wrong.
            raise QueryError("INVALID_QUERY")
        records = _aggregate_symbol_self(graph, window_index)
        if text is not None:
            records = {k: v for k, v in records.items()
                       if text in k[0] + "." + k[1]}
        # Compute only the requested page's top candidates. Ties are stable
        # by exact symbol tuple rather than dict insertion order.
        top = heapq.nsmallest(offset + limit + 1, records.items(),
                              key=lambda kv: (-kv[1][0], kv[0]))
        picked = [{"class_name": key[0], "method_name": key[1],
                   "method_desc": key[2], "self": value[0],
                   "occurrences": value[1],
                   "thread_fraction": value[0] / thread_total if thread_total else None}
                  for key, value in top[offset:offset + limit]]
        more = len(top) > offset + limit
        return {"profile_ref": profile.profile_ref, "thread_index": thread_index,
                "thread_name": graph.thread.name, "window_index": window_index,
                "sampler_mode": profile.message.metadata.sampler_mode,
                "sampler_engine": profile.message.metadata.sampler_engine,
                "aggregator_type": profile.message.metadata.data_aggregator.type,
                "sort_by": sort_by, "group_by": group_by,
                "text_filter": text, "thread_total": thread_total,
                "weight_unit": _unit(profile),
                "items": picked, "total_groups": len(records), "offset": offset,
                "has_more": more, "next_offset": offset + limit if more else None,
                "complete": offset == 0 and not more and len(picked) == len(records)}
    records = (_frame(graph, i, window_index) for i, node in enumerate(graph.thread.children)
               if text is None or text in node.class_name + "." + node.method_name)
    top = heapq.nlargest(offset + limit + 1, records,
                         key=lambda r: (r[sort_by], -r["node_index"]))
    picked = top[offset:offset + limit]
    for item in picked:
        item["thread_fraction"] = item[sort_by] / thread_total if thread_total else None
    return {"profile_ref": profile.profile_ref, "thread_index": thread_index,
            "window_index": window_index, "sort_by": sort_by, "group_by": "node",
            "thread_total": thread_total, "weight_unit": _unit(profile),
            "items": picked, "offset": offset, "has_more": len(top) > offset + limit,
            "next_offset": offset + limit if len(top) > offset + limit else None,
            "note": "Nodes may be nested; inclusive values must not be summed."}


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


def _aggregate_symbol_self(graph, window_index: int | None) -> dict:
    """Full scan of reachable pool nodes, counting each self weight once.

    Shared nodes have no unambiguous per-path attribution; do not silently
    reuse a node's weight for several paths. Recursive symbols are safe for
    self weight because individual frame self values do not overlap.
    """
    nodes = graph.thread.children
    if len(nodes) > 200000:
        raise QueryError("RESOURCE_LIMIT")
    groups = {}
    visited = set()
    pending = list(graph.roots)
    while pending:
        index = pending.pop()
        if index in visited:
            raise QueryError("PROFILE_UNSUPPORTED")
        visited.add(index)
        if len(graph.parents[index]) != 1:
            raise QueryError("PROFILE_UNSUPPORTED")
        node = nodes[index]
        pending.extend(node.children_refs)
        record = _frame(graph, index, window_index)
        key = (node.class_name, node.method_name, node.method_desc)
        if key in groups:
            weight, count = groups[key]
            groups[key] = (weight + record["self"], count + 1)
        else:
            groups[key] = (record["self"], 1)
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
