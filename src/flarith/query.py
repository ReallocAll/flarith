"""Bounded, read-only Spark Protobuf queries over the R0 node pool.

All values come from the encoded SamplerData. No deobfuscation or extrapolated
causality. Offset pagination is scoped to an immutable cached Profile.
"""
from __future__ import annotations

import base64
import heapq
import math
from itertools import islice

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
            "type": f.message_type.full_name if f.message_type else f.enum_type.full_name if f.enum_type else f.type,
            "has_presence": f.has_presence,
        })
    return {"message_type": descriptor.full_name, "fields": fields}


def read_fields(profile: Profile, path: list[str | int], offset: int = 0, limit: int = 50) -> dict:
    _bounds(offset, limit)
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
        values = [_encode(v, field) for v in islice(value, offset, offset + limit)]
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
                 offset: int = 0, limit: int = 50) -> dict:
    _bounds(offset, limit)
    _window(profile, window_index)
    if type(thread_index) is not int or not 0 <= thread_index < len(profile.threads) or sort_by not in ("self", "inclusive"):
        raise QueryError("INVALID_QUERY")
    if text is not None and (not 1 <= len(text) <= 256):
        raise QueryError("INVALID_QUERY")
    graph = profile.threads[thread_index]
    records = (_frame(graph, i, window_index) for i, node in enumerate(graph.thread.children)
               if text is None or text in node.class_name + "." + node.method_name)
    top = heapq.nlargest(offset + limit + 1, records,
                         key=lambda r: (r[sort_by], -r["node_index"]))
    picked = top[offset:offset + limit]
    thread_total = _weights(graph.thread.times, window_index)
    for item in picked:
        item["thread_fraction"] = item[sort_by] / thread_total if thread_total else None
    return {"profile_ref": profile.profile_ref, "thread_index": thread_index,
            "window_index": window_index, "sort_by": sort_by, "thread_total": thread_total,
            "weight_unit": "bytes" if profile.message.metadata.sampler_mode == 1 else "milliseconds",
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
        rows = [{"index": i, "value": _encode(values[i]),
                 "timestamp_delta_ms": deltas[i] if i < len(deltas) else None}
                for i in range(offset, min(offset + limit, total))]
    return {"profile_ref": profile.profile_ref, "series": series,
            "start_timestamp_ms": str(getattr(data.metadata.metrics, series).start_timestamp_ms) if series != "windows" else None,
            "total": total, "offset": offset, "items": rows,
            "has_more": offset + len(rows) < total,
            "next_offset": offset + len(rows) if offset + len(rows) < total else None}
