"""Conservative, calibration-pending Spark parse cost estimates.

The wire preflight reads field boundaries, not a second decoded object graph.
Coefficients are safety-side heuristics, NOT validated OOM guarantees.
"""

from dataclasses import dataclass

MIB = 1024 * 1024
SMALL_PROFILE_BYTES = 8 * MIB


@dataclass(frozen=True, slots=True)
class MemoryEstimate:
    resident_bytes: int
    parse_peak_additional_bytes: int
    nodes: int | None = None
    time_elements: int | None = None
    references: int | None = None
    string_bytes: int | None = None


def _varint(data: memoryview, pos: int, end: int) -> tuple[int, int]:
    value = 0
    for shift in range(0, 70, 7):
        if pos == end:
            raise ValueError("truncated protobuf varint")
        b = data[pos]
        pos += 1
        value |= (b & 127) << shift
        if not b & 128:
            return value, pos
    raise ValueError("oversized protobuf varint")


def _fields(data: memoryview, start: int, end: int):
    pos = start
    while pos < end:
        tag, pos = _varint(data, pos, end)
        number, kind = tag >> 3, tag & 7
        if number == 0:
            raise ValueError("invalid protobuf field number")
        value_start = pos
        if kind == 0:
            _, pos = _varint(data, pos, end)
        elif kind == 1:
            pos += 8
        elif kind == 2:
            length, pos = _varint(data, pos, end)
            value_start = pos
            pos += length
        elif kind == 5:
            pos += 4
        else:
            raise ValueError("unsupported protobuf wire type")
        if pos > end:
            raise ValueError("protobuf field extends beyond message")
        yield number, kind, value_start, pos


def _count_doubles(kind: int, start: int, end: int) -> int:
    if kind == 1:
        return 1
    if kind == 2 and (end - start) % 8 == 0:
        return (end - start) // 8
    raise ValueError("malformed packed doubles")


def _count_ints(data: memoryview, kind: int, start: int, end: int) -> int:
    if kind == 0:
        return 1
    if kind != 2:
        raise ValueError("malformed integer references")
    count = 0
    while start < end:
        _, start = _varint(data, start, end)
        count += 1
    return count


def estimate_profile(payload: bytes) -> MemoryEstimate:
    size = len(payload)
    if size <= SMALL_PROFILE_BYTES:
        resident = max(size * 6, size + 8 * MIB)
        return MemoryEstimate(resident, max(resident - size, 0) + max(size, 8 * MIB))

    data = memoryview(payload)
    nodes = times = references = strings = 0
    for field, wire, start, end in _fields(data, 0, size):
        if field != 2 or wire != 2:  # SamplerData.threads
            continue
        for tf, tw, ts, te in _fields(data, start, end):
            if tf == 4:  # ThreadNode.times
                times += _count_doubles(tw, ts, te)
            elif tf == 5:  # thread roots
                references += _count_ints(data, tw, ts, te)
            elif tf == 3 and tw == 2:  # ThreadNode.children: node pool
                nodes += 1
                for nf, nw, ns, ne in _fields(data, ts, te):
                    if nf == 8:
                        times += _count_doubles(nw, ns, ne)
                    elif nf == 9:
                        references += _count_ints(data, nw, ns, ne)
                    elif nf in (3, 4, 7) and nw == 2:
                        strings += ne - ns

    # The wire size pays for retained bytes and unknown metadata. Additional
    # costs cover protobuf objects and the R0 parent index (not a copied tree).
    resident = max(
        size * 2,
        size + MIB + nodes * 768 + times * 16 + references * 40 + strings,
    )
    return MemoryEstimate(
        resident_bytes=resident,
        parse_peak_additional_bytes=resident - size + max(size, 8 * MIB),
        nodes=nodes,
        time_elements=times,
        references=references,
        string_bytes=strings,
    )
