"""Process-local memory-aware LRU for Spark Profile objects.

No persistence or source-history tracking. Admission is conservative: RSS
may stay elevated after eviction and Python cannot enforce a hard RSS ceiling.
"""

import gc
import hashlib
import os
import threading
import zlib
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Iterator

from flarith.core import InvalidProfileError, Profile
from flarith.memory import MIB, MemoryEstimate, estimate_profile

# This is a provisional decoded-size safety bound, not a promise that every
# 256 MiB protobuf will fit under the configured process budget.
MAX_DECODED_BYTES = 256 * MIB


class ResourceLimitError(ValueError):
    """Input size, predicted residency or measured RSS exceeds budget."""


class ProfileNotFoundError(KeyError):
    """Not present in this process (including after restart or eviction)."""


class ProfileInUseError(RuntimeError):
    """Cannot explicitly release a Profile currently leased by a query."""


@dataclass(frozen=True, slots=True)
class Limits:
    max_input_mib: int = 128
    max_profile_memory_mib: int = 768
    max_process_rss_mib: int = 896

    def __post_init__(self) -> None:
        if not (0 < self.max_input_mib and 0 < self.max_profile_memory_mib
                < self.max_process_rss_mib):
            raise ValueError("invalid memory limits")


@dataclass(slots=True)
class CachedProfile:
    profile: Profile
    raw: bytes
    estimated_bytes: int
    active: int = 0


def process_rss_bytes() -> int:
    """Read current Linux RSS; fail closed if not observable."""
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            resident_pages = int(statm.read().split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError) as exc:
        raise ResourceLimitError("current process RSS is unavailable") from exc


def _unzip(raw: bytes, reserve: Callable[[int], None]) -> bytes:
    """Inflate incrementally with bounded output chunks and RSS reservations.

    The reserve callback may evict inactive cache entries during inflation.
    Reserving future concatenation space reduces transient duplicate buffers;
    this remains an application-level best-effort bound, not an OS limit.
    """
    if not raw.startswith(b"\x1f\x8b"):
        if len(raw) > MAX_DECODED_BYTES:
            raise ResourceLimitError("decoded protobuf exceeds limit")
        return raw

    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    parts: list[bytes] = []
    total = 0
    try:
        for offset in range(0, len(raw), 65536):
            if decoder.eof:
                raise InvalidProfileError("trailing gzip data")
            pending = raw[offset:offset + 65536]
            while pending:
                # Never request more than 4 MiB of inflation at a time.
                allowance = min(4 * MIB, MAX_DECODED_BYTES + 1 - total)
                reserve(total + allowance + 8 * MIB)
                chunk = decoder.decompress(pending, allowance)
                total += len(chunk)
                if total > MAX_DECODED_BYTES:
                    raise ResourceLimitError("gzip expands beyond decoded-size limit")
                if chunk:
                    parts.append(chunk)
                remaining = decoder.unconsumed_tail
                if decoder.eof:
                    if decoder.unused_data or remaining or offset + 65536 < len(raw):
                        raise InvalidProfileError("trailing gzip data")
                    break
                if remaining == pending and not chunk:
                    raise InvalidProfileError("gzip decoder made no progress")
                pending = remaining
        tail = decoder.flush(MAX_DECODED_BYTES + 1 - total)
    except zlib.error as exc:
        raise InvalidProfileError("invalid gzip profile") from exc
    total += len(tail)
    if total > MAX_DECODED_BYTES:
        raise ResourceLimitError("gzip expands beyond decoded-size limit")
    if not decoder.eof:
        raise InvalidProfileError("incomplete gzip profile")
    parts.append(tail)
    reserve(total + 8 * MIB)  # joining immutable chunks allocates new bytes
    return b"".join(parts)


class ProfileCache:
    """Serialize loading, permit concurrent read leases, evict unpinned LRU."""

    def __init__(
        self,
        limits: Limits = Limits(),
        *,
        rss_reader: Callable[[], int] = process_rss_bytes,
    ) -> None:
        self.limits = limits
        self._rss_reader = rss_reader
        self._lock = threading.RLock()
        self._entries: OrderedDict[str, CachedProfile] = OrderedDict()

    @property
    def estimated_bytes(self) -> int:
        with self._lock:
            return sum(entry.estimated_bytes for entry in self._entries.values())

    def _evict_one(self) -> bool:
        for key, entry in self._entries.items():
            if entry.active:
                continue
            del self._entries[key]
            del entry
            gc.collect()
            return True
        return False

    def _reserve_bytes(self, additional: int) -> None:
        cap = self.limits.max_process_rss_mib * MIB
        while self._rss_reader() + additional > cap:
            if not self._evict_one():
                raise ResourceLimitError("insufficient measured RSS headroom")

    def _admit(self, estimate: MemoryEstimate) -> None:
        if estimate.resident_bytes > self.limits.max_profile_memory_mib * MIB:
            raise ResourceLimitError("predicted profile residency exceeds single-profile budget")
        self._reserve_bytes(estimate.parse_peak_additional_bytes)

    def load(self, raw: bytes) -> str:
        """Load an in-memory file. R2 will supply bounded streamed sources.

        The caller owns the input buffer and it is already counted in RSS.
        The decoded raw bytes are retained to support future read_raw queries.
        """
        if not isinstance(raw, bytes):
            raise TypeError("profile input must be bytes")
        if not raw or len(raw) > self.limits.max_input_mib * MIB:
            raise ResourceLimitError("input size exceeds configured limit")
        with self._lock:  # blocks concurrent decoders to prevent stacked peaks
            # Decompression has a separate hard byte limit and is performed
            # before preflight, so gigantic gzip envelopes cannot bypass it.
            decoded = _unzip(raw, self._reserve_bytes)
            reference = "sha256:" + hashlib.sha256(decoded).hexdigest()
            if reference in self._entries:
                self._entries.move_to_end(reference)
                return reference

            try:
                estimate = estimate_profile(decoded)
            except ValueError as exc:
                raise InvalidProfileError("invalid protobuf wire structure") from exc
            self._admit(estimate)
            rss_before = self._rss_reader()
            profile = Profile.from_bytes(decoded)
            rss_after = self._rss_reader()
            if rss_after > self.limits.max_process_rss_mib * MIB:
                raise ResourceLimitError("measured RSS above process budget after decoding")
            # RSS deltas are not a complete memory ledger. Keep the
            # conservative estimate as cache cost; export measurements
            # separately instead of treating allocator RSS as object size.
            self._entries[reference] = CachedProfile(
                profile=profile,
                raw=decoded,
                estimated_bytes=max(estimate.resident_bytes, rss_after - rss_before + len(decoded)),
            )
            return reference

    @contextmanager
    def borrow(self, reference: str) -> Iterator[CachedProfile]:
        with self._lock:
            entry = self._entries.get(reference)
            if entry is None:
                raise ProfileNotFoundError("PROFILE_NOT_FOUND")
            entry.active += 1
            self._entries.move_to_end(reference)
        try:
            yield entry
        finally:
            with self._lock:
                entry.active -= 1

    def release(self, reference: str) -> bool:
        """Explicit release returns False for a missing reference."""
        with self._lock:
            entry = self._entries.get(reference)
            if entry is None:
                return False
            if entry.active:
                raise ProfileInUseError("profile is in use")
            del self._entries[reference]
            return True

    def references(self) -> tuple[str, ...]:
        """Snapshot of locally cached references in LRU order."""
        with self._lock:
            return tuple(self._entries)
