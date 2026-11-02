import gzip
import threading

import pytest

from flarith import Profile
from flarith.cache import (
    Limits, ProfileCache, ProfileInUseError, ProfileNotFoundError, ResourceLimitError,
)
from flarith.memory import MIB, estimate_profile
from flarith.proto.spark import spark_sampler_pb2


def fixture_bytes(name="one"):
    obj = spark_sampler_pb2.SamplerData()
    obj.metadata.comment = name
    obj.threads.add().name = "main"
    return obj.SerializeToString()


def test_stable_content_identity_and_gzip_equivalence():
    raw = fixture_bytes()
    cache = ProfileCache(rss_reader=lambda: 50 * MIB)
    ref = cache.load(raw)
    assert cache.load(gzip.compress(raw)) == ref
    assert cache.references() == (ref,)
    with cache.borrow(ref) as loaded:
        assert isinstance(loaded.profile, Profile)
        assert loaded.raw == raw
        assert loaded.profile.profile_ref == ref
    assert cache.release(ref)
    assert not cache.release(ref)
    with pytest.raises(ProfileNotFoundError, match="PROFILE_NOT_FOUND"):
        with cache.borrow(ref):
            pass
    assert cache.load(raw) == ref


def test_lru_reclaims_before_refusing_and_most_recent_is_protected():
    cache = None

    def rss():
        return 57 * MIB + cache.estimated_bytes

    cache = ProfileCache(
        Limits(max_input_mib=1, max_profile_memory_mib=20, max_process_rss_mib=85),
        rss_reader=rss,
    )
    a, b, c = (cache.load(fixture_bytes(n)) for n in ("a", "b", "c"))
    assert a != b != c
    assert cache.references() == (b, c)  # first evicted
    with cache.borrow(b):
        pass  # hit moves B to end
    d = cache.load(fixture_bytes("d"))
    assert cache.references() == (b, d)
    with pytest.raises(ProfileNotFoundError):
        with cache.borrow(c):
            pass


def test_active_read_never_evicts_and_exception_releases_lease():
    cache = None
    def rss():
        return 57 * MIB + cache.estimated_bytes
    cache = ProfileCache(
        Limits(max_input_mib=1, max_profile_memory_mib=20, max_process_rss_mib=85),
        rss_reader=rss,
    )
    a = cache.load(fixture_bytes("a"))
    b = cache.load(fixture_bytes("b"))
    with pytest.raises(ValueError):
        with cache.borrow(a):
            with pytest.raises(ProfileInUseError):
                cache.release(a)
            cache.load(fixture_bytes("c"))  # B should be evicted
            assert cache.references()[0] == a
            raise ValueError("cancelled")
    assert cache.release(a)
    with pytest.raises(ProfileNotFoundError):
        with cache.borrow(b):
            pass


def test_all_pinned_rejects_without_evicting():
    cache = None
    def rss():
        return 57 * MIB + cache.estimated_bytes
    cache = ProfileCache(
        Limits(max_input_mib=1, max_profile_memory_mib=20, max_process_rss_mib=85),
        rss_reader=rss,
    )
    a = cache.load(fixture_bytes("a"))
    b = cache.load(fixture_bytes("b"))
    with cache.borrow(a), cache.borrow(b):
        with pytest.raises(ResourceLimitError, match="headroom"):
            cache.load(fixture_bytes("c"))
    assert set(cache.references()) == {a, b}


def test_limit_rejection_does_not_mutate_cache(monkeypatch):
    cache = ProfileCache(
        Limits(max_input_mib=1, max_profile_memory_mib=20, max_process_rss_mib=50),
        rss_reader=lambda: 50 * MIB,
    )
    with pytest.raises(ResourceLimitError, match="headroom"):
        cache.load(fixture_bytes())
    assert cache.references() == ()
    with pytest.raises(ResourceLimitError, match="input size"):
        cache.load(b"x" * (MIB + 1))
    with pytest.raises(ResourceLimitError, match="decoded-size"):
        cache.load(gzip.compress(b"a" * (256 * MIB + 1)))
    assert cache.references() == ()


def test_invalid_gzip_and_protobuf_rejected():
    cache = ProfileCache(rss_reader=lambda: 50 * MIB)
    from flarith.core import InvalidProfileError
    with pytest.raises(InvalidProfileError):
        cache.load(b"\x1f\x8bnot-gzip")
    with pytest.raises(InvalidProfileError):
        cache.load(b"not-spark")


def test_structural_wire_preflight_distinguishes_node_and_packed_time_profiles():
    packed = spark_sampler_pb2.SamplerData()
    packed.metadata.start_time = 1
    t = packed.threads.add()
    t.times.extend([1.] * 1_200_000)  # >8 MiB, compact times
    seq = packed.SerializeToString()
    stats = estimate_profile(seq)
    assert stats.time_elements == 1_200_000
    assert stats.nodes == 0
    assert stats.resident_bytes >= len(seq)
    assert stats.parse_peak_additional_bytes > 0
    assert estimate_profile(fixture_bytes()).nodes is None


def test_serialized_concurrent_identical_loads():
    cache = ProfileCache(rss_reader=lambda: 50 * MIB)
    raw = fixture_bytes()
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as pool:
        refs = list(pool.map(cache.load, [raw] * 10))
    assert len(set(refs)) == 1
    assert len(cache.references()) == 1
