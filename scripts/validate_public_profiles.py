"""Optional real Spark regression for parser, R1 cache and memory observations.

Public IDs only; no source bytes or credentials persisted in CI artifacts.
RSS and process HWM are observational, not per-profile isolated allocations.
"""

import resource
import sys
import urllib.request

from flarith.cache import ProfileCache, process_rss_bytes

PROFILE_IDS = (
    "adJC232qE2", "E9OaJxLCyZ", "uwpd8wXBy1", "ybTNGYFtST",
    "G4LQMFYvZ1", "9ORLFk9hlU", "0ikjjUiO00", "t71w07Zdjh",
    "X9ItMjOO6p",  # large, time-series-heavy public fixture (~35 MiB)
)
MAX_TRANSFER = 128 * 1024 * 1024


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file, code, message, headers, url):
        return None


def download(profile_id: str) -> bytes:
    request = urllib.request.Request(
        "https://spark-usercontent.lucko.me/" + profile_id,
        headers={
            "Accept": "application/x-spark-sampler",
            "Accept-Encoding": "identity",
            "User-Agent": "Flarith/0.1",
        },
    )
    with urllib.request.build_opener(NoRedirect()).open(request, timeout=25) as response:
        parts = []
        size = 0
        while chunk := response.read(65536):
            size += len(chunk)
            if size > MAX_TRANSFER:
                raise ValueError("download exceeds public-fixture limit")
            parts.append(chunk)
    return b"".join(parts)


def main() -> int:
    failed = 0
    cache = ProfileCache()
    for profile_id in PROFILE_IDS:
        try:
            raw = download(profile_id)
            before = process_rss_bytes()
            reference = cache.load(raw)
            with cache.borrow(reference) as loaded:
                profile = loaded.profile
                roots = sum(len(g.roots) for g in profile.threads)
                print(
                    f"{profile_id}: OK bytes={len(raw)} threads={len(profile.threads)} "
                    f"nodes={profile.node_count} root_refs={roots} "
                    f"windows={len(profile.message.time_windows)}"
                )
                if profile_id == "X9ItMjOO6p":
                    if profile.node_count != 64468:
                        raise AssertionError("large fixture node count mismatch")
                    from flarith.query import query_frames, query_metrics
                    top = query_frames(profile, 0, sort_by="self", limit=3)
                    assert top["items"] and top["weight_unit"] == "milliseconds"
                    windows = query_metrics(profile, "windows", limit=3)
                    assert windows["total"] == 61 and len(windows["items"]) == 3
                    print(f"R3 large query: OK top_self={top['items'][0]['self']:.2f}")
                    from flarith.query import compare_profiles
                    grouped = query_frames(profile, 0, sort_by="self",
                                           group_by="symbol", limit=10)
                    assert grouped["items"] and grouped["total_groups"] >= 10
                    same = compare_profiles(profile, profile, profile.threads[0].thread.name,
                                            limit=10)
                    assert same["complete"] and same["total_symbols"] == grouped["total_groups"]
                    assert all(row["delta_fraction"] == 0.0 for row in same["items"])
                    print(
                        f"R3 large symbol comparison: OK symbols={same['total_symbols']} "
                        f"top_self={grouped['items'][0]['self']:.2f}"
                    )
            assert cache.load(raw) == reference
            print(
                f"  rss_before_mib={before / 1048576:.2f} "
                f"rss_after_mib={process_rss_bytes() / 1048576:.2f} "
                f"process_hwm_mib={resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.2f} "
                f"cache_estimated_mib={cache.estimated_bytes / 1048576:.2f}"
            )
        except Exception as exc:
            failed += 1
            print(f"{profile_id}: FAIL {type(exc).__name__}: {exc}", file=sys.stderr)
    # Exercise the actual R2 HTTPS connector, not only the raw R1 cache.
    try:
        from flarith.mcp import open_profile, describe_profile, release_profile
        imported = open_profile("adJC232qE2")
        assert imported["ok"], imported.get("error", {}).get("code")
        ref = imported["profile_ref"]
        assert describe_profile(ref)["node_count"] == 4479
        assert release_profile(ref)["ok"]
        assert describe_profile(ref)["error"]["code"] == "PROFILE_NOT_FOUND"
        restored = open_profile("https://spark.lucko.me/adJC232qE2")
        assert restored["profile_ref"] == ref
        print("R2 official Spark HTTPS import and cache recovery: OK")
        from flarith.mcp import (
            describe_schema, read_fields, get_call_tree, search_symbols,
            query_frames, query_metrics, read_raw,
        )
        schema = describe_schema()
        assert schema["ok"] and any(f["name"] == "time_window_statistics" for f in schema["fields"])
        start = read_fields(ref, ["metadata", "start_time"])
        assert start["ok"] and isinstance(start["value"], str)
        roots = get_call_tree(ref, 0, limit=10)
        assert roots["ok"] and roots["total_children"] > 0
        symbol = search_symbols(ref, "tick", limit=5)
        assert symbol["ok"] and "total_matches" in symbol
        ranked = query_frames(ref, 0, sort_by="inclusive", limit=3)
        assert ranked["ok"] and ranked["items"]
        window = query_metrics(ref, "windows", limit=2)
        assert window["ok"] and window["items"]
        raw_window = read_raw(ref, offset=0, limit=32)
        assert raw_window["ok"] and raw_window["data_base64"]
        print(
            f"R3 public query checks: OK roots={roots['total_children']} "
            f"top_inclusive={ranked['items'][0]['inclusive']:.2f} "
            f"thread_total={ranked['thread_total']:.2f} "
            f"window_stats={len(window['items'])}"
        )
    except Exception as exc:
        failed += 1
        print(f"R2 connector: FAIL {type(exc).__name__}: {str(exc)[:80]}", file=sys.stderr)
    print(f"Live schema/cache checks: {len(PROFILE_IDS)-failed}/{len(PROFILE_IDS)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
