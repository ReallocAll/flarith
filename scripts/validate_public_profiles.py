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
                if profile_id == "X9ItMjOO6p" and profile.node_count != 64468:
                    raise AssertionError("large fixture node count mismatch")
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
    print(f"Live schema/cache checks: {len(PROFILE_IDS)-failed}/{len(PROFILE_IDS)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
