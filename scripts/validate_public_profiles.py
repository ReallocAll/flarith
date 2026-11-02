"""Optional live regression against eight previously verified public Spark IDs.

No sample bytes, symbols, metadata or attachment URLs are retained in Git.
This is an external-network integration check, not a deterministic unit test.
"""

import sys
import urllib.request
import zlib

from flarith import Profile

PROFILE_IDS = (
    "adJC232qE2", "E9OaJxLCyZ", "uwpd8wXBy1", "ybTNGYFtST",
    "G4LQMFYvZ1", "9ORLFk9hlU", "0ikjjUiO00", "t71w07Zdjh",
)
COMPRESSED_LIMIT = 24 * 1024 * 1024
DECODED_LIMIT = 96 * 1024 * 1024


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file, code, message, headers, url):
        return None


def download(profile_id: str) -> bytes:
    url = "https://spark-usercontent.lucko.me/" + profile_id
    request = urllib.request.Request(
        url, headers={"Accept": "application/x-spark-sampler", "Accept-Encoding": "identity"}
    )
    with urllib.request.build_opener(NoRedirect()).open(request, timeout=15) as response:
        parts = []
        size = 0
        while chunk := response.read(65536):
            size += len(chunk)
            if size > COMPRESSED_LIMIT:
                raise ValueError("download exceeds public-fixture limit")
            parts.append(chunk)
    payload = b"".join(parts)
    if payload.startswith(b"\x1f\x8b"):
        decoder = zlib.decompressobj(zlib.MAX_WBITS | 16)
        decoded = decoder.decompress(payload, DECODED_LIMIT + 1)
        if len(decoded) > DECODED_LIMIT or decoder.unconsumed_tail:
            raise ValueError("decompression exceeds public-fixture limit")
        decoded += decoder.flush(DECODED_LIMIT + 1 - len(decoded))
        if len(decoded) > DECODED_LIMIT or not decoder.eof or decoder.unused_data:
            raise ValueError("invalid gzip fixture")
        return decoded
    return payload


def main() -> int:
    failed = 0
    for profile_id in PROFILE_IDS:
        try:
            profile = Profile.from_bytes(download(profile_id))
            roots = sum(len(graph.roots) for graph in profile.threads)
            print(
                f"{profile_id}: OK threads={len(profile.threads)} "
                f"nodes={profile.node_count} root_refs={roots} "
                f"windows={len(profile.message.time_windows)}"
            )
        except Exception as exc:
            failed += 1
            print(f"{profile_id}: FAIL {type(exc).__name__}: {exc}", file=sys.stderr)
    print(f"Live schema/graph checks: {len(PROFILE_IDS)-failed}/{len(PROFILE_IDS)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
