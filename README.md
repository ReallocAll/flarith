# Flarith

Spark Profile analysis MCP for AI agents. **0.1.0 is in development.**

Supports ChatGPT native attachments (`openai/fileParams`), Spark IDs/URLs,
content-addressed caching, Protobuf field and raw-byte reads, call-tree
navigation, symbol search, Self/Inclusive ranking, raw metrics, and
per-symbol Self-share comparisons, shallow field projection, and world entity
distribution. Source attribution is not yet available.

## Run

Requires Python 3.12+.

```sh
python -m pip install -e .
python mcp_server.py
```

Streamable HTTP is served at `/mcp` on `PORT` (default 8000).
On Prefect Horizon, select `mcp_server.py:mcp` as the server entrypoint;
project dependencies are defined in `pyproject.toml`.
Connect through an authenticated/private Horizon deployment; the server does
not itself establish an MCP caller identity or persistent access control.

`import_chatgpt_file` accepts a ChatGPT-authorized attachment.
`open_profile` accepts a ten-character Spark ID or an official
`https://spark.lucko.me/<id>` URL. Other remote URLs are not supported.
`describe_profile` and `release_profile` operate on the current instance's
cache. Query tools accept `profile_ref` with bounded `offset`/`limit` results.
After `PROFILE_NOT_FOUND`, import again from the original source.

`read_raw` defaults to a 2 MiB raw-byte page and accepts up to 4 MiB per
request. It returns one MCP text item (no duplicate structured content) with
Base64 data and an MCP response-size estimate, bounded strictly below 4 MiB
including an 8 KiB transport allowance. If needed it returns fewer source
bytes automatically. Use `next_request` when `has_more` is true to fetch
the next page; `returned_bytes` is the exact source-byte count, while
`mcp_response_bytes` is the calculated JSON-RPC frame size with a conservative
request ID, not a measurement of transport-specific SSE framing.

`query_entity_distribution(profile_ref, entity_type, world_index=None,
limit=10, offset=0)` ranks world/chunk entity counts in the recorded world
statistics snapshot. It returns both the all-world `global_entity_count` and
the sum `observed_chunk_entity_count` of scanned chunk records, plus scanned
coverage and duplicate record counts. These values may differ; selecting a
world does not narrow the global count. The data is not a time series or proof
that an entity type caused a sampled hotspot.

`read_fields(..., fields=["x", "z", "total_entities", "entity_counts"])`
projects up to eight scalar or scalar-map fields from a Protobuf message or
repeated-message page. It does not recursively expand nested messages;
non-projected reads are unchanged. Projection results have a 256 KiB JSON
response budget and a 256-entry per-map limit; oversize results return
`RESOURCE_LIMIT` rather than silently truncating.
Self/Inclusive weights match Spark's node calculations; nested inclusive
weights must not be added together.

Default limits: 128 MiB transferred, 768 MiB estimated single-profile
residency and 896 MiB process RSS. Memory preflight is **best-effort**, not an
OS-level OOM guarantee. Large files and native attachment imports still
require real Horizon validation.

## Development

```sh
python -m pip install -e ".[dev]"
pytest -q
```

Protobuf schema is pinned from
[ReallocAll/spark](https://github.com/ReallocAll/spark/tree/2656b5315d17815fdabb420aa26c33d0eafea69f/proto/spark).
The generated Python bindings are committed; regeneration uses
`python scripts/generate_proto.py`.

## License

Flarith's license has not been selected. Upstream schema obligations are
recorded in [THIRD_PARTY.md](THIRD_PARTY.md). The repository remains private.
