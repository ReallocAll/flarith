# Flarith

Flarith is a Spark Profile analysis MCP server for AI agents. **0.1.0 is in development.** The deployment target is **Prefect Horizon Free (personal use)**, not a multi-tenant MCP service.

It supports Spark IDs/URLs and ChatGPT native attachments (`openai/fileParams`), call-tree navigation, Self/Inclusive ranking, time-range comparisons, metrics, world entity distribution, and bounded Protobuf reads. Source attribution is not implemented.

## Deploy to Horizon Free

Requires Python 3.12+.

1. Create a Horizon server from this GitHub repository.
2. Set the server entrypoint to `mcp_server.py:mcp`; dependencies are in `pyproject.toml`.
3. Connect your AI client to the authenticated Horizon MCP endpoint (`/mcp`).

Horizon handles hosting and caller authentication. Flarith has no user accounts, per-caller Profile isolation, or persistent storage. Use it as a personal service with trusted, authenticated clients.

## Import and cache

- `open_profile` accepts a ten-character Spark ID or an official `https://spark.lucko.me/<id>` URL, not arbitrary remote URLs.
- `import_chatgpt_file` accepts a ChatGPT-authorized attachment via `openai/fileParams` and supports an optional transport `expected_sha256` check.
- The `profile_ref` is a SHA-256 identity of **decoded** Protobuf bytes, not a durable storage handle or authentication credential. `describe_profile` and `release_profile` operate on the current process's cache.
- A restart, Horizon scale-to-zero event, eviction, or request routed to another instance may cause `PROFILE_NOT_FOUND`. Import again from the original attachment or Spark ID/URL; do not depend on cache persistence or instance affinity.

## Analysis and interpretation

- `get_call_tree` navigates child nodes in original or descending Self/Inclusive order; `search_symbols` finds symbol nodes. Self = Inclusive − direct-child Inclusive. **Never sum nested Inclusive weights.**
- `query_frames` ranks nodes or groups Self by symbol/class. Grouped Self is computed from reachable stack nodes before pagination. `occurrences` counts distinct stack nodes, not runtime calls. A `text` filter limits returned groups, but fractions retain the full unfiltered thread denominator. Its `complete` flag means the returned page includes every matching group.
- For time ranges, pass both `start_window` (inclusive) and `end_window` (exclusive) as zero-based indices instead of `window_index`. `compare_time_ranges` compares fully aggregated Self fractions across two ranges. `compare_profiles` compares a uniquely named thread in compatible Profiles. Result pagination does not make their underlying aggregation partial.
- `compare_results` compares caller-supplied symbol pages. Symbols missing from an incomplete Top-K page are **unknown**, not zero; supplied pages are not independently verified.
- `query_subtree_hotspots` ranks Self within one subtree; `subtree_fraction` uses that subtree's Inclusive weight as its denominator. `example_node_index` supports navigation.
- `query_metrics` returns recorded statistics without interpolation. Sampled weights describe where samples appeared, not the cause of an MSPT spike; sleeping/waiting can appear in execution samples.

`query_entity_distribution` ranks a snapshot of entity counts by world and chunk. `global_entity_count` covers all worlds even when selecting `world_index`; `observed_chunk_entity_count` sums scanned chunk records and may differ. Neither is a time series or proof of causation.

`describe_schema` and `read_fields` expose Protobuf data. Shallow projection accepts up to eight scalar or scalar-map fields and has a 256 KiB JSON budget and 256-entry per-map limit. Oversize projections return `RESOURCE_LIMIT` rather than truncation.

`read_raw` returns Base64 Protobuf pages (default 2 MiB, requested maximum 4 MiB). Pages shrink automatically to keep the **estimated** MCP JSON-RPC frame below 4 MiB, including an 8 KiB allowance. Follow `next_request` while `has_more`; `returned_bytes` is exact, but `mcp_response_bytes` is an estimate, not measured transport framing.

## Resource limits

Implementation defaults: 128 MiB downloaded input, 256 MiB decoded Protobuf, 768 MiB estimated single-Profile residency, and 896 MiB process RSS. **These are Flarith defaults, not verified Horizon Free quotas.** Memory preflight is best-effort, not an OS-level OOM guarantee. Validate large imports on the effective Horizon runtime before changing the limits.

## Development and verification

```sh
python -m pip install -e ".[dev]"
pytest -q
fastmcp inspect mcp_server.py:mcp
```

For local testing, `python mcp_server.py` starts Streamable HTTP on `0.0.0.0:PORT/mcp` (default `PORT=8000`). This local entrypoint has **no caller authentication**; do not expose it publicly.

CI runs unit tests and checks generated Protobuf bindings on push and pull request. Optional `workflow_dispatch` runs `scripts/validate_public_profiles.py` with public Spark Profiles; passing unit tests alone does not establish current Horizon deployment success.

Protobuf schema is pinned from [ReallocAll/spark](https://github.com/ReallocAll/spark/tree/2656b5315d17815fdabb420aa26c33d0eafea69f/proto/spark). Generated Python bindings are committed; regenerate with `python scripts/generate_proto.py`.

## License

The repository is public, but Flarith has not selected a project license. See [THIRD_PARTY.md](THIRD_PARTY.md) for upstream provenance and unresolved licensing considerations.
