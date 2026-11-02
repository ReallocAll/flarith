# Flarith

Spark Profile analysis MCP for AI agents. **0.1.0 is in development.**

Supports ChatGPT native attachments (`openai/fileParams`), Spark IDs/URLs,
content-addressed caching, Protobuf field and raw-byte reads, call-tree
navigation, symbol search, per-node Self/Inclusive ranking, and raw metrics.
Cross-profile comparisons and aggregated source attribution are not yet available.

## Run

Requires Python 3.12+.

```sh
python -m pip install -e .
python mcp_server.py
```

Streamable HTTP is served at `/mcp` on `PORT` (default 8000).
Connect through an authenticated/private Horizon deployment; the server does
not itself establish an MCP caller identity or persistent access control.

`import_chatgpt_file` accepts a ChatGPT-authorized attachment.
`open_profile` accepts a ten-character Spark ID or an official
`https://spark.lucko.me/<id>` URL. Other remote URLs are not supported.
`describe_profile` and `release_profile` operate on the current instance's
cache. Query tools accept `profile_ref` with bounded `offset`/`limit` results.
After `PROFILE_NOT_FOUND`, import again from the original source.
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
