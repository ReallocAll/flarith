# Flarith

Spark Profile analysis for AI agents. Version 0.1.0 is under development.

## R0: Protobuf and call-graph correctness

The current code parses Spark `SamplerData` using schema definitions pinned from
[ReallocAll/spark at 2656b531](https://github.com/ReallocAll/spark/tree/2656b5315d17815fdabb420aa26c33d0eafea69f/proto/spark).
Unlike the legacy viewer schema, `ThreadNode.children` is the **node pool**,
and `children_refs` holds graph edges.

```sh
python -m pip install -e ".[dev]"
python scripts/generate_proto.py
pytest -q
```

Generated `*_pb2.py` modules are committed to source. `grpcio-tools` is
needed only to regenerate them, not at runtime. R0 provides raw weights and
call-graph traversal, **not** interpreted Self / Inclusive accounting.
No network file import or MCP endpoint is implemented in this phase.

## R1: bounded in-process caching

`ProfileCache` accepts bytes from a trusted, already-bounded input source.
It normalizes raw/gzip SamplerData to a content-derived `sha256:` reference,
caches retained source bytes and decoded Protobuf, and uses a fixed LRU policy.
Queries must use `with cache.borrow(ref) as entry:` for eviction-safe access.
Missing references raise `ProfileNotFoundError`; an active lease prevents
both automatic eviction and explicit release.

Defaults are 128 MiB maximum transferred input, 768 MiB estimated single
Profile residency and 896 MiB total process RSS. Expanded gzip has a
*provisional* 256 MiB byte bound. Files <=8 MiB use a conservative quick
estimate; larger files use a bounded Protobuf wire-structure preflight to
count stack nodes, packed times, edge references and symbol bytes. The preflight
is **not** an alternative decoder. Loading is serialized.

**Safety boundary:** Python Protobuf allocations and gzip inflation can spike
between RSS checks. The coefficients and extra buffer estimates have not been
calibrated near the Horizon memory limit. Thus RSS gating is best-effort
admission, **not an OS-enforced hard memory ceiling**. RSS may remain high after
cache eviction; the loader conservatively refuses when it cannot measure
headroom. Final Horizon admission validation remains necessary.

To run the optional real public fixture check (requires outbound network):

```sh
python scripts/validate_public_profiles.py
```

The same check runs only on manually dispatched GitHub Actions CI, not on
every PR. It prints aggregate memory observations only; no fixture bytes
are checked into the repository.

## Roadmap

- R0: complete Schema parsing, stable node identity, safe reference graph
- R1: memory-aware loading and LRU
- R2: native ChatGPT file attachment and Spark URL import
- R3: agent-oriented schema, fields, symbols, metrics and comparisons
- R4: real Horizon end-to-end validation

## License status

Flarith's own license is **not selected yet**. The included Spark Protobuf
schemas are upstream material with their own licensing obligations; see
[THIRD_PARTY.md](THIRD_PARTY.md). Do not redistribute or publish this
repository until the license review is complete.
