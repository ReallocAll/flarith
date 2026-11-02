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
