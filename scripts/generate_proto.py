"""Regenerate committed Spark bindings from pinned upstream schemas."""

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "src" / "flarith" / "proto"
SOURCES = ["proto/spark/spark.proto", "proto/spark/spark_sampler.proto"]


def main() -> None:
    subprocess.run(
        [sys.executable, "-m", "grpc_tools.protoc", "-I", str(ROOT / "proto"),
         "--python_out", str(OUT), *(str(ROOT / p) for p in SOURCES)],
        check=True,
    )
    sampler = OUT / "spark" / "spark_sampler_pb2.py"
    original = sampler.read_text(encoding="utf-8")
    needle = "from spark import spark_pb2 as spark_dot_spark__pb2"
    if original.count(needle) != 1:
        raise RuntimeError("Unexpected protoc dependency import; review generator output")
    sampler.write_text(
        original.replace(needle, "from flarith.proto.spark import spark_pb2 as spark_dot_spark__pb2"),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
