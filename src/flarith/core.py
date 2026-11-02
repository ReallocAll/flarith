"""Schema-backed Spark SamplerData and stable indexed call-graph access.

Node pool indices are wire-format identities, not traversal-order positions.
Weights are exposed exactly as encoded; no unsupported Self/Inclusive
interpretation is performed by this module.
"""

from dataclasses import dataclass
from hashlib import sha256

from google.protobuf.message import DecodeError

from flarith.proto.spark import spark_sampler_pb2


class InvalidProfileError(ValueError):
    """Malformed Spark SamplerData or an invalid reference graph."""


class AmbiguousPathError(ValueError):
    """A node has multiple call paths; the caller must select a path."""


@dataclass(frozen=True, slots=True)
class ThreadGraph:
    thread_index: int
    thread: spark_sampler_pb2.ThreadNode
    roots: tuple[int, ...]
    parents: tuple[tuple[int | None, ...], ...]

    @classmethod
    def from_thread(
        cls, thread_index: int, thread: spark_sampler_pb2.ThreadNode
    ) -> "ThreadGraph":
        nodes = thread.children
        count = len(nodes)
        incoming: list[list[int | None]] = [[] for _ in range(count)]
        roots = tuple(thread.children_refs)

        for root in roots:
            if not 0 <= root < count:
                raise InvalidProfileError(
                    f"thread {thread_index}: root reference {root} outside node pool {count}"
                )
            incoming[root].append(None)

        for parent_index, node in enumerate(nodes):
            for child in node.children_refs:
                if not 0 <= child < count:
                    raise InvalidProfileError(
                        f"thread {thread_index}: node {parent_index} references {child} "
                        f"outside node pool {count}"
                    )
                incoming[child].append(parent_index)

        # Validate the entire pool, including unreferenced nodes. Iterative
        # DFS prevents recursion limits for real-world native stacks.
        state = bytearray(count)
        for start in range(count):
            if state[start]:
                continue
            state[start] = 1
            stack = [(start, iter(nodes[start].children_refs))]
            while stack:
                current, edges = stack[-1]
                try:
                    child = next(edges)
                except StopIteration:
                    state[current] = 2
                    stack.pop()
                    continue
                if state[child] == 1:
                    raise InvalidProfileError(
                        f"thread {thread_index}: cycle involving node {child}"
                    )
                if state[child] == 0:
                    state[child] = 1
                    stack.append((child, iter(nodes[child].children_refs)))

        return cls(thread_index, thread, roots, tuple(tuple(p) for p in incoming))

    def node(self, index: int) -> spark_sampler_pb2.StackTraceNode:
        if not 0 <= index < len(self.thread.children):
            raise IndexError(f"invalid node index: {index}")
        return self.thread.children[index]

    def children(self, index: int) -> tuple[int, ...]:
        return tuple(self.node(index).children_refs)

    def raw_times(self, index: int | None = None) -> tuple[float, ...]:
        source = self.thread if index is None else self.node(index)
        return tuple(source.times)

    def path_to(self, index: int) -> tuple[int, ...]:
        """Return a unique root-to-node path or reject ambiguous/unrooted nodes."""
        self.node(index)
        path = [index]
        current = index
        while True:
            parents = self.parents[current]
            if len(parents) != 1:
                if not parents:
                    raise InvalidProfileError(
                        f"thread {self.thread_index}: node {index} is not reachable "
                        "from a root"
                    )
                raise AmbiguousPathError(
                    f"thread {self.thread_index}: node {current} has "
                    f"{len(parents)} incoming references"
                )
            parent = parents[0]
            if parent is None:
                return tuple(reversed(path))
            current = parent
            path.append(current)


@dataclass(frozen=True, slots=True)
class Profile:
    message: spark_sampler_pb2.SamplerData
    profile_ref: str
    threads: tuple[ThreadGraph, ...]

    @classmethod
    def from_bytes(cls, payload: bytes) -> "Profile":
        if not payload:
            raise InvalidProfileError("empty profile")
        message = spark_sampler_pb2.SamplerData()
        try:
            message.ParseFromString(payload)
        except DecodeError as exc:
            raise InvalidProfileError("invalid SamplerData protobuf") from exc
        # Protobuf often accepts arbitrary byte sequences as unknown fields;
        # require a recognizable sampler envelope before treating it as Spark.
        if not message.HasField("metadata") or not message.threads:
            raise InvalidProfileError("missing sampler metadata or threads")
        threads = tuple(
            ThreadGraph.from_thread(i, thread)
            for i, thread in enumerate(message.threads)
        )
        return cls(message, "sha256:" + sha256(payload).hexdigest(), threads)

    @property
    def node_count(self) -> int:
        return sum(len(thread.thread.children) for thread in self.threads)
