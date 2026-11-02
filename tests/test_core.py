import pytest

from flarith import AmbiguousPathError, InvalidProfileError, Profile
from flarith.proto.spark import spark_sampler_pb2


def make_sample():
    sample = spark_sampler_pb2.SamplerData()
    sample.metadata.start_time = 100
    sample.metadata.end_time = 200
    sample.metadata.interval = 4000
    sample.time_windows.extend([10, 20])
    sample.time_window_statistics[10].tps = 19.5
    thread = sample.threads.add()
    thread.name = "Server thread"
    thread.times.extend([2.25, 4.5])
    root = thread.children.add()
    root.class_name, root.method_name = "Level", "tick"
    root.times.extend([2.25, 4.5])
    root.children_refs.extend([2, 1])
    leaf = thread.children.add()
    leaf.method_name = "leaf"
    leaf.times.extend([1.0, 1.5])
    middle = thread.children.add()
    middle.method_name = "middle"
    middle.children_refs.append(1)
    middle.times.extend([1.25, 3.0])
    thread.children_refs.append(0)
    return sample


def test_node_pool_edges_and_raw_metadata():
    sample = make_sample()
    profile = Profile.from_bytes(sample.SerializeToString())
    graph = profile.threads[0]
    assert profile.node_count == 3
    assert profile.profile_ref.startswith("sha256:")
    assert graph.roots == (0,)
    assert graph.children(0) == (2, 1)
    assert graph.node(2).method_name == "middle"
    assert graph.raw_times(0) == (2.25, 4.5)
    assert graph.raw_times() == (2.25, 4.5)
    assert profile.message.time_window_statistics[10].tps == 19.5
    assert profile.profile_ref == Profile.from_bytes(sample.SerializeToString()).profile_ref
    assert graph.path_to(2) == (0, 2)


def test_shared_node_requires_path_disambiguation():
    graph = Profile.from_bytes(make_sample().SerializeToString()).threads[0]
    assert graph.parents[1] == (0, 2)
    with pytest.raises(AmbiguousPathError):
        graph.path_to(1)


@pytest.mark.parametrize("bad_index", [-1, 3, 300])
def test_invalid_child_reference(bad_index):
    sample = make_sample()
    sample.threads[0].children[0].children_refs.append(bad_index)
    with pytest.raises(InvalidProfileError, match="references"):
        Profile.from_bytes(sample.SerializeToString())


def test_invalid_root_reference():
    sample = make_sample()
    sample.threads[0].children_refs.append(3)
    with pytest.raises(InvalidProfileError, match="root reference"):
        Profile.from_bytes(sample.SerializeToString())


def test_cycles_detected_even_in_orphan_pool():
    sample = make_sample()
    thread = sample.threads[0]
    orphan = thread.children.add()
    orphan.children_refs.append(3)
    with pytest.raises(InvalidProfileError, match="cycle"):
        Profile.from_bytes(sample.SerializeToString())


def test_deep_paths_iterative():
    sample = make_sample()
    thread = sample.threads[0]
    del thread.children[:]
    del thread.children_refs[:]
    thread.children_refs.append(0)
    for i in range(3000):
        node = thread.children.add()
        node.method_name = str(i)
        if i < 2999:
            node.children_refs.append(i + 1)
    graph = Profile.from_bytes(sample.SerializeToString()).threads[0]
    path = graph.path_to(2999)
    assert len(path) == 3000
    assert path[0] == 0 and path[-1] == 2999


def test_malformed_payloads():
    for value in (b"", b"\x0a\xff", b"\x08\x01"):
        with pytest.raises(InvalidProfileError):
            Profile.from_bytes(value)


def test_node_bounds():
    graph = Profile.from_bytes(make_sample().SerializeToString()).threads[0]
    with pytest.raises(IndexError):
        graph.node(-1)
    with pytest.raises(IndexError):
        graph.node(3)


def test_orphan_not_mistaken_for_root():
    sample = make_sample()
    sample.threads[0].children.add().method_name = "orphan"
    graph = Profile.from_bytes(sample.SerializeToString()).threads[0]
    with pytest.raises(InvalidProfileError, match="not reachable"):
        graph.path_to(3)
