from dataclasses import dataclass

import pytest

from find_duplicates import FileEntry, should_skip_file, group_by_hash, select_top_groups


@dataclass(frozen=True)
class FakeMeta:
    """Subset of dropbox.files.FileMetadata fields we care about."""
    name: str
    path_display: str
    size: int
    content_hash: str
    server_modified: str
    sharing_info: object | None = None


@pytest.mark.parametrize(
    "size, path, expected",
    [
        (50_000, "/Photos/img.jpg", True),     # below 100 KB threshold
        (200_000, "/Photos/img.jpg", False),   # above threshold
        (200_000, "/Photos/.hidden", True),    # hidden file
        (200_000, "/.dropbox.cache/x", True),  # hidden folder segment
        (0, "/Photos/empty.txt", True),        # empty file
    ],
)
def test_should_skip_file_size_and_hidden(size: int, path: str, expected: bool) -> None:
    meta = FakeMeta(name=path.rsplit("/", 1)[-1], path_display=path, size=size,
                    content_hash="abc", server_modified="2024-01-01T00:00:00Z")
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=True,
                            skip_shared_not_owned=True, owner_account_id="self") is expected


@dataclass(frozen=True)
class FakeShareInfo:
    modified_by: str | None


def test_should_skip_shared_not_owned() -> None:
    meta = FakeMeta(
        name="x.pdf", path_display="/Shared/x.pdf", size=200_000,
        content_hash="h", server_modified="2024-01-01T00:00:00Z",
        sharing_info=FakeShareInfo(modified_by="other-account"),
    )
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=True,
                            skip_shared_not_owned=True, owner_account_id="self") is True


def test_should_keep_shared_owned_by_self() -> None:
    meta = FakeMeta(
        name="x.pdf", path_display="/Shared/x.pdf", size=200_000,
        content_hash="h", server_modified="2024-01-01T00:00:00Z",
        sharing_info=FakeShareInfo(modified_by="self"),
    )
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=True,
                            skip_shared_not_owned=True, owner_account_id="self") is False


def test_should_keep_shared_when_modified_by_is_none() -> None:
    """When sharing_info exists but modified_by is None, we can't prove ownership;
    keep the file rather than skip it."""
    meta = FakeMeta(
        name="x.pdf", path_display="/Shared/x.pdf", size=200_000,
        content_hash="h", server_modified="2024-01-01T00:00:00Z",
        sharing_info=FakeShareInfo(modified_by=None),
    )
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=True,
                            skip_shared_not_owned=True, owner_account_id="self") is False


def test_should_keep_hidden_when_skip_hidden_disabled() -> None:
    meta = FakeMeta(
        name=".hidden", path_display="/Photos/.hidden", size=200_000,
        content_hash="h", server_modified="2024-01-01T00:00:00Z",
    )
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=False,
                            skip_shared_not_owned=True, owner_account_id="self") is False


def test_should_keep_shared_not_owned_when_flag_disabled() -> None:
    meta = FakeMeta(
        name="x.pdf", path_display="/Shared/x.pdf", size=200_000,
        content_hash="h", server_modified="2024-01-01T00:00:00Z",
        sharing_info=FakeShareInfo(modified_by="other-account"),
    )
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=True,
                            skip_shared_not_owned=False, owner_account_id="self") is False


def make_entry(name: str, path: str, size: int, h: str) -> FileEntry:
    return FileEntry(name=name, path=path, size=size, content_hash=h,
                     server_modified="2024-01-01T00:00:00Z")


def test_group_by_hash_drops_singletons() -> None:
    entries = [
        make_entry("a.txt", "/a.txt", 1000, "h1"),
        make_entry("b.txt", "/b.txt", 1000, "h1"),
        make_entry("solo.txt", "/solo.txt", 999, "h2"),
    ]
    groups = group_by_hash(entries)
    assert list(groups.keys()) == ["h1"]
    assert len(groups["h1"]) == 2


def test_select_top_groups_orders_by_wasted_space_desc() -> None:
    # Group X: 2 copies × 5 KB → 5 KB wasted
    # Group Y: 4 copies × 1.5 KB → 4.5 KB wasted
    # Group Z: 3 copies × 2 KB → 4 KB wasted
    groups = {
        "Z": [make_entry("z.txt", f"/z{i}", 2000, "Z") for i in range(3)],
        "X": [make_entry("x.txt", f"/x{i}", 5000, "X") for i in range(2)],
        "Y": [make_entry("y.txt", f"/y{i}", 1500, "Y") for i in range(4)],
    }
    selected = select_top_groups(groups, max_csv_rows=100)
    # Order: X (5 KB), Y (4.5 KB), Z (4 KB)
    assert [g[0].content_hash for g in selected] == ["X", "Y", "Z"]


def test_select_top_groups_never_splits_a_group() -> None:
    # Two groups: A has 3 rows, B has 2 rows. Cap is 4. We should pick whichever
    # ranks first that fits whole. A wastes (3-1)*1000=2000; B wastes (2-1)*1500=1500.
    # A is bigger waster. A has 3 rows ≤ 4 → take A. Then B has 2 rows, A+B = 5 > 4 → skip B.
    groups = {
        "A": [make_entry("a.txt", f"/a{i}", 1000, "A") for i in range(3)],
        "B": [make_entry("b.txt", f"/b{i}", 1500, "B") for i in range(2)],
    }
    selected = select_top_groups(groups, max_csv_rows=4)
    assert len(selected) == 1
    assert selected[0][0].content_hash == "A"


def test_select_top_groups_skips_oversize_first_group() -> None:
    # First-ranked group is bigger than cap; should skip and try smaller ones.
    groups = {
        "BIG": [make_entry("b.txt", f"/b{i}", 10_000, "BIG") for i in range(10)],
        "SMALL": [make_entry("s.txt", f"/s{i}", 500, "SMALL") for i in range(2)],
    }
    selected = select_top_groups(groups, max_csv_rows=5)
    # BIG has 10 rows > 5; skip. SMALL has 2 rows ≤ 5; keep.
    assert [g[0].content_hash for g in selected] == ["SMALL"]


def test_select_top_groups_rejects_non_positive_cap() -> None:
    with pytest.raises(ValueError, match="max_csv_rows must be positive"):
        select_top_groups({"X": [make_entry("a", "/a", 1, "X")]}, max_csv_rows=0)


def test_group_by_hash_empty_input_returns_empty() -> None:
    assert group_by_hash([]) == {}
