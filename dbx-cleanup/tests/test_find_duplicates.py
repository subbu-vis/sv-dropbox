from dataclasses import dataclass
from pathlib import Path

import pytest

from find_duplicates import FileEntry, should_skip_file, group_by_hash, select_top_groups, write_csv


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


@pytest.mark.parametrize(
    "path, expected_skip",
    [
        # exact-folder match (case-insensitive) and recursive descendants
        ("/Cetachi Comics/issue1.cbr", True),
        ("/cetachi comics/issue1.cbr", True),
        ("/Cetachi Comics/sub/x/y.cbr", True),
        # not under an ignored folder
        ("/Photos/img.jpg", False),
        # near-match: different folder name that starts with same prefix
        ("/Cetachi Comics And More/x.cbr", False),
        # the folder itself (rare, but defensive)
        ("/Cetachi Comics", True),
    ],
)
def test_should_skip_file_honors_ignored_folders(path: str, expected_skip: bool) -> None:
    meta = FakeMeta(
        name=path.rsplit("/", 1)[-1] or "x",
        path_display=path,
        size=200_000,
        content_hash="h",
        server_modified="2024-01-01T00:00:00Z",
    )
    assert should_skip_file(
        meta,
        min_file_size_bytes=100_000,
        skip_hidden=True,
        skip_shared_not_owned=True,
        owner_account_id="self",
        ignored_folders=("/cetachi comics",),
    ) is expected_skip


@pytest.mark.parametrize(
    "path, expected_skip",
    [
        # ignoring /folder1/subfolder2 only affects that subtree
        ("/folder1/subfolder2/file.txt", True),
        ("/folder1/subfolder2/deep/nested/x.txt", True),
        # parent and siblings are untouched
        ("/folder1/file.txt", False),
        ("/folder1/subfolder1/file.txt", False),
        ("/folder1/subfolder3/file.txt", False),
        # different top-level folder
        ("/elsewhere/file.txt", False),
    ],
)
def test_ignored_folders_handles_nested_paths(path: str, expected_skip: bool) -> None:
    meta = FakeMeta(
        name=path.rsplit("/", 1)[-1],
        path_display=path,
        size=200_000,
        content_hash="h",
        server_modified="2024-01-01T00:00:00Z",
    )
    assert should_skip_file(
        meta,
        min_file_size_bytes=100_000,
        skip_hidden=True,
        skip_shared_not_owned=True,
        owner_account_id="self",
        ignored_folders=("/folder1/subfolder2",),
    ) is expected_skip


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


def test_write_csv_empty_groups_writes_header_only(tmp_path: Path) -> None:
    out_path = tmp_path / "empty.csv"
    write_csv([], out_path)
    assert out_path.read_text().strip() == \
        "group_id,filename,size_bytes,path,content_hash,last_modified,delete"


def test_write_csv_single_group_has_no_trailing_blank(tmp_path: Path) -> None:
    groups = [
        [
            make_entry("a.txt", "/A/a.txt", 1000, "h1"),
            make_entry("a.txt", "/B/a.txt", 1000, "h1"),
        ],
    ]
    out_path = tmp_path / "one.csv"
    write_csv(groups, out_path)
    lines = out_path.read_text().splitlines()
    assert len(lines) == 3  # header + 2 rows, no trailing blank


def test_write_csv_sorts_paths_case_insensitively(tmp_path: Path) -> None:
    groups = [[
        make_entry("a.txt", "/B/a.txt", 1000, "h1"),
        make_entry("a.txt", "/a/a.txt", 1000, "h1"),
        make_entry("a.txt", "/C/a.txt", 1000, "h1"),
    ]]
    out_path = tmp_path / "case.csv"
    write_csv(groups, out_path)
    lines = out_path.read_text().splitlines()
    paths_in_order = [line.split(",")[3] for line in lines[1:]]
    assert paths_in_order == ["/a/a.txt", "/B/a.txt", "/C/a.txt"]


def test_write_csv_columns_and_blank_rows_between_groups(tmp_path: Path) -> None:
    groups = [
        [
            make_entry("a.txt", "/A/a.txt", 1000, "h1"),
            make_entry("a.txt", "/B/a.txt", 1000, "h1"),
        ],
        [
            make_entry("b.txt", "/C/b.txt", 2000, "h2"),
            make_entry("b.txt", "/D/b.txt", 2000, "h2"),
        ],
    ]
    out_path = tmp_path / "duplicates.csv"
    write_csv(groups, out_path)

    text = out_path.read_text()
    lines = text.splitlines()
    assert lines[0] == "group_id,filename,size_bytes,path,content_hash,last_modified,delete"
    # Group 1: 2 rows, then blank, then group 2: 2 rows = 6 lines after header
    assert lines[1].startswith("1,a.txt,1000,/A/a.txt,h1,")
    assert lines[2].startswith("1,a.txt,1000,/B/a.txt,h1,")
    assert lines[3] == ""
    assert lines[4].startswith("2,b.txt,2000,/C/b.txt,h2,")
    assert lines[5].startswith("2,b.txt,2000,/D/b.txt,h2,")
