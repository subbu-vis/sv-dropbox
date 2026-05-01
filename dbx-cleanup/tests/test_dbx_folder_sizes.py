from pathlib import Path

import pytest

from dbx_folder_sizes import (
    MAX_FOLDER_DEPTH,
    aggregate_folder_sizes,
    iter_ancestors,
    write_csv,
)


@pytest.mark.parametrize(
    "path, expected",
    [
        # Files at depth ≤ 3 yield all named ancestors
        ("/a/b/c/file.txt", ["/a", "/a/b", "/a/b/c"]),
        ("/photos/img.jpg", ["/photos"]),
        ("/file.txt", []),
        ("/Old Backups/issue1.cbr", ["/Old Backups"]),
        # Deep files cap at MAX_FOLDER_DEPTH=3 — /a/b/c/d/e.txt yields /a, /a/b, /a/b/c
        ("/a/b/c/d/e.txt", ["/a", "/a/b", "/a/b/c"]),
        ("/a/b/c/d/e/f/g.txt", ["/a", "/a/b", "/a/b/c"]),
    ],
)
def test_iter_ancestors_default_caps_at_three(path: str, expected: list[str]) -> None:
    assert list(iter_ancestors(path)) == expected


def test_iter_ancestors_respects_explicit_max_depth() -> None:
    assert list(iter_ancestors("/a/b/c/d/e.txt", max_depth=2)) == ["/a", "/a/b"]
    assert list(iter_ancestors("/a/b/c/d/e.txt", max_depth=5)) == [
        "/a", "/a/b", "/a/b/c", "/a/b/c/d"
    ]


def test_aggregate_folder_sizes_rolls_up_each_ancestor() -> None:
    files = [
        ("/Photos/2019/raw/img1.cr2", 5_000_000),
        ("/Photos/2019/raw/img2.cr2", 3_000_000),
        ("/Photos/2019/edited/img1.jpg", 1_000_000),
        ("/Photos/2020/img.jpg", 2_000_000),
        ("/Misc/note.txt", 500),
    ]
    agg = aggregate_folder_sizes(files)

    assert agg["/Photos"] == (5_000_000 + 3_000_000 + 1_000_000 + 2_000_000, 4)
    assert agg["/Photos/2019"] == (5_000_000 + 3_000_000 + 1_000_000, 3)
    assert agg["/Photos/2019/raw"] == (8_000_000, 2)
    assert agg["/Photos/2019/edited"] == (1_000_000, 1)
    assert agg["/Photos/2020"] == (2_000_000, 1)
    assert agg["/Misc"] == (500, 1)


def test_aggregate_folder_sizes_does_not_emit_depth_4_keys() -> None:
    """A file 5 levels deep contributes to depth 1/2/3 only; its depth-4
    ancestor folder does NOT appear in the output."""
    files = [("/a/b/c/d/file.txt", 1_000_000)]
    agg = aggregate_folder_sizes(files)
    assert set(agg.keys()) == {"/a", "/a/b", "/a/b/c"}
    # All three ancestors get the full file size
    assert agg["/a"] == (1_000_000, 1)
    assert agg["/a/b"] == (1_000_000, 1)
    assert agg["/a/b/c"] == (1_000_000, 1)


def test_aggregate_folder_sizes_empty_input() -> None:
    assert aggregate_folder_sizes([]) == {}


def test_max_folder_depth_constant_is_three() -> None:
    assert MAX_FOLDER_DEPTH == 3


def test_write_csv_emits_tree_order_with_children_under_parent(tmp_path: Path) -> None:
    """The output groups each folder with its subfolders. At each level,
    siblings are sorted by size desc."""
    aggregated = {
        # Top-level folders
        "/Photos": (200 * 1024 * 1024, 50),
        "/Music": (100 * 1024 * 1024, 30),
        # /Photos children
        "/Photos/2020": (120 * 1024 * 1024, 30),
        "/Photos/2019": (80 * 1024 * 1024, 20),
        # /Music children
        "/Music/Albums": (90 * 1024 * 1024, 25),
        "/Music/Singles": (10 * 1024 * 1024, 5),
        # depth-3 grandchildren
        "/Photos/2020/raw": (100 * 1024 * 1024, 25),
        "/Photos/2020/edited": (20 * 1024 * 1024, 5),
        "/Music/Albums/Stones": (50 * 1024 * 1024, 12),
        "/Music/Albums/Beatles": (30 * 1024 * 1024, 10),
        # /Photos/2019 has no children in this fixture
    }
    out_path = tmp_path / "tree.csv"
    write_csv(aggregated, out_path)

    lines = out_path.read_text().splitlines()
    # Skip header, take folder column only
    folders_in_order = [line.split(",")[0] for line in lines[1:]]

    expected_order = [
        "/Photos",                 # biggest top-level
        "/Photos/2020",            #   bigger child first
        "/Photos/2020/raw",        #     bigger grandchild first
        "/Photos/2020/edited",
        "/Photos/2019",            #   smaller sibling
        "/Music",                  # second top-level
        "/Music/Albums",           #   bigger child first
        "/Music/Albums/Stones",    #     bigger grandchild first
        "/Music/Albums/Beatles",
        "/Music/Singles",          #   smaller sibling
    ]
    assert folders_in_order == expected_order


def test_write_csv_size_mb_rounds_up(tmp_path: Path) -> None:
    aggregated = {
        "/Big": (200 * 1024 * 1024, 50),       # 200 MB exact
        "/Tiny": (700 * 1024, 1),               # 0.68 MB -> ceil to 1
        "/Small": (1024, 1),                    # 1 KB -> ceil to 1
    }
    out_path = tmp_path / "sizes.csv"
    write_csv(aggregated, out_path)

    lines = out_path.read_text().splitlines()
    assert lines[0] == "folder,size_mb,file_count"
    assert lines[1] == "/Big,200,50"
    # Tie at 1 MB; sorted by raw bytes desc -> Tiny before Small.
    assert lines[2] == "/Tiny,1,1"
    assert lines[3] == "/Small,1,1"


def test_write_csv_creates_parent_dir(tmp_path: Path) -> None:
    nested = tmp_path / "a" / "b" / "out.csv"
    write_csv({"/x": (1024 * 1024, 1)}, nested)
    assert nested.exists()
