import math
from pathlib import Path

import pytest

from dbx_folder_sizes import (
    aggregate_folder_sizes,
    iter_ancestors,
    write_csv,
)


@pytest.mark.parametrize(
    "path, expected",
    [
        # nested file -> all named ancestors, no root, no file itself
        ("/a/b/c/file.txt", ["/a", "/a/b", "/a/b/c"]),
        # one level deep
        ("/photos/img.jpg", ["/photos"]),
        # top-level file -> no folder ancestors
        ("/file.txt", []),
        # spaces and mixed case preserved (display path)
        ("/Cetachi Comics/issue1.cbr", ["/Cetachi Comics"]),
    ],
)
def test_iter_ancestors(path: str, expected: list[str]) -> None:
    assert list(iter_ancestors(path)) == expected


def test_aggregate_folder_sizes_rolls_up_each_ancestor() -> None:
    files = [
        ("/Photos/2019/raw/img1.cr2", 5_000_000),
        ("/Photos/2019/raw/img2.cr2", 3_000_000),
        ("/Photos/2019/edited/img1.jpg", 1_000_000),
        ("/Photos/2020/img.jpg", 2_000_000),
        ("/Misc/note.txt", 500),
    ]
    agg = aggregate_folder_sizes(files)

    # /Photos rolls up everything under it
    assert agg["/Photos"] == (5_000_000 + 3_000_000 + 1_000_000 + 2_000_000, 4)
    # /Photos/2019 rolls up raw + edited
    assert agg["/Photos/2019"] == (5_000_000 + 3_000_000 + 1_000_000, 3)
    # leaf folders
    assert agg["/Photos/2019/raw"] == (8_000_000, 2)
    assert agg["/Photos/2019/edited"] == (1_000_000, 1)
    assert agg["/Photos/2020"] == (2_000_000, 1)
    assert agg["/Misc"] == (500, 1)


def test_aggregate_folder_sizes_empty_input() -> None:
    assert aggregate_folder_sizes([]) == {}


def test_write_csv_sorts_descending_by_size_mb_and_rounds_up(tmp_path: Path) -> None:
    aggregated = {
        "/Big": (200 * 1024 * 1024, 50),       # 200 MB exact
        "/Medium": (50 * 1024 * 1024, 10),      # 50 MB exact
        "/Tiny": (700 * 1024, 1),               # 0.68 MB -> ceil to 1
        "/Small": (1024, 1),                    # 1 KB -> ceil to 1
    }
    out_path = tmp_path / "sizes.csv"
    write_csv(aggregated, out_path)

    lines = out_path.read_text().splitlines()
    assert lines[0] == "folder,size_mb,file_count"
    # Sorted by size desc; ties broken by sort stability (insertion order)
    assert lines[1] == "/Big,200,50"
    assert lines[2] == "/Medium,50,10"
    # /Tiny (700 KB) and /Small (1 KB) both round up to 1 MB; sort by raw bytes
    # determines order: /Tiny before /Small.
    assert lines[3] == "/Tiny,1,1"
    assert lines[4] == "/Small,1,1"


def test_write_csv_creates_parent_dir(tmp_path: Path) -> None:
    nested = tmp_path / "a" / "b" / "out.csv"
    write_csv({"/x": (1024 * 1024, 1)}, nested)
    assert nested.exists()
