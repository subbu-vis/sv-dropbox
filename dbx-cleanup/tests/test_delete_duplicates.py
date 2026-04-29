from pathlib import Path

import pytest

from delete_duplicates import CsvRow, parse_csv


def write_csv(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "dup.csv"
    p.write_text(
        "group_id,filename,size_bytes,path,content_hash,last_modified,delete\n" + body
    )
    return p


def test_parse_csv_basic(tmp_path: Path) -> None:
    p = write_csv(tmp_path, (
        "1,a.txt,1000,/A/a.txt,h1,2024-01-01T00:00:00,\n"
        "1,a.txt,1000,/B/a.txt,h1,2024-01-01T00:00:00,x\n"
        "\n"
        "2,b.txt,2000,/C/b.txt,h2,2024-01-01T00:00:00,\n"
        "2,b.txt,2000,/D/b.txt,h2,2024-01-01T00:00:00,\n"
    ))
    rows = parse_csv(p)
    assert len(rows) == 4
    assert rows[0].group_id == 1
    assert rows[0].marked_delete is False
    assert rows[1].marked_delete is True
    assert rows[1].path == "/B/a.txt"


def test_parse_csv_ignores_blank_separator_rows(tmp_path: Path) -> None:
    p = write_csv(tmp_path, (
        "1,a.txt,1000,/A/a.txt,h1,2024-01-01T00:00:00,\n"
        "\n"
        "1,a.txt,1000,/B/a.txt,h1,2024-01-01T00:00:00,\n"
    ))
    rows = parse_csv(p)
    assert len(rows) == 2


def test_parse_csv_normalizes_x_case_and_whitespace(tmp_path: Path) -> None:
    p = write_csv(tmp_path, (
        "1,a.txt,1000,/A/a.txt,h1,2024-01-01T00:00:00, X \n"
        "1,a.txt,1000,/B/a.txt,h1,2024-01-01T00:00:00,x\n"
    ))
    rows = parse_csv(p)
    assert all(r.marked_delete for r in rows)
