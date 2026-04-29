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


def test_parse_csv_strips_utf8_bom(tmp_path: Path) -> None:
    p = tmp_path / "bom.csv"
    body = (
        "group_id,filename,size_bytes,path,content_hash,last_modified,delete\n"
        "1,a.txt,1000,/A/a.txt,h1,2024-01-01T00:00:00,\n"
    )
    p.write_bytes("﻿".encode("utf-8") + body.encode("utf-8"))
    rows = parse_csv(p)
    assert len(rows) == 1
    assert rows[0].group_id == 1


def test_parse_csv_missing_required_column_raises(tmp_path: Path) -> None:
    p = tmp_path / "bad.csv"
    p.write_text(
        # missing `path` column
        "group_id,filename,size_bytes,content_hash,last_modified,delete\n"
        "1,a.txt,1000,h1,2024-01-01T00:00:00,\n"
    )
    with pytest.raises(ValueError, match="missing required columns"):
        parse_csv(p)


def test_parse_csv_bad_int_includes_line_number(tmp_path: Path) -> None:
    p = write_csv(tmp_path, (
        "1,a.txt,1000,/A/a.txt,h1,2024-01-01T00:00:00,\n"
        "abc,b.txt,2000,/B/b.txt,h1,2024-01-01T00:00:00,\n"
    ))
    with pytest.raises(ValueError, match="line 3"):
        parse_csv(p)
