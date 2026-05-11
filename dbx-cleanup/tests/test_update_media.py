from __future__ import annotations

from pathlib import Path

import pytest

from update_media import parse_csv, EditedRow


def test_parse_csv_happy_path(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,existing_tags,new_tags,delete\n"
        "/x/a.jpg,h1,a.jpg,,seema,\n"
        "/x/b.jpg,h2,b.jpg,existing,,x\n"
    )
    rows = parse_csv(csv_path)
    assert rows == [
        EditedRow(path="/x/a.jpg", content_hash="h1", filename="a.jpg",
                  existing_tags=[], new_tags=["seema"], marked_delete=False),
        EditedRow(path="/x/b.jpg", content_hash="h2", filename="b.jpg",
                  existing_tags=["existing"], new_tags=[], marked_delete=True),
    ]


def test_parse_csv_existing_tags_comma_split(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,existing_tags,new_tags,delete\n"
        '"/x/a.jpg",h1,a.jpg,"tag-a,tag-b","new1,new2",\n'
    )
    rows = parse_csv(csv_path)
    assert rows[0].existing_tags == ["tag-a", "tag-b"]
    assert rows[0].new_tags == ["new1", "new2"]


def test_parse_csv_strips_whitespace_in_tags(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,existing_tags,new_tags,delete\n"
        '/x/a.jpg,h,a.jpg,,"  seema , performance  ",\n'
    )
    rows = parse_csv(csv_path)
    assert rows[0].new_tags == ["seema", "performance"]


def test_parse_csv_tolerates_blank_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,existing_tags,new_tags,delete\n"
        "/x/a.jpg,h1,a.jpg,,seema,\n"
        "\n"
        "/x/b.jpg,h2,b.jpg,,family,\n"
    )
    rows = parse_csv(csv_path)
    assert len(rows) == 2


def test_parse_csv_missing_required_column(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,new_tags,delete\n"  # missing existing_tags
        "/x/a.jpg,h,a.jpg,seema,\n"
    )
    with pytest.raises(ValueError, match="missing required columns"):
        parse_csv(csv_path)


def test_parse_csv_delete_case_insensitive(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,existing_tags,new_tags,delete\n"
        "/x/a.jpg,h,a.jpg,,,X\n"
        "/x/b.jpg,h,b.jpg,,, x \n"
    )
    rows = parse_csv(csv_path)
    assert rows[0].marked_delete is True
    assert rows[1].marked_delete is True
