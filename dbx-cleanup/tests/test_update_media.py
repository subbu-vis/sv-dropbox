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


from update_media import (
    ValidationProblem,
    validate_conflict_tag_and_delete,
    validate_max_rows,
    validate_tag_normalization_and_count,
)


def _row(path: str, new_tags: list[str], delete: bool = False,
         existing: list[str] | None = None) -> EditedRow:
    return EditedRow(path=path, content_hash="h", filename=path.rsplit("/", 1)[-1],
                     existing_tags=existing or [], new_tags=new_tags,
                     marked_delete=delete)


def test_validate_conflict_clean() -> None:
    rows = [_row("/a.jpg", ["seema"]), _row("/b.jpg", [], delete=True)]
    assert validate_conflict_tag_and_delete(rows) == []


def test_validate_conflict_detects_both_populated() -> None:
    rows = [_row("/a.jpg", ["seema"], delete=True)]
    problems = validate_conflict_tag_and_delete(rows)
    assert len(problems) == 1
    assert problems[0].code == "CONFLICT_TAG_AND_DELETE"
    assert "/a.jpg" in problems[0].offending_paths


def test_validate_max_rows_within_limit() -> None:
    rows = [_row(f"/p{i}.jpg", ["x"]) for i in range(10)]
    assert validate_max_rows(rows, max_csv_rows=10) == []


def test_validate_max_rows_exceeds() -> None:
    rows = [_row(f"/p{i}.jpg", ["x"]) for i in range(11)]
    problems = validate_max_rows(rows, max_csv_rows=10)
    assert len(problems) == 1
    assert problems[0].code == "EXCEEDS_MAX_ROWS"


def test_validate_tag_invalid_chars() -> None:
    rows = [_row("/a.jpg", ["valid", "in!valid"])]
    problems = validate_tag_normalization_and_count(rows)
    codes = [p.code for p in problems]
    assert "INVALID_TAG" in codes


def test_validate_tag_too_long() -> None:
    rows = [_row("/a.jpg", ["a" * 33])]
    problems = validate_tag_normalization_and_count(rows)
    assert any(p.code == "INVALID_TAG" for p in problems)


def test_validate_tag_count_at_limit() -> None:
    """existing 15 + new 5 = 20: ok (exactly at Dropbox cap)."""
    rows = [_row("/a.jpg", [f"new{i}" for i in range(5)],
                 existing=[f"old{i}" for i in range(15)])]
    problems = validate_tag_normalization_and_count(rows)
    assert all(p.code != "TOO_MANY_TAGS" for p in problems)


def test_validate_tag_count_over_limit() -> None:
    """existing 18 + new 5 = 23: rejects."""
    rows = [_row("/a.jpg", [f"new{i}" for i in range(5)],
                 existing=[f"old{i}" for i in range(18)])]
    problems = validate_tag_normalization_and_count(rows)
    assert any(p.code == "TOO_MANY_TAGS" for p in problems)


def test_validate_tag_count_dedupes_against_existing() -> None:
    """existing 18 + new 5 where 3 of the 5 already exist: union = 20, ok."""
    rows = [_row("/a.jpg", ["old0", "old1", "old2", "n1", "n2"],
                 existing=[f"old{i}" for i in range(18)])]
    problems = validate_tag_normalization_and_count(rows)
    assert all(p.code != "TOO_MANY_TAGS" for p in problems)


def test_validate_tag_normalizes_before_validating() -> None:
    """User input '#Diwali 2019' is valid post-normalization."""
    rows = [_row("/a.jpg", ["#Diwali 2019", "Seema"])]
    problems = validate_tag_normalization_and_count(rows)
    assert all(p.code != "INVALID_TAG" for p in problems)
