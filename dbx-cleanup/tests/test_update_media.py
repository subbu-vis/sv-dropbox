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


from unittest.mock import MagicMock
from dropbox.exceptions import ApiError

from update_media import validate_paths_and_hashes


def test_validate_paths_and_hashes_all_ok() -> None:
    client = MagicMock()
    # Each get_metadata returns the matching hash for the path.
    def _meta(path: str) -> MagicMock:
        m = MagicMock(); m.content_hash = {"/a.jpg": "h1", "/b.jpg": "h2"}[path]; return m
    client.files_get_metadata.side_effect = _meta
    rows = [_row("/a.jpg", ["x"]), _row("/b.jpg", ["y"])]
    rows = [EditedRow(r.path, {"/a.jpg": "h1", "/b.jpg": "h2"}[r.path],
                      r.filename, r.existing_tags, r.new_tags, r.marked_delete)
            for r in rows]
    assert validate_paths_and_hashes(client, rows) == []


def test_validate_paths_path_not_found() -> None:
    client = MagicMock()
    client.files_get_metadata.side_effect = ApiError(
        "req-id", MagicMock(__str__=lambda self: "path/not_found"), "user", "user")
    rows = [EditedRow("/missing.jpg", "h", "missing.jpg", [], ["x"], False)]
    problems = validate_paths_and_hashes(client, rows)
    assert len(problems) == 1
    assert problems[0].code == "PATH_NOT_FOUND"
    assert "/missing.jpg" in problems[0].offending_paths


def test_validate_paths_hash_changed() -> None:
    client = MagicMock()
    meta = MagicMock(); meta.content_hash = "different_hash"
    client.files_get_metadata.return_value = meta
    rows = [EditedRow("/a.jpg", "csv_hash", "a.jpg", [], ["x"], False)]
    problems = validate_paths_and_hashes(client, rows)
    assert len(problems) == 1
    assert problems[0].code == "HASH_CHANGED"


def test_validate_paths_skips_unmarked_unchanged_rows() -> None:
    """A row with no new_tags and no delete flag is a no-op — skip the API call."""
    client = MagicMock()
    rows = [EditedRow("/a.jpg", "h", "a.jpg", [], [], False)]
    problems = validate_paths_and_hashes(client, rows)
    assert problems == []
    assert client.files_get_metadata.call_count == 0


from update_media import execute_actions, write_error_log


def test_execute_tags_new_tag_only(tmp_path: Path) -> None:
    """Row with new_tags only -> apply_tags called with deduped list."""
    client = MagicMock()
    archive: dict[str, dict] = {}
    rows = [EditedRow("/a.jpg", "h", "a.jpg",
                      existing_tags=["already"],
                      new_tags=["already", "seema"],  # already dup, only seema new
                      marked_delete=False)]
    audit_path = tmp_path / "tag-log.csv"
    summary = execute_actions(client, rows, archive, audit_path)
    # apply_tags should have been called for "seema" only
    client.files_tags_add.assert_called_once_with("/a.jpg", "seema")
    assert summary.tagged_count == 1
    assert summary.deleted_count == 0
    assert summary.skipped_count == 0
    assert summary.error_count == 0
    # Archive should have the merged tags
    assert set(archive["/a.jpg"]["tags"]) == {"already", "seema"}
    # Audit log should exist with a tagged row
    assert audit_path.exists()
    contents = audit_path.read_text()
    assert "/a.jpg" in contents
    assert "tagged" in contents


def test_execute_deletes_a_row(tmp_path: Path) -> None:
    client = MagicMock()
    archive = {"/a.jpg": {"content_hash": "h", "tags": ["x"], "last_updated": "older"}}
    rows = [EditedRow("/a.jpg", "h", "a.jpg", ["x"], [], marked_delete=True)]
    audit_path = tmp_path / "tag-log.csv"
    summary = execute_actions(client, rows, archive, audit_path)
    client.files_delete_v2.assert_called_once_with("/a.jpg")
    assert summary.deleted_count == 1
    assert "deleted_at" in archive["/a.jpg"]


def test_execute_skips_noop_row(tmp_path: Path) -> None:
    client = MagicMock()
    archive: dict[str, dict] = {}
    rows = [EditedRow("/a.jpg", "h", "a.jpg", [], [], marked_delete=False)]
    audit_path = tmp_path / "tag-log.csv"
    summary = execute_actions(client, rows, archive, audit_path)
    assert summary.skipped_count == 1
    assert client.files_tags_add.call_count == 0
    assert client.files_delete_v2.call_count == 0


def test_execute_continues_on_per_row_error(tmp_path: Path) -> None:
    """One failing tag_add doesn't abort the rest."""
    client = MagicMock()
    # First call raises, second succeeds.
    client.files_tags_add.side_effect = [
        ApiError("rid", MagicMock(__str__=lambda self: "tag/conflict"), "u", "u"),
        None,
    ]
    archive: dict[str, dict] = {}
    rows = [
        EditedRow("/a.jpg", "h", "a.jpg", [], ["fail"], False),
        EditedRow("/b.jpg", "h", "b.jpg", [], ["ok"], False),
    ]
    audit_path = tmp_path / "tag-log.csv"
    summary = execute_actions(client, rows, archive, audit_path)
    assert summary.error_count == 1
    assert summary.tagged_count == 1
    assert "/b.jpg" in archive  # success → archive updated
    assert "/a.jpg" not in archive  # failure → archive NOT updated


def test_write_error_log(tmp_path: Path) -> None:
    log_path = tmp_path / "error.log"
    problems = [
        ValidationProblem(code="X", message="something happened",
                          offending_paths=("/a.jpg", "/b.jpg")),
    ]
    write_error_log(problems, log_path)
    contents = log_path.read_text()
    assert "Pre-flight validation failed" in contents
    assert "[X]" in contents
    assert "/a.jpg" in contents
    assert "/b.jpg" in contents
