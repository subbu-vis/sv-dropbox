from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata

from delete_duplicates import CsvRow, ValidationProblem, execute_deletes, parse_csv, validate_groups_have_survivor, validate_max_rows, validate_paths_and_hashes, write_error_log


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


def make_row(group_id: int, path: str, marked: bool, h: str = "h") -> CsvRow:
    return CsvRow(
        group_id=group_id, filename=path.rsplit("/", 1)[-1], size_bytes=1000,
        path=path, content_hash=h, last_modified="2024-01-01T00:00:00",
        marked_delete=marked,
    )


def test_validate_groups_have_survivor_passes_when_one_unmarked() -> None:
    rows = [make_row(1, "/a", False), make_row(1, "/b", True)]
    assert validate_groups_have_survivor(rows) == []


def test_validate_groups_have_survivor_flags_fully_marked_group() -> None:
    rows = [make_row(1, "/a", True), make_row(1, "/b", True),
            make_row(2, "/c", False), make_row(2, "/d", True)]
    problems = validate_groups_have_survivor(rows)
    assert len(problems) == 1
    assert problems[0].code == "GROUP_FULLY_MARKED"
    assert "Group 1" in problems[0].message
    # Both rows from group 1 should be in offending_paths
    assert set(problems[0].offending_paths) == {"/a", "/b"}


def test_validate_groups_have_survivor_flags_single_row_group_marked() -> None:
    """A 1-row group with that row marked is the textbook user-error case."""
    rows = [make_row(1, "/only-copy", True)]
    problems = validate_groups_have_survivor(rows)
    assert len(problems) == 1
    assert problems[0].code == "GROUP_FULLY_MARKED"


def test_validate_max_rows_passes_under_cap() -> None:
    rows = [make_row(1, f"/p{i}", True) for i in range(5)]
    assert validate_max_rows(rows, max_csv_rows=100) == []


def test_validate_max_rows_flags_overage() -> None:
    rows = [make_row(1, f"/p{i}", True) for i in range(101)]
    problems = validate_max_rows(rows, max_csv_rows=100)
    assert len(problems) == 1
    assert problems[0].code == "EXCEEDS_MAX_ROWS"
    assert "101" in problems[0].message


def fake_metadata(path: str, content_hash: str) -> FileMetadata:
    m = MagicMock(spec=FileMetadata)
    m.path_display = path
    m.content_hash = content_hash
    return m


def test_validate_paths_and_hashes_all_good() -> None:
    rows = [make_row(1, "/a", True, h="h1"), make_row(1, "/b", False, h="h1")]
    client = MagicMock()
    client.files_get_metadata.side_effect = lambda p: fake_metadata(p, "h1")
    problems = validate_paths_and_hashes(client, rows)
    assert problems == []
    # only checks marked rows
    client.files_get_metadata.assert_called_once_with("/a")


def _not_found_api_error() -> ApiError:
    """ApiError whose .error str contains 'not_found' (matches our narrowing
    in validate_paths_and_hashes)."""
    err_payload = MagicMock()
    err_payload.__str__ = lambda self: "GetMetadataError('path', LookupError('not_found'))"
    return ApiError("req-id", err_payload, "user-msg", "")


def test_validate_paths_and_hashes_missing_path() -> None:
    rows = [make_row(1, "/gone", True, h="h1"), make_row(1, "/b", False, h="h1")]
    client = MagicMock()
    client.files_get_metadata.side_effect = _not_found_api_error()
    problems = validate_paths_and_hashes(client, rows)
    assert len(problems) == 1
    assert problems[0].code == "PATH_NOT_FOUND"
    assert "/gone" in problems[0].offending_paths


def test_validate_paths_and_hashes_other_api_error_propagates() -> None:
    """ApiError that isn't path/not_found (e.g. permission, malformed) must
    surface as itself, not be silently bucketed as PATH_NOT_FOUND."""
    rows = [make_row(1, "/locked", True, h="h1")]
    client = MagicMock()
    err_payload = MagicMock()
    err_payload.__str__ = lambda self: "PermissionError('access_denied')"
    client.files_get_metadata.side_effect = ApiError("req-id", err_payload, "msg", "")
    with pytest.raises(ApiError):
        validate_paths_and_hashes(client, rows)


def test_validate_paths_and_hashes_changed_hash() -> None:
    rows = [make_row(1, "/a", True, h="h1"), make_row(1, "/b", False, h="h1")]
    client = MagicMock()
    client.files_get_metadata.side_effect = lambda p: fake_metadata(p, "h_NEW")
    problems = validate_paths_and_hashes(client, rows)
    assert len(problems) == 1
    assert problems[0].code == "HASH_CHANGED"
    assert "/a" in problems[0].offending_paths


def test_write_error_log_lists_all_problems(tmp_path: Path) -> None:
    problems = [
        ValidationProblem("GROUP_FULLY_MARKED", "Group 1 fully marked", ("/a", "/b")),
        ValidationProblem("PATH_NOT_FOUND", "1 missing", ("/gone",)),
    ]
    log_path = tmp_path / "error.log"
    write_error_log(problems, log_path)
    text = log_path.read_text()
    assert "GROUP_FULLY_MARKED" in text
    assert "PATH_NOT_FOUND" in text
    assert "/a" in text
    assert "/b" in text
    assert "/gone" in text


def test_execute_deletes_continues_on_error(tmp_path: Path) -> None:
    rows_to_delete = [make_row(1, "/a", True), make_row(1, "/b", True),
                      make_row(2, "/c", True)]
    client = MagicMock()
    # /b raises an ApiError; /a and /c succeed
    def fake_delete(path: str):
        if path == "/b":
            raise ApiError("req-id", MagicMock(), "boom", "")
        return MagicMock(metadata=MagicMock(path_display=path))
    client.files_delete_v2.side_effect = fake_delete

    log_path = tmp_path / "delete-log.csv"
    summary = execute_deletes(client, rows_to_delete, log_path)
    assert summary.success_count == 2
    assert summary.error_count == 1
    # /a (1000) + /c (1000) succeeded; /b errored. Bytes freed = sum of successes.
    assert summary.bytes_freed == 2000

    log_text = log_path.read_text()
    # CSV header + 3 rows
    assert log_text.count("\n") >= 4
    assert "/a" in log_text and "/b" in log_text and "/c" in log_text
    assert "deleted" in log_text and "error" in log_text


def test_execute_deletes_propagates_auth_error(tmp_path: Path) -> None:
    """Mid-batch token expiry must fail fast, not silently log 100 'errors'."""
    from dropbox.exceptions import AuthError

    rows_to_delete = [make_row(1, "/a", True), make_row(1, "/b", True)]
    client = MagicMock()
    client.files_delete_v2.side_effect = AuthError("req-id", "expired token")

    log_path = tmp_path / "delete-log.csv"
    with pytest.raises(AuthError):
        execute_deletes(client, rows_to_delete, log_path)

    # Audit log was created with header but no completed deletes (and crucially
    # not a row-per-failure for every remaining file).
    assert log_path.exists()
    assert log_path.read_text().count("\n") <= 1


def test_write_error_log_includes_introduction_text(tmp_path: Path) -> None:
    log_path = tmp_path / "error.log"
    write_error_log(
        [ValidationProblem("X", "msg", ("/p",))],
        log_path,
    )
    text = log_path.read_text()
    assert "Pre-flight validation failed at" in text
    assert "No deletions were performed." in text
