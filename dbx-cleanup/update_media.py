"""Engine for update_images.py and update_videos.py.

Reads the edited CSV produced by the HTML 'Export' button, validates everything,
prompts the user, applies native Dropbox tags or deletes flagged files,
merges results into the local JSON archive, and writes an audit log.
"""

from __future__ import annotations

import csv as csv_lib
from dataclasses import dataclass, field
from pathlib import Path

from dropbox.exceptions import ApiError

from dbx_client import with_retry
from dbx_media import normalize_tag


REQUIRED_COLUMNS = {"path", "content_hash", "filename",
                    "existing_tags", "new_tags", "delete"}


@dataclass(frozen=True)
class EditedRow:
    path: str
    content_hash: str
    filename: str
    existing_tags: list[str]
    new_tags: list[str]
    marked_delete: bool


def _split_tags(raw: str) -> list[str]:
    """Split a comma-joined tag string into a list, stripping whitespace.
    Empty input or all-whitespace returns []."""
    if not raw or not raw.strip():
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


def parse_csv(csv_path: Path) -> list[EditedRow]:
    """Parse the edited CSV exported from the HTML review page.
    Raises ValueError for missing required columns.
    Blank separator rows are tolerated."""
    rows: list[EditedRow] = []
    with csv_path.open(encoding="utf-8-sig") as f:
        reader = csv_lib.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - fieldnames
        if missing:
            raise ValueError(f"{csv_path}: CSV is missing required columns: "
                             f"{sorted(missing)}")
        for raw in reader:
            if not raw.get("path"):
                continue  # blank separator row
            rows.append(EditedRow(
                path=raw["path"],
                content_hash=raw["content_hash"],
                filename=raw["filename"],
                existing_tags=_split_tags(raw.get("existing_tags", "")),
                new_tags=_split_tags(raw.get("new_tags", "")),
                marked_delete=raw.get("delete", "").strip().lower() == "x",
            ))
    return rows


@dataclass(frozen=True)
class ValidationProblem:
    code: str
    message: str
    offending_paths: tuple[str, ...]


def validate_conflict_tag_and_delete(rows: list[EditedRow]) -> list[ValidationProblem]:
    """No row may have both new_tags non-empty AND marked_delete=True."""
    bad = [r for r in rows if r.marked_delete and r.new_tags]
    if not bad:
        return []
    return [ValidationProblem(
        code="CONFLICT_TAG_AND_DELETE",
        message=(f"{len(bad)} row(s) have both new tags AND delete=x. "
                 "A single row cannot be both tagged and deleted."),
        offending_paths=tuple(r.path for r in bad),
    )]


def validate_max_rows(rows: list[EditedRow], max_csv_rows: int) -> list[ValidationProblem]:
    """Total rows in CSV must not exceed the configured batch_size cap.
    Catches hand-edited CSVs that ballooned past the intended batch."""
    if len(rows) <= max_csv_rows:
        return []
    return [ValidationProblem(
        code="EXCEEDS_MAX_ROWS",
        message=(f"{len(rows)} rows in CSV; cap is {max_csv_rows}. "
                 "Reduce the file or raise [media].batch_size."),
        offending_paths=tuple(r.path for r in rows),
    )]


def validate_tag_normalization_and_count(
    rows: list[EditedRow],
) -> list[ValidationProblem]:
    """For each row's new_tags:
      1. Normalize each (strips #, lowercases, spaces->hyphens, validates)
      2. Compute the would-be union with existing_tags
      3. Reject if any normalization fails OR if union size > 20.
    Returns problems with row context."""
    invalid: list[tuple[str, str]] = []  # (path, offending_tag)
    too_many: list[tuple[str, int]] = []
    for r in rows:
        normalized_new: list[str] = []
        for raw in r.new_tags:
            try:
                normalized_new.append(normalize_tag(raw))
            except ValueError:
                invalid.append((r.path, raw))
        if r.marked_delete:
            continue  # tag count check doesn't apply to delete-only rows
        union = set(r.existing_tags) | set(normalized_new)
        if len(union) > 20:
            too_many.append((r.path, len(union)))

    problems: list[ValidationProblem] = []
    if invalid:
        msg_parts = [f"{p}: {t!r}" for p, t in invalid]
        problems.append(ValidationProblem(
            code="INVALID_TAG",
            message=("Tags failing Dropbox's rules (a-z, 0-9, hyphens; 1-32 chars). "
                     "Offending: " + "; ".join(msg_parts)),
            offending_paths=tuple({p for p, _ in invalid}),
        ))
    if too_many:
        msg_parts = [f"{p} would have {n} tags" for p, n in too_many]
        problems.append(ValidationProblem(
            code="TOO_MANY_TAGS",
            message=("Dropbox allows max 20 tags per file. " + "; ".join(msg_parts)),
            offending_paths=tuple(p for p, _ in too_many),
        ))
    return problems


def validate_paths_and_hashes(client, rows: list[EditedRow]) -> list[ValidationProblem]:
    """For each row that will result in a Dropbox action (tag-add or delete),
    verify the path exists AND content_hash matches what's in the CSV.
    Combined into one API call per row (files_get_metadata)."""
    missing: list[str] = []
    changed: list[str] = []
    for row in rows:
        if not row.marked_delete and not row.new_tags:
            continue  # no-op row, skip API call
        try:
            meta = with_retry(lambda r=row: client.files_get_metadata(r.path))
        except ApiError as exc:
            if "not_found" in str(exc.error):
                missing.append(row.path)
                continue
            raise
        if getattr(meta, "content_hash", None) != row.content_hash:
            changed.append(row.path)

    problems: list[ValidationProblem] = []
    if missing:
        problems.append(ValidationProblem(
            code="PATH_NOT_FOUND",
            message=(f"{len(missing)} path(s) no longer exist in Dropbox. "
                     "Re-run get_images.py / get_videos.py to refresh the batch."),
            offending_paths=tuple(missing),
        ))
    if changed:
        problems.append(ValidationProblem(
            code="HASH_CHANGED",
            message=(f"{len(changed)} file(s) have changed since the scan. "
                     "Re-run get_images.py / get_videos.py to refresh."),
            offending_paths=tuple(changed),
        ))
    return problems
