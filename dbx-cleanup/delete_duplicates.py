"""Move user-flagged duplicate files in Dropbox to the recycle bin."""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import dropbox
from dropbox.exceptions import ApiError, DropboxException

from dbx_client import Config, get_client, load_config, load_token, with_retry


@dataclass(frozen=True)
class CsvRow:
    group_id: int
    filename: str
    size_bytes: int
    path: str
    content_hash: str
    last_modified: str
    marked_delete: bool


REQUIRED_COLUMNS = {"group_id", "filename", "size_bytes", "path",
                    "content_hash", "last_modified"}


def parse_csv(csv_path: Path) -> list[CsvRow]:
    """Parse a duplicates CSV (header + rows + blank separators between groups).

    Raises ValueError with row context for missing columns or non-int values
    in `group_id`/`size_bytes`. `delete` column is optional; absent or
    whitespace-only values mean "do not delete"."""
    rows: list[CsvRow] = []
    # utf-8-sig transparently strips a BOM if Excel-on-Windows added one.
    with csv_path.open(encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - fieldnames
        if missing:
            raise ValueError(f"{csv_path}: CSV is missing required columns: "
                             f"{sorted(missing)}")
        # DictReader yields header at line 1, first data row at line 2.
        for lineno, raw in enumerate(reader, start=2):
            if not raw.get("group_id"):
                continue  # blank separator row
            try:
                rows.append(CsvRow(
                    group_id=int(raw["group_id"]),
                    filename=raw["filename"],
                    size_bytes=int(raw["size_bytes"]),
                    path=raw["path"],
                    content_hash=raw["content_hash"],
                    last_modified=raw["last_modified"],
                    marked_delete=raw.get("delete", "").strip().lower() == "x",
                ))
            except (ValueError, TypeError) as exc:
                raise ValueError(f"{csv_path} line {lineno}: {exc}") from exc
    return rows


@dataclass(frozen=True)
class ValidationProblem:
    code: str       # e.g. "GROUP_FULLY_MARKED"
    message: str    # human-readable
    offending_paths: tuple[str, ...]


def validate_groups_have_survivor(rows: list[CsvRow]) -> list[ValidationProblem]:
    """Validation B: each group must keep at least one row not marked for delete."""
    problems: list[ValidationProblem] = []
    by_group: dict[int, list[CsvRow]] = {}
    for r in rows:
        by_group.setdefault(r.group_id, []).append(r)
    for gid, group_rows in by_group.items():
        if all(r.marked_delete for r in group_rows):
            problems.append(ValidationProblem(
                code="GROUP_FULLY_MARKED",
                message=(f"Group {gid} has every row marked 'x'. Refusing to delete "
                         f"all copies of a file."),
                offending_paths=tuple(r.path for r in group_rows),
            ))
    return problems


def validate_max_rows(rows: list[CsvRow], max_csv_rows: int) -> list[ValidationProblem]:
    """Validation C: total marked-delete count must not exceed max_csv_rows."""
    marked = [r for r in rows if r.marked_delete]
    if len(marked) > max_csv_rows:
        return [ValidationProblem(
            code="EXCEEDS_MAX_ROWS",
            message=(f"{len(marked)} rows are marked for deletion; daily cap is "
                     f"{max_csv_rows}. Reduce the marked rows and re-run."),
            offending_paths=tuple(r.path for r in marked),
        )]
    return []
