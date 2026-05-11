"""Engine for update_images.py and update_videos.py.

Reads the edited CSV produced by the HTML 'Export' button, validates everything,
prompts the user, applies native Dropbox tags or deletes flagged files,
merges results into the local JSON archive, and writes an audit log.
"""

from __future__ import annotations

import csv as csv_lib
from dataclasses import dataclass, field
from pathlib import Path


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
