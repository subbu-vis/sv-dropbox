"""Scan Dropbox for duplicate files and write a CSV ranked by wasted space."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class FileEntry:
    name: str
    path: str  # Dropbox path_display (case-preserving); use for CSV + delete API calls
    size: int
    content_hash: str
    server_modified: str


def should_skip_file(
    meta: Any,
    *,
    min_file_size_bytes: int,
    skip_hidden: bool,
    skip_shared_not_owned: bool,
    owner_account_id: str,
) -> bool:
    if meta.size == 0:
        return True
    if meta.size < min_file_size_bytes:
        return True
    if skip_hidden:
        for segment in meta.path_display.split("/"):
            if segment.startswith("."):
                return True
    if skip_shared_not_owned and getattr(meta, "sharing_info", None) is not None:
        info = meta.sharing_info
        # FileMetadata.sharing_info has modified_by (account_id of last modifier)
        # and the file's parent_shared_folder_id. We treat any file under a shared
        # folder we did NOT modify last as not-owned. This is a heuristic; the
        # smoke test verifies it for the common cases.
        modified_by = getattr(info, "modified_by", None)
        if modified_by and modified_by != owner_account_id:
            return True
    return False


def group_by_hash(entries: Iterable[FileEntry]) -> dict[str, list[FileEntry]]:
    """Group entries by content_hash; drop singletons."""
    groups: dict[str, list[FileEntry]] = {}
    for entry in entries:
        groups.setdefault(entry.content_hash, []).append(entry)
    return {h: g for h, g in groups.items() if len(g) > 1}


def _wasted_bytes(group: list[FileEntry]) -> int:
    # All entries in a group share the same content_hash, which by Dropbox's
    # definition means identical bytes -> identical size. Assert it so a bad
    # mock or upstream bug fails loudly rather than producing wrong rankings.
    assert all(e.size == group[0].size for e in group), "mixed-size group"
    return (len(group) - 1) * group[0].size


def select_top_groups(
    groups: dict[str, list[FileEntry]],
    max_csv_rows: int,
) -> list[list[FileEntry]]:
    """Sort groups by wasted bytes desc (with group-size as tie-breaker),
    greedily take whole groups whose cumulative row count stays <= max_csv_rows.
    Never splits a group.

    Greedy is intentionally not optimal: it can miss a better packing of two
    smaller groups when a bigger group fits but uses up the budget. For the
    realistic case (surface the largest wasters in <=100 rows), greedy is fine."""
    if max_csv_rows <= 0:
        raise ValueError(f"max_csv_rows must be positive, got {max_csv_rows}")
    ranked = sorted(groups.values(), key=lambda g: (_wasted_bytes(g), len(g)),
                    reverse=True)
    out: list[list[FileEntry]] = []
    rows_used = 0
    for group in ranked:
        if rows_used + len(group) <= max_csv_rows:
            out.append(group)
            rows_used += len(group)
    return out


CSV_HEADER = ["group_id", "filename", "size_bytes", "path", "content_hash",
              "last_modified", "delete"]


def write_csv(groups: list[list[FileEntry]], out_path: Path) -> None:
    """Write `groups` as a CSV at `out_path`. Header row + one row per FileEntry,
    with a blank row separator between groups. `delete` column is empty for the
    user to fill in. Empty `groups` produces a header-only file."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for idx, group in enumerate(groups, start=1):
            # Case-fold the sort key so siblings with mixed-case parent folders
            # (Dropbox is case-insensitive) appear adjacent.
            for entry in sorted(group, key=lambda e: e.path.lower()):
                writer.writerow([
                    idx, entry.name, entry.size, entry.path,
                    entry.content_hash, entry.server_modified, "",
                ])
            # blank row between groups (but not after the last one)
            if idx < len(groups):
                writer.writerow([])
