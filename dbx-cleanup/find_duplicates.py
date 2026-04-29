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
    return (len(group) - 1) * group[0].size


def select_top_groups(
    groups: dict[str, list[FileEntry]],
    max_csv_rows: int,
) -> list[list[FileEntry]]:
    """Sort groups by wasted bytes desc, greedily take whole groups whose
    cumulative row count stays <= max_csv_rows. Never split a group."""
    ranked = sorted(groups.values(), key=_wasted_bytes, reverse=True)
    out: list[list[FileEntry]] = []
    rows_used = 0
    for group in ranked:
        if rows_used + len(group) <= max_csv_rows:
            out.append(group)
            rows_used += len(group)
    return out
