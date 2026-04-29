"""Scan Dropbox for duplicate files and write a CSV ranked by wasted space."""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import dropbox
from dropbox.exceptions import AuthError
from dropbox.files import FileMetadata, ListFolderResult

from dbx_client import Config, MissingTokenError, get_client, load_config, load_token, with_retry


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


def scan_dropbox(
    client: dropbox.Dropbox,
    root: str,
    config: Config,
    owner_account_id: str,
) -> list[FileEntry]:
    """Walk Dropbox starting at `root`, applying skip rules, returning kept entries.
    Stops scanning when running totals indicate >= early_exit_row_threshold
    duplicate rows have been found."""
    kept: list[FileEntry] = []
    files_scanned = 0

    # Dropbox's recursive list returns from the given root. "/" is special-cased to "".
    list_path = "" if root == "/" else root.rstrip("/")

    result: ListFolderResult = with_retry(
        lambda: client.files_list_folder(list_path, recursive=True)
    )
    while True:
        for entry in result.entries:
            if not isinstance(entry, FileMetadata):
                continue
            # Skip files Dropbox can't fully describe yet: incomplete uploads have
            # content_hash=None; some surfaced types (e.g. Paper docs) have
            # server_modified=None. Both are required for downstream work.
            if entry.content_hash is None or entry.server_modified is None:
                continue
            files_scanned += 1
            if should_skip_file(
                entry,
                min_file_size_bytes=config.min_file_size_bytes,
                skip_hidden=config.skip_hidden,
                skip_shared_not_owned=config.skip_shared_not_owned,
                owner_account_id=owner_account_id,
            ):
                continue
            kept.append(FileEntry(
                name=entry.name,
                path=entry.path_display,
                size=entry.size,
                content_hash=entry.content_hash,
                server_modified=entry.server_modified.isoformat(),
            ))

            if files_scanned % 1000 == 0:
                groups_so_far = group_by_hash(kept)
                dup_rows = sum(len(g) for g in groups_so_far.values())
                print(f"Scanned {files_scanned} files, "
                      f"{len(groups_so_far)} duplicate groups "
                      f"({dup_rows} rows)...")
                if dup_rows >= config.early_exit_row_threshold:
                    print(f"Hit early-exit threshold ({config.early_exit_row_threshold}); "
                          "stopping scan.")
                    return kept

        if not result.has_more:
            break
        next_cursor = result.cursor
        result = with_retry(lambda c=next_cursor: client.files_list_folder_continue(c))

    print(f"Scan complete. Total files scanned: {files_scanned}.")
    return kept


def main() -> int:
    parser = argparse.ArgumentParser(description="Find Dropbox duplicates.")
    parser.add_argument("--config", default="config.ini",
                        help="Path to config.ini (default: config.ini)")
    parser.add_argument("--root", default="/",
                        help="Dropbox path to scan (default: /)")
    args = parser.parse_args()

    # Normalize --root: strip whitespace, ensure leading slash so users can pass
    # "test-duplicates/" or "/test-duplicates" interchangeably.
    root = args.root.strip()
    if not root:
        print("--root cannot be empty", file=sys.stderr)
        return 1
    if not root.startswith("/"):
        root = "/" + root

    try:
        config = load_config(Path(args.config))
        token = load_token()
        client = get_client(token)
    except FileNotFoundError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 1
    except MissingTokenError as exc:
        print(f"Token error: {exc}", file=sys.stderr)
        return 1
    except AuthError as exc:
        print(f"Dropbox auth failed: {exc}. See README for token regeneration.",
              file=sys.stderr)
        return 1

    owner = client.users_get_current_account().account_id

    entries = scan_dropbox(client, root, config, owner)
    groups = group_by_hash(entries)
    selected = select_top_groups(groups, config.max_csv_rows)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
    out_path = config.csv_output_dir / f"duplicates-{timestamp}.csv"
    write_csv(selected, out_path)

    total_rows = sum(len(g) for g in selected)
    total_wasted = sum(_wasted_bytes(g) for g in selected)
    print(f"\nWrote {len(selected)} groups, {total_rows} rows to {out_path}")
    print(f"Total wasted space across selected groups: {total_wasted:,} bytes "
          f"({total_wasted / 1024 / 1024:.2f} MB)")
    dropped = len(groups) - len(selected)
    if dropped:
        deferred_waste = sum(_wasted_bytes(g) for g in groups.values()) - total_wasted
        print(f"({dropped} more groups deferred to next run; "
              f"~{deferred_waste / 1024 / 1024:.1f} MB additional wasted space)")
    if not selected:
        print("(No duplicate groups found above the configured threshold.)")
    else:
        print("Mark 'x' in the delete column for files to remove, then run:")
        print(f"  python delete_duplicates.py --csv {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
