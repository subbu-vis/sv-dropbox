"""Walk Dropbox and write a CSV of every folder's total size (descending).

Read-only: no Dropbox modifications. Each file's bytes are attributed to every
named ancestor folder (e.g., a 5 MB file under /Photos/2019/raw counts toward
/Photos, /Photos/2019, and /Photos/2019/raw)."""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

import dropbox
from dropbox.exceptions import AuthError
from dropbox.files import FileMetadata, ListFolderResult

from dbx_client import MissingTokenError, get_client, load_config, load_token, with_retry


CSV_HEADER = ["folder", "size_mb", "file_count"]


def iter_ancestors(path: str) -> Iterable[str]:
    """Yield named folder ancestors of a file path, excluding root.

    Examples:
        '/a/b/c/file.txt' -> '/a', '/a/b', '/a/b/c'
        '/photos/img.jpg' -> '/photos'
        '/file.txt'       -> (nothing — top-level file has no folder ancestor)
    """
    parts = path.split("/")
    # parts[0] is the empty string before the leading "/"; parts[-1] is the
    # filename. Slice [:i] for i in 2..len(parts)-1 produces each ancestor.
    for i in range(2, len(parts)):
        yield "/".join(parts[:i])


def aggregate_folder_sizes(
    files: Iterable[tuple[str, int]],
) -> dict[str, tuple[int, int]]:
    """Sum bytes and file counts for each ancestor folder.

    Returns dict[folder_path -> (total_bytes, file_count)]."""
    bytes_by: dict[str, int] = defaultdict(int)
    count_by: dict[str, int] = defaultdict(int)
    for path, size in files:
        for ancestor in iter_ancestors(path):
            bytes_by[ancestor] += size
            count_by[ancestor] += 1
    return {f: (bytes_by[f], count_by[f]) for f in bytes_by}


def write_csv(aggregated: dict[str, tuple[int, int]], out_path: Path) -> None:
    """Write folder/size_mb/file_count rows sorted by raw bytes desc.

    Sort key uses raw bytes (not the rounded MB) so two folders that both round
    up to 1 MB still have a stable, useful order. size_mb is ceil(bytes / 1MB)
    so anything above 0 bytes shows at least 1 MB."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(
        ((folder, bytes_, count) for folder, (bytes_, count) in aggregated.items()),
        key=lambda r: r[1],
        reverse=True,
    )
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for folder, bytes_, count in rows:
            mb = math.ceil(bytes_ / (1024 * 1024)) if bytes_ > 0 else 0
            writer.writerow([folder, mb, count])


def walk_dropbox_files(client: dropbox.Dropbox) -> Iterable[tuple[str, int]]:
    """Walk every file in the account; yield (path_display, size). No filtering."""
    files_seen = 0
    result: ListFolderResult = with_retry(
        lambda: client.files_list_folder("", recursive=True)
    )
    while True:
        for entry in result.entries:
            if not isinstance(entry, FileMetadata):
                continue
            if entry.size is None or entry.path_display is None:
                continue
            files_seen += 1
            yield entry.path_display, entry.size
            if files_seen % 1000 == 0:
                print(f"  scanned {files_seen} files...")
        if not result.has_more:
            break
        next_cursor = result.cursor
        result = with_retry(lambda c=next_cursor: client.files_list_folder_continue(c))
    print(f"Scan complete. Total files scanned: {files_seen}.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List Dropbox folders by total size (descending).")
    parser.add_argument("--config", default="config.ini",
                        help="Path to config.ini (default: config.ini)")
    args = parser.parse_args()

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

    print("Walking all files (this can take a few minutes on large accounts)...")
    aggregated = aggregate_folder_sizes(walk_dropbox_files(client))

    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
    out_path = config.csv_output_dir / f"dbx-file-size-{timestamp}.csv"
    write_csv(aggregated, out_path)

    print(f"\nWrote {len(aggregated)} folders to {out_path}")
    print("Rows are sorted by size desc. The top-level folders show your "
          "total Dropbox footprint per top-level area.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
