"""Walk Dropbox and print total photo + video counts.

Read-only. Uses [media].photo_extensions / video_extensions to classify each
file by its extension. [media].ignored_folders is honored. Hidden files
(any path segment starting with '.') are skipped, matching the [scan] convention.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dropbox.exceptions import AuthError
from dropbox.files import FileMetadata, ListFolderResult

from dbx_client import MissingTokenError, get_client, load_media_config, load_token, with_retry
from dbx_media import classify_media


def main() -> int:
    parser = argparse.ArgumentParser(description="Count photos and videos in Dropbox.")
    parser.add_argument("--config", default="config.ini")
    parser.add_argument("--root", default="/",
                        help="Dropbox path to scan (default: /)")
    args = parser.parse_args()

    root = args.root.strip()
    if not root.startswith("/"):
        root = "/" + root

    try:
        mc = load_media_config(Path(args.config))
        token = load_token()
        client = get_client(token)
    except FileNotFoundError as exc:
        print(f"Config error: {exc}", file=sys.stderr); return 1
    except MissingTokenError as exc:
        print(f"Token error: {exc}", file=sys.stderr); return 1
    except AuthError as exc:
        print(f"Dropbox auth failed: {exc}. See README.", file=sys.stderr); return 1

    list_path = "" if root == "/" else root.rstrip("/")
    photos = 0
    videos = 0
    scanned = 0

    result: ListFolderResult = with_retry(
        lambda: client.files_list_folder(list_path, recursive=True)
    )
    while True:
        for entry in result.entries:
            if not isinstance(entry, FileMetadata):
                continue
            scanned += 1
            # Skip hidden segments.
            if any(seg.startswith(".") for seg in entry.path_display.split("/")):
                continue
            # Skip [media].ignored_folders.
            path_lower = entry.path_display.lower()
            if any(path_lower == f or path_lower.startswith(f + "/")
                   for f in mc.ignored_folders):
                continue
            kind = classify_media(entry.path_display, mc.photo_extensions, mc.video_extensions)
            if kind == "photo":
                photos += 1
            elif kind == "video":
                videos += 1
            if scanned % 1000 == 0:
                print(f"  scanned {scanned} files...", file=sys.stderr)
        if not result.has_more:
            break
        cursor = result.cursor
        result = with_retry(lambda c=cursor: client.files_list_folder_continue(c))

    print(f"Photos: {photos:,}")
    print(f"Videos: {videos:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
