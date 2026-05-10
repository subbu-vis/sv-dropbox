"""Shared helpers for photo/video tagging scripts.

Three sections in one module:
  1. Pure helpers (no Dropbox calls)
  2. Dropbox-using helpers (use a client)
  3. Tag archive I/O (JSON file)
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import PurePosixPath
from typing import Literal

# --- 1. Pure helpers -------------------------------------------------------

# Dropbox native tag rules (per the API docs):
#   - 1 to 32 characters
#   - lowercase a-z, 0-9, and hyphens only
TAG_REGEX = re.compile(r"^[a-z0-9-]{1,32}$")


def classify_media(
    path: str,
    photo_extensions: frozenset[str],
    video_extensions: frozenset[str],
) -> Literal["photo", "video", "other"]:
    """Return media class based on file extension. Case-insensitive.
    `.gitignore` and other dotfiles-without-extension return "other"."""
    name = PurePosixPath(path).name
    if "." not in name:
        return "other"
    ext = name.rsplit(".", 1)[1].lower()
    if not ext:
        return "other"
    if ext in photo_extensions:
        return "photo"
    if ext in video_extensions:
        return "video"
    return "other"


def normalize_tag(raw: str) -> str:
    """Normalize user tag input to Dropbox's native-tag format.
    Strips leading '#', lowercases, replaces runs of whitespace with single hyphens,
    strips surrounding whitespace, then validates.
    Raises ValueError(f"invalid tag: {raw!r}") if the result doesn't match
    a-z0-9- and 1-32 chars."""
    s = raw.strip()
    if s.startswith("#"):
        s = s[1:]
    s = s.lower()
    s = re.sub(r"\s+", "-", s)
    if not TAG_REGEX.fullmatch(s):
        raise ValueError(f"invalid tag: {raw!r} -> {s!r} "
                         f"(must be 1-32 chars of a-z, 0-9, and hyphens)")
    return s


def fold_to_folders(paths: list[str]) -> list[tuple[str, list[str]]]:
    """Group paths by parent folder. Returns list of (folder, [paths_in_folder]),
    sorted by cluster size desc (biggest clusters first). Order within a cluster
    is preserved from input order."""
    clusters: dict[str, list[str]] = defaultdict(list)
    for p in paths:
        parent = p.rsplit("/", 1)[0]
        clusters[parent].append(p)
    return sorted(clusters.items(), key=lambda kv: -len(kv[1]))


# --- 2. Dropbox helpers ----------------------------------------------------

# Map thumbnail widths to Dropbox SDK's ThumbnailSize enum values.
# Built lazily to avoid forcing dropbox import at module-load time in tests
# that don't need it.
_THUMBNAIL_SIZE_BY_WIDTH: dict[int, str] = {
    32: "w32h32",
    64: "w64h64",
    128: "w128h128",
    256: "w256h256",
    480: "w480h320",
    640: "w640h480",
    960: "w960h640",
    1024: "w1024h768",
    2048: "w2048h1536",
}


def fetch_existing_tags(client, paths: list[str]) -> dict[str, list[str]]:
    """Look up native Dropbox tags for each path. Chunks at 100 paths per call
    (Dropbox's max batch size). Empty input returns empty dict, no API calls.

    Returns {path: [tag_text, ...]}. Paths with no tags map to empty list."""
    out: dict[str, list[str]] = {}
    if not paths:
        return out
    for i in range(0, len(paths), 100):
        chunk = paths[i:i + 100]
        result = client.files_tags_get_batch(chunk)
        for pt in result.paths_to_tags:
            out[pt.path] = [t.tag_text for t in pt.tags]
    return out


def fetch_thumbnail(client, path: str, width: int) -> bytes:
    """Fetch JPEG thumbnail bytes at the given width via files_get_thumbnail_v2."""
    if width not in _THUMBNAIL_SIZE_BY_WIDTH:
        raise ValueError(
            f"thumbnail width {width} not supported; allowed: "
            f"{sorted(_THUMBNAIL_SIZE_BY_WIDTH.keys())}"
        )
    # Imported here so the module loads cleanly in unit tests that mock the client.
    from dropbox.files import (
        PathOrLink, ThumbnailFormat, ThumbnailMode, ThumbnailSize,
    )
    size_attr = _THUMBNAIL_SIZE_BY_WIDTH[width]
    _, response = client.files_get_thumbnail_v2(
        resource=PathOrLink.path(path),
        format=ThumbnailFormat.jpeg,
        size=getattr(ThumbnailSize, size_attr),
        mode=ThumbnailMode.strict,
    )
    return response.content


def apply_tags(client, path: str, tags_to_add: list[str]) -> None:
    """Call files_tags_add for each tag. Caller is responsible for deduping
    against existing tags before calling. Each call is independent — if one
    fails, others may still succeed (caller decides whether to abort)."""
    for tag in tags_to_add:
        client.files_tags_add(path, tag)


# --- 3. Tag archive I/O ----------------------------------------------------

import json
from pathlib import Path


ArchiveEntry = dict  # {content_hash, tags, last_updated, [deleted_at]}
Archive = dict[str, ArchiveEntry]


def load_archive(path: Path) -> Archive:
    """Load the JSON archive. Returns empty dict if the file doesn't exist
    (first-run case)."""
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)


def save_archive(path: Path, archive: Archive) -> None:
    """Atomically write the archive. Sorted keys + indent=2 for readable diffs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(archive, f, sort_keys=True, indent=2)


def merge_tagged(
    archive: Archive,
    path: str,
    content_hash: str,
    new_tags: list[str],
    timestamp: str,
) -> None:
    """Union new_tags into archive[path].tags. Updates content_hash and
    last_updated. Creates the entry if it doesn't exist."""
    existing = archive.get(path, {})
    existing_tags = existing.get("tags", [])
    merged = sorted(set(existing_tags) | set(new_tags))
    archive[path] = {
        "content_hash": content_hash,
        "tags": merged,
        "last_updated": timestamp,
    }
    # Preserve deleted_at if it was set previously (file was deleted then restored).
    if "deleted_at" in existing:
        archive[path]["deleted_at"] = existing["deleted_at"]


def merge_deleted(archive: Archive, path: str, timestamp: str) -> None:
    """Mark an existing entry as deleted. No-op if path is not already in archive."""
    if path not in archive:
        return
    archive[path]["deleted_at"] = timestamp
