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
