"""Shared helpers: config loading, Dropbox auth, retry wrapper."""

from __future__ import annotations

import configparser
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeVar

import dropbox
from dotenv import load_dotenv
from dropbox.exceptions import AuthError, RateLimitError

T = TypeVar("T")


@dataclass(frozen=True)
class Config:
    min_file_size_bytes: int
    skip_shared_not_owned: bool
    skip_hidden: bool
    early_exit_row_threshold: int
    max_csv_rows: int
    csv_output_dir: Path
    log_dir: Path
    # Folder paths to skip during scan. Stored normalized (leading slash, no
    # trailing slash, lowercase) for efficient case-insensitive prefix match.
    ignored_folders: tuple[str, ...]


def _parse_ignored_folders(raw: str) -> tuple[str, ...]:
    """Parse a multi-line INI value into a normalized tuple of folder paths."""
    out: list[str] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if not line.startswith("/"):
            line = "/" + line
        line = line.rstrip("/")
        out.append(line.lower())
    return tuple(out)


def load_config(path: Path) -> Config:
    parser = configparser.ConfigParser()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    parser.read(path)
    scan = parser["scan"]
    paths = parser["paths"]
    return Config(
        min_file_size_bytes=scan.getint("min_file_size_bytes"),
        skip_shared_not_owned=scan.getboolean("skip_shared_not_owned"),
        skip_hidden=scan.getboolean("skip_hidden"),
        early_exit_row_threshold=scan.getint("early_exit_row_threshold"),
        max_csv_rows=scan.getint("max_csv_rows"),
        csv_output_dir=Path(paths["csv_output_dir"]),
        log_dir=Path(paths["log_dir"]),
        ignored_folders=_parse_ignored_folders(scan.get("ignored_folders", "")),
    )


class MissingTokenError(RuntimeError):
    """Raised when the Dropbox access token is missing or empty."""


def load_token(env_path: Path | None = None) -> str:
    if env_path is not None:
        load_dotenv(env_path)
    else:
        load_dotenv()
    token = os.environ.get("DROPBOX_ACCESS_TOKEN", "").strip()
    if not token:
        raise MissingTokenError(
            "DROPBOX_ACCESS_TOKEN is not set. "
            "See README for steps to generate a personal access token "
            "in the Dropbox App Console and add it to .env."
        )
    return token


def get_client(token: str) -> dropbox.Dropbox:
    """Build a Dropbox SDK client and verify the token by calling users_get_current_account."""
    client = dropbox.Dropbox(token)
    account = client.users_get_current_account()
    print(f"Connected to Dropbox as {account.email}")
    return client


def with_retry(call: Callable[[], T], max_attempts: int = 3) -> T:
    """Run `call` with retry on RateLimitError. AuthError is re-raised immediately.

    The dropbox SDK puts the server-recommended retry delay on RateLimitError.backoff
    (seconds). We honor that; default to 1s if absent or non-positive."""
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
    last_error: RateLimitError | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return call()
        except RateLimitError as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            backoff = max(exc.backoff or 1, 1)
            print(f"Rate limited (attempt {attempt}/{max_attempts}); sleeping {backoff}s")
            time.sleep(backoff)
        except AuthError:
            raise
    assert last_error is not None
    raise last_error


# Thumbnail widths that Dropbox's files_get_thumbnail_v2 supports.
ALLOWED_THUMBNAIL_WIDTHS = frozenset({32, 64, 128, 256, 480, 640, 960, 1024, 2048})


@dataclass(frozen=True)
class MediaConfig:
    photo_extensions: frozenset[str]
    video_extensions: frozenset[str]
    batch_size: int
    thumbnail_width: int
    tag_archive_path: Path
    csv_output_dir: Path
    log_dir: Path
    ignored_folders: tuple[str, ...]


def _parse_extensions(raw: str, field_name: str) -> frozenset[str]:
    """Parse a comma-separated extension list. Lowercases, strips, rejects empty input
    and any entry with a leading dot."""
    items = [s.strip().lower() for s in raw.split(",")]
    items = [s for s in items if s]
    if not items:
        raise ValueError(f"{field_name} must not be empty")
    for ext in items:
        if ext.startswith("."):
            raise ValueError(f"{field_name} entries must not start with '.': {ext!r}")
    return frozenset(items)


def load_media_config(path: Path) -> MediaConfig:
    parser = configparser.ConfigParser()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    parser.read(path)
    media = parser["media"]
    paths = parser["paths"]

    thumbnail_width = media.getint("thumbnail_width")
    if thumbnail_width not in ALLOWED_THUMBNAIL_WIDTHS:
        allowed = ", ".join(str(w) for w in sorted(ALLOWED_THUMBNAIL_WIDTHS))
        raise ValueError(f"thumbnail_width must be one of: {allowed}; got {thumbnail_width}")

    batch_size = media.getint("batch_size")
    if batch_size <= 0:
        raise ValueError(f"batch_size must be positive, got {batch_size}")

    return MediaConfig(
        photo_extensions=_parse_extensions(media.get("photo_extensions", ""), "photo_extensions"),
        video_extensions=_parse_extensions(media.get("video_extensions", ""), "video_extensions"),
        batch_size=batch_size,
        thumbnail_width=thumbnail_width,
        tag_archive_path=Path(media["tag_archive_path"]),
        csv_output_dir=Path(paths["csv_output_dir"]),
        log_dir=Path(paths["log_dir"]),
        ignored_folders=_parse_ignored_folders(media.get("ignored_folders", "")),
    )
