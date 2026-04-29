"""Shared helpers: config loading, Dropbox auth, retry wrapper."""

from __future__ import annotations

import configparser
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, TypeVar

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
    (seconds). We honor that; default to 1s if absent."""
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return call()
        except RateLimitError as exc:
            last_error = exc
            backoff = getattr(exc, "backoff", None) or 1
            print(f"Rate limited (attempt {attempt}/{max_attempts}); sleeping {backoff}s")
            time.sleep(backoff)
        except AuthError:
            raise
    assert last_error is not None
    raise last_error
