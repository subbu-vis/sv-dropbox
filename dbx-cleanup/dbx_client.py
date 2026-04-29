"""Shared helpers: config loading, Dropbox auth, retry wrapper."""

from __future__ import annotations

import configparser
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, TypeVar

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
