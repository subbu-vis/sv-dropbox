"""Move user-flagged duplicate files in Dropbox to the recycle bin."""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import dropbox
from dropbox.exceptions import ApiError, DropboxException

from dbx_client import Config, get_client, load_config, load_token, with_retry


@dataclass(frozen=True)
class CsvRow:
    group_id: int
    filename: str
    size_bytes: int
    path: str
    content_hash: str
    last_modified: str
    marked_delete: bool


def parse_csv(csv_path: Path) -> list[CsvRow]:
    rows: list[CsvRow] = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for raw in reader:
            # Skip the blank-separator rows (DictReader yields all-None / all-empty rows).
            if not raw.get("group_id"):
                continue
            rows.append(CsvRow(
                group_id=int(raw["group_id"]),
                filename=raw["filename"],
                size_bytes=int(raw["size_bytes"]),
                path=raw["path"],
                content_hash=raw["content_hash"],
                last_modified=raw["last_modified"],
                marked_delete=raw.get("delete", "").strip().lower() == "x",
            ))
    return rows
