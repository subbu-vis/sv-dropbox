# Dropbox Cleanup — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build two Python scripts (`find_duplicates.py`, `delete_duplicates.py`) that locate byte-identical duplicate files in Dropbox and safely move flagged copies to the recycle bin, with strict pre-flight validations.

**Architecture:** Two CLI scripts plus a shared `dbx_client.py` helper. The find script walks Dropbox via the official SDK, groups files by `content_hash`, ranks by wasted space, and writes a CSV ≤100 rows. The user marks deletions in a `delete` column. The delete script pre-validates the entire CSV (existence, hash unchanged, max rows, never-delete-all-copies) before any API destructive call, then invokes `files_delete_v2` per row with continue-on-error and an audit log.

**Tech Stack:** Python 3.10+, `dropbox` SDK 12+, `python-dotenv`, `pytest` for tests.

**Spec:** `docs/superpowers/specs/2026-04-28-dropbox-cleanup-design.md`

---

## File Structure

```
dbx-cleanup/
├── README.md
├── requirements.txt
├── .env.example
├── config.ini                  # min_file_size_bytes=102400, max_csv_rows=100, ...
├── config.test.ini             # min_file_size_bytes=1024, smaller thresholds
├── dbx_client.py               # load_config, load_token, get_client, with_retry
├── find_duplicates.py          # main + scan/filter/group/rank/write logic
├── delete_duplicates.py        # main + parse/validate/execute logic
├── seed_test_data.py           # uploads known files to /test-duplicates/
├── tests/
│   ├── __init__.py
│   ├── conftest.py             # shared pytest fixtures (mock Dropbox client)
│   ├── test_dbx_client.py
│   ├── test_find_duplicates.py
│   └── test_delete_duplicates.py
├── output/                     # generated CSVs (gitignored)
└── logs/                       # audit + error logs (gitignored)
```

Each module has one clear responsibility:

- `dbx_client.py` — config + auth + retry. No Dropbox business logic.
- `find_duplicates.py` — pure functions for filter/group/rank/csv-write + a `main()` that orchestrates the scan.
- `delete_duplicates.py` — pure functions for parse/validate/log + a `main()` that orchestrates validation+execution.
- `seed_test_data.py` — one-shot test fixture uploader.

Pure functions take data in and return data out; orchestration code in `main()` calls Dropbox. This keeps the testable surface large and the mocked surface small.

---

### Task 1: Project skeleton, config files, dependencies

**Files:**
- Create: `dbx-cleanup/requirements.txt`
- Create: `dbx-cleanup/.env.example`
- Create: `dbx-cleanup/config.ini`
- Create: `dbx-cleanup/config.test.ini`
- Create: `dbx-cleanup/tests/__init__.py` (empty)
- Create: `dbx-cleanup/tests/conftest.py` (empty for now)

- [ ] **Step 1: Create the directory tree**

Run:
```bash
mkdir -p <repo>/dbx-cleanup/tests
mkdir -p <repo>/dbx-cleanup/output
mkdir -p <repo>/dbx-cleanup/logs
```

- [ ] **Step 2: Write `requirements.txt`**

```
dropbox>=12.0.0
python-dotenv>=1.0.0
pytest>=7.4.0
pytest-mock>=3.12.0
```

- [ ] **Step 3: Write `.env.example`**

```
# Dropbox personal access token. Generate at https://www.dropbox.com/developers/apps
# Required scopes: files.metadata.read, files.content.read, files.content.write
DROPBOX_ACCESS_TOKEN=
```

- [ ] **Step 4: Write `config.ini`**

```ini
[scan]
min_file_size_bytes = 102400
skip_shared_not_owned = true
skip_hidden = true
early_exit_row_threshold = 1000
max_csv_rows = 100

[paths]
csv_output_dir = ./output
log_dir = ./logs
```

- [ ] **Step 5: Write `config.test.ini`**

```ini
[scan]
min_file_size_bytes = 1024
skip_shared_not_owned = true
skip_hidden = true
early_exit_row_threshold = 50
max_csv_rows = 100

[paths]
csv_output_dir = ./output
log_dir = ./logs
```

- [ ] **Step 6: Create empty test scaffolding**

Create `dbx-cleanup/tests/__init__.py` (empty).
Create `dbx-cleanup/tests/conftest.py` (empty — fixtures added in later tasks).

- [ ] **Step 7: Verify dependencies install in a fresh venv**

```bash
cd <repo>/dbx-cleanup
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -c "import dropbox; import dotenv; import pytest; print('ok')"
```

Expected: `ok` printed.

- [ ] **Step 8: Commit**

```bash
cd <repo>
git add dbx-cleanup/requirements.txt dbx-cleanup/.env.example dbx-cleanup/config.ini dbx-cleanup/config.test.ini dbx-cleanup/tests/__init__.py dbx-cleanup/tests/conftest.py
git commit -m "scaffold: dbx-cleanup project skeleton and config"
```

---

### Task 2: `dbx_client.py` — config loading

**Files:**
- Create: `dbx-cleanup/dbx_client.py`
- Create: `dbx-cleanup/tests/test_dbx_client.py`

- [ ] **Step 1: Write the failing test for `load_config`**

Add to `tests/test_dbx_client.py`:

```python
import configparser
from pathlib import Path

import pytest

from dbx_client import load_config


def test_load_config_reads_scan_and_paths(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\n"
        "min_file_size_bytes = 102400\n"
        "skip_shared_not_owned = true\n"
        "skip_hidden = true\n"
        "early_exit_row_threshold = 1000\n"
        "max_csv_rows = 100\n"
        "\n"
        "[paths]\n"
        "csv_output_dir = ./output\n"
        "log_dir = ./logs\n"
    )

    cfg = load_config(cfg_path)

    assert cfg.min_file_size_bytes == 102400
    assert cfg.skip_shared_not_owned is True
    assert cfg.skip_hidden is True
    assert cfg.early_exit_row_threshold == 1000
    assert cfg.max_csv_rows == 100
    assert cfg.csv_output_dir == Path("./output")
    assert cfg.log_dir == Path("./logs")
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd <repo>/dbx-cleanup
source .venv/bin/activate
PYTHONPATH=. pytest tests/test_dbx_client.py::test_load_config_reads_scan_and_paths -v
```

Expected: FAIL with `ImportError: cannot import name 'load_config'`.

- [ ] **Step 3: Implement `load_config`**

Create `dbx-cleanup/dbx_client.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
PYTHONPATH=. pytest tests/test_dbx_client.py::test_load_config_reads_scan_and_paths -v
```

Expected: PASS.

- [ ] **Step 5: Add a test for missing config file**

Add to `tests/test_dbx_client.py`:

```python
def test_load_config_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "nope.ini"
    with pytest.raises(FileNotFoundError):
        load_config(missing)
```

Run: `PYTHONPATH=. pytest tests/test_dbx_client.py -v`. Expected: both PASS.

- [ ] **Step 6: Commit**

```bash
cd <repo>
git add dbx-cleanup/dbx_client.py dbx-cleanup/tests/test_dbx_client.py
git commit -m "feat(dbx-cleanup): add config loader"
```

---

### Task 3: `dbx_client.py` — token loading + client builder

**Files:**
- Modify: `dbx-cleanup/dbx_client.py`
- Modify: `dbx-cleanup/tests/test_dbx_client.py`

- [ ] **Step 1: Write the failing test for `load_token`**

Append to `tests/test_dbx_client.py`:

```python
from dbx_client import MissingTokenError, load_token


def test_load_token_returns_value(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("DROPBOX_ACCESS_TOKEN=sl.test123\n")
    monkeypatch.chdir(tmp_path)
    assert load_token(env_file) == "sl.test123"


def test_load_token_missing_raises_with_helpful_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("DROPBOX_ACCESS_TOKEN=\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DROPBOX_ACCESS_TOKEN", raising=False)
    with pytest.raises(MissingTokenError) as excinfo:
        load_token(env_file)
    assert "DROPBOX_ACCESS_TOKEN" in str(excinfo.value)
    assert "README" in str(excinfo.value)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=. pytest tests/test_dbx_client.py -v
```

Expected: 2 new tests FAIL with `ImportError`.

- [ ] **Step 3: Implement `load_token` and `MissingTokenError`**

Append to `dbx-cleanup/dbx_client.py`:

```python
from dotenv import load_dotenv


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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=. pytest tests/test_dbx_client.py -v
```

Expected: all PASS.

- [ ] **Step 5: Implement `get_client`**

Append to `dbx-cleanup/dbx_client.py`:

```python
import dropbox


def get_client(token: str) -> dropbox.Dropbox:
    """Build a Dropbox SDK client and verify the token by calling users_get_current_account."""
    client = dropbox.Dropbox(token)
    account = client.users_get_current_account()
    print(f"Connected to Dropbox as {account.email}")
    return client
```

(Not unit-tested — covered by smoke test in Task 14.)

- [ ] **Step 6: Commit**

```bash
git add dbx-cleanup/dbx_client.py dbx-cleanup/tests/test_dbx_client.py
git commit -m "feat(dbx-cleanup): add token loader and client builder"
```

---

### Task 4: `dbx_client.py` — retry wrapper

**Files:**
- Modify: `dbx-cleanup/dbx_client.py`
- Modify: `dbx-cleanup/tests/test_dbx_client.py`

- [ ] **Step 1: Write the failing test for `with_retry` on rate limits**

Append to `tests/test_dbx_client.py`:

```python
from unittest.mock import MagicMock

from dropbox.exceptions import AuthError, RateLimitError

from dbx_client import with_retry


def _rate_limit_error(backoff: float) -> RateLimitError:
    """RateLimitError(request_id, error, backoff) — backoff is seconds to wait."""
    return RateLimitError("req-id", MagicMock(), backoff)


def test_with_retry_retries_on_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_calls: list[float] = []
    monkeypatch.setattr("dbx_client.time.sleep", lambda s: sleep_calls.append(s))

    call = MagicMock()
    call.side_effect = [_rate_limit_error(2.0), "ok"]
    result = with_retry(call)
    assert result == "ok"
    assert sleep_calls == [2.0]


def test_with_retry_gives_up_after_max_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("dbx_client.time.sleep", lambda s: None)
    call = MagicMock()
    call.side_effect = _rate_limit_error(1.0)
    with pytest.raises(RateLimitError):
        with_retry(call, max_attempts=3)
    assert call.call_count == 3


def test_with_retry_does_not_retry_auth_error() -> None:
    call = MagicMock(side_effect=AuthError("req-id", "user-message"))
    with pytest.raises(AuthError):
        with_retry(call)
    assert call.call_count == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=. pytest tests/test_dbx_client.py -v
```

Expected: 3 new tests FAIL with `ImportError`.

- [ ] **Step 3: Implement `with_retry`**

Append to `dbx-cleanup/dbx_client.py`:

```python
from dropbox.exceptions import AuthError, RateLimitError


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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=. pytest tests/test_dbx_client.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/dbx_client.py dbx-cleanup/tests/test_dbx_client.py
git commit -m "feat(dbx-cleanup): add rate-limit retry wrapper"
```

---

### Task 5: `find_duplicates.py` — file-skip filter

**Files:**
- Create: `dbx-cleanup/find_duplicates.py`
- Create: `dbx-cleanup/tests/test_find_duplicates.py`

- [ ] **Step 1: Write the failing test for `should_skip_file`**

Create `tests/test_find_duplicates.py`:

```python
from dataclasses import dataclass

import pytest

from find_duplicates import FileEntry, should_skip_file


@dataclass(frozen=True)
class FakeMeta:
    """Subset of dropbox.files.FileMetadata fields we care about."""
    name: str
    path_display: str
    size: int
    content_hash: str
    server_modified: str
    sharing_info: object | None = None


@pytest.mark.parametrize(
    "size, path, expected",
    [
        (50_000, "/Photos/img.jpg", True),     # below 100 KB threshold
        (200_000, "/Photos/img.jpg", False),   # above threshold
        (200_000, "/Photos/.hidden", True),    # hidden file
        (200_000, "/.dropbox.cache/x", True),  # hidden folder segment
        (0, "/Photos/empty.txt", True),        # empty file
    ],
)
def test_should_skip_file_size_and_hidden(size: int, path: str, expected: bool) -> None:
    meta = FakeMeta(name=path.rsplit("/", 1)[-1], path_display=path, size=size,
                    content_hash="abc", server_modified="2024-01-01T00:00:00Z")
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=True,
                            skip_shared_not_owned=True, owner_account_id="self") is expected
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH=. pytest tests/test_find_duplicates.py -v
```

Expected: FAIL with `ImportError: cannot import name 'should_skip_file'`.

- [ ] **Step 3: Implement `should_skip_file` and `FileEntry`**

Create `dbx-cleanup/find_duplicates.py`:

```python
"""Scan Dropbox for duplicate files and write a CSV ranked by wasted space."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class FileEntry:
    name: str
    path: str
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
PYTHONPATH=. pytest tests/test_find_duplicates.py -v
```

Expected: PASS.

- [ ] **Step 5: Add test for shared-folder ownership filter**

Append to `tests/test_find_duplicates.py`:

```python
@dataclass(frozen=True)
class FakeShareInfo:
    modified_by: str | None


def test_should_skip_shared_not_owned() -> None:
    meta = FakeMeta(
        name="x.pdf", path_display="/Shared/x.pdf", size=200_000,
        content_hash="h", server_modified="2024-01-01T00:00:00Z",
        sharing_info=FakeShareInfo(modified_by="other-account"),
    )
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=True,
                            skip_shared_not_owned=True, owner_account_id="self") is True


def test_should_keep_shared_owned_by_self() -> None:
    meta = FakeMeta(
        name="x.pdf", path_display="/Shared/x.pdf", size=200_000,
        content_hash="h", server_modified="2024-01-01T00:00:00Z",
        sharing_info=FakeShareInfo(modified_by="self"),
    )
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=True,
                            skip_shared_not_owned=True, owner_account_id="self") is False
```

Run: `PYTHONPATH=. pytest tests/test_find_duplicates.py -v`. Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add dbx-cleanup/find_duplicates.py dbx-cleanup/tests/test_find_duplicates.py
git commit -m "feat(find_duplicates): add file-skip filter"
```

---

### Task 6: `find_duplicates.py` — group by hash, rank, select top groups

**Files:**
- Modify: `dbx-cleanup/find_duplicates.py`
- Modify: `dbx-cleanup/tests/test_find_duplicates.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_find_duplicates.py`:

```python
from find_duplicates import group_by_hash, select_top_groups


def make_entry(name: str, path: str, size: int, h: str) -> FileEntry:
    return FileEntry(name=name, path=path, size=size, content_hash=h,
                     server_modified="2024-01-01T00:00:00Z")


def test_group_by_hash_drops_singletons() -> None:
    entries = [
        make_entry("a.txt", "/a.txt", 1000, "h1"),
        make_entry("b.txt", "/b.txt", 1000, "h1"),
        make_entry("solo.txt", "/solo.txt", 999, "h2"),
    ]
    groups = group_by_hash(entries)
    assert list(groups.keys()) == ["h1"]
    assert len(groups["h1"]) == 2


def test_select_top_groups_orders_by_wasted_space_desc() -> None:
    # Group X: 2 copies × 5 KB → 5 KB wasted
    # Group Y: 4 copies × 1.5 KB → 4.5 KB wasted
    # Group Z: 3 copies × 2 KB → 4 KB wasted
    groups = {
        "Z": [make_entry("z.txt", f"/z{i}", 2000, "Z") for i in range(3)],
        "X": [make_entry("x.txt", f"/x{i}", 5000, "X") for i in range(2)],
        "Y": [make_entry("y.txt", f"/y{i}", 1500, "Y") for i in range(4)],
    }
    selected = select_top_groups(groups, max_csv_rows=100)
    # Order: X (5 KB), Y (4.5 KB), Z (4 KB)
    assert [g[0].content_hash for g in selected] == ["X", "Y", "Z"]


def test_select_top_groups_never_splits_a_group() -> None:
    # Two groups: A has 3 rows, B has 2 rows. Cap is 4. We should pick whichever
    # ranks first that fits whole. A wastes (3-1)*1000=2000; B wastes (2-1)*1500=1500.
    # A is bigger waster. A has 3 rows ≤ 4 → take A. Then B has 2 rows, A+B = 5 > 4 → skip B.
    groups = {
        "A": [make_entry("a.txt", f"/a{i}", 1000, "A") for i in range(3)],
        "B": [make_entry("b.txt", f"/b{i}", 1500, "B") for i in range(2)],
    }
    selected = select_top_groups(groups, max_csv_rows=4)
    assert len(selected) == 1
    assert selected[0][0].content_hash == "A"


def test_select_top_groups_skips_oversize_first_group() -> None:
    # First-ranked group is bigger than cap; should skip and try smaller ones.
    groups = {
        "BIG": [make_entry("b.txt", f"/b{i}", 10_000, "BIG") for i in range(10)],
        "SMALL": [make_entry("s.txt", f"/s{i}", 500, "SMALL") for i in range(2)],
    }
    selected = select_top_groups(groups, max_csv_rows=5)
    # BIG has 10 rows > 5; skip. SMALL has 2 rows ≤ 5; keep.
    assert [g[0].content_hash for g in selected] == ["SMALL"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=. pytest tests/test_find_duplicates.py -v
```

Expected: 4 new tests FAIL with `ImportError`.

- [ ] **Step 3: Implement `group_by_hash` and `select_top_groups`**

Append to `dbx-cleanup/find_duplicates.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=. pytest tests/test_find_duplicates.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/find_duplicates.py dbx-cleanup/tests/test_find_duplicates.py
git commit -m "feat(find_duplicates): group by hash and select top groups by wasted space"
```

---

### Task 7: `find_duplicates.py` — CSV writer

**Files:**
- Modify: `dbx-cleanup/find_duplicates.py`
- Modify: `dbx-cleanup/tests/test_find_duplicates.py`

- [ ] **Step 1: Write failing test for `write_csv`**

Append to `tests/test_find_duplicates.py`:

```python
from find_duplicates import write_csv


def test_write_csv_columns_and_blank_rows_between_groups(tmp_path: Path) -> None:
    groups = [
        [
            make_entry("a.txt", "/A/a.txt", 1000, "h1"),
            make_entry("a.txt", "/B/a.txt", 1000, "h1"),
        ],
        [
            make_entry("b.txt", "/C/b.txt", 2000, "h2"),
            make_entry("b.txt", "/D/b.txt", 2000, "h2"),
        ],
    ]
    out_path = tmp_path / "duplicates.csv"
    write_csv(groups, out_path)

    text = out_path.read_text()
    lines = text.splitlines()
    assert lines[0] == "group_id,filename,size_bytes,path,content_hash,last_modified,delete"
    # Group 1: 2 rows, then blank, then group 2: 2 rows = 6 lines after header
    assert lines[1].startswith("1,a.txt,1000,/A/a.txt,h1,")
    assert lines[2].startswith("1,a.txt,1000,/B/a.txt,h1,")
    assert lines[3] == ""
    assert lines[4].startswith("2,b.txt,2000,/C/b.txt,h2,")
    assert lines[5].startswith("2,b.txt,2000,/D/b.txt,h2,")
```

Add `from pathlib import Path` to test imports if not already there.

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH=. pytest tests/test_find_duplicates.py::test_write_csv_columns_and_blank_rows_between_groups -v
```

Expected: FAIL with `ImportError: cannot import name 'write_csv'`.

- [ ] **Step 3: Implement `write_csv`**

Append to `dbx-cleanup/find_duplicates.py`:

```python
CSV_HEADER = ["group_id", "filename", "size_bytes", "path", "content_hash",
              "last_modified", "delete"]


def write_csv(groups: list[list[FileEntry]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for idx, group in enumerate(groups, start=1):
            for entry in sorted(group, key=lambda e: e.path):
                writer.writerow([
                    idx, entry.name, entry.size, entry.path,
                    entry.content_hash, entry.server_modified, "",
                ])
            # blank row between groups (but not after the last one)
            if idx < len(groups):
                writer.writerow([])
```

- [ ] **Step 4: Run test to verify it passes**

```bash
PYTHONPATH=. pytest tests/test_find_duplicates.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/find_duplicates.py dbx-cleanup/tests/test_find_duplicates.py
git commit -m "feat(find_duplicates): write candidate-duplicates CSV"
```

---

### Task 8: `find_duplicates.py` — main + scan orchestration

**Files:**
- Modify: `dbx-cleanup/find_duplicates.py`

- [ ] **Step 1: Implement scan loop and `main`**

Append to `dbx-cleanup/find_duplicates.py`:

```python
import argparse
from datetime import datetime

import dropbox
from dropbox.files import FileMetadata, ListFolderResult

from dbx_client import Config, get_client, load_config, load_token, with_retry


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
    cursor: str | None = None
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
                dup_rows = sum(len(g) for g in group_by_hash(kept).values())
                print(f"Scanned {files_scanned} files, "
                      f"{len(group_by_hash(kept))} duplicate groups "
                      f"({dup_rows} rows)...")
                if dup_rows >= config.early_exit_row_threshold:
                    print(f"Hit early-exit threshold ({config.early_exit_row_threshold}); "
                          "stopping scan.")
                    return kept

        if not result.has_more:
            break
        cursor = result.cursor
        result = with_retry(lambda: client.files_list_folder_continue(cursor))

    print(f"Scan complete. Total files scanned: {files_scanned}.")
    return kept


def main() -> int:
    parser = argparse.ArgumentParser(description="Find Dropbox duplicates.")
    parser.add_argument("--config", default="config.ini",
                        help="Path to config.ini (default: config.ini)")
    parser.add_argument("--root", default="/",
                        help="Dropbox path to scan (default: /)")
    args = parser.parse_args()

    config = load_config(Path(args.config))
    token = load_token()
    client = get_client(token)
    owner = client.users_get_current_account().account_id

    entries = scan_dropbox(client, args.root, config, owner)
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
    print("Mark 'x' in the delete column for files to remove, then run:")
    print(f"  python delete_duplicates.py --csv {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Smoke check imports**

```bash
PYTHONPATH=. python -c "import find_duplicates; print('ok')"
```

Expected: `ok`.

- [ ] **Step 3: Run full test suite to confirm nothing regressed**

```bash
PYTHONPATH=. pytest -v
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add dbx-cleanup/find_duplicates.py
git commit -m "feat(find_duplicates): wire up Dropbox scan and main entrypoint"
```

---

### Task 9: `delete_duplicates.py` — CSV parser

**Files:**
- Create: `dbx-cleanup/delete_duplicates.py`
- Create: `dbx-cleanup/tests/test_delete_duplicates.py`

- [ ] **Step 1: Write failing test**

Create `tests/test_delete_duplicates.py`:

```python
from pathlib import Path

import pytest

from delete_duplicates import CsvRow, parse_csv


def write_csv(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "dup.csv"
    p.write_text(
        "group_id,filename,size_bytes,path,content_hash,last_modified,delete\n" + body
    )
    return p


def test_parse_csv_basic(tmp_path: Path) -> None:
    p = write_csv(tmp_path, (
        "1,a.txt,1000,/A/a.txt,h1,2024-01-01T00:00:00,\n"
        "1,a.txt,1000,/B/a.txt,h1,2024-01-01T00:00:00,x\n"
        "\n"
        "2,b.txt,2000,/C/b.txt,h2,2024-01-01T00:00:00,\n"
        "2,b.txt,2000,/D/b.txt,h2,2024-01-01T00:00:00,\n"
    ))
    rows = parse_csv(p)
    assert len(rows) == 4
    assert rows[0].group_id == 1
    assert rows[0].marked_delete is False
    assert rows[1].marked_delete is True
    assert rows[1].path == "/B/a.txt"


def test_parse_csv_ignores_blank_separator_rows(tmp_path: Path) -> None:
    p = write_csv(tmp_path, (
        "1,a.txt,1000,/A/a.txt,h1,2024-01-01T00:00:00,\n"
        "\n"
        "1,a.txt,1000,/B/a.txt,h1,2024-01-01T00:00:00,\n"
    ))
    rows = parse_csv(p)
    assert len(rows) == 2


def test_parse_csv_normalizes_x_case_and_whitespace(tmp_path: Path) -> None:
    p = write_csv(tmp_path, (
        "1,a.txt,1000,/A/a.txt,h1,2024-01-01T00:00:00, X \n"
        "1,a.txt,1000,/B/a.txt,h1,2024-01-01T00:00:00,x\n"
    ))
    rows = parse_csv(p)
    assert all(r.marked_delete for r in rows)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=. pytest tests/test_delete_duplicates.py -v
```

Expected: 3 tests FAIL with `ImportError`.

- [ ] **Step 3: Implement `parse_csv` and `CsvRow`**

Create `dbx-cleanup/delete_duplicates.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=. pytest tests/test_delete_duplicates.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/delete_duplicates.py dbx-cleanup/tests/test_delete_duplicates.py
git commit -m "feat(delete_duplicates): parse user-edited CSV"
```

---

### Task 10: `delete_duplicates.py` — local validators (B and C)

**Files:**
- Modify: `dbx-cleanup/delete_duplicates.py`
- Modify: `dbx-cleanup/tests/test_delete_duplicates.py`

- [ ] **Step 1: Write failing tests for the two local validators**

Append to `tests/test_delete_duplicates.py`:

```python
from delete_duplicates import (
    ValidationProblem,
    validate_groups_have_survivor,
    validate_max_rows,
)


def make_row(group_id: int, path: str, marked: bool, h: str = "h") -> CsvRow:
    return CsvRow(
        group_id=group_id, filename=path.rsplit("/", 1)[-1], size_bytes=1000,
        path=path, content_hash=h, last_modified="2024-01-01T00:00:00",
        marked_delete=marked,
    )


def test_validate_groups_have_survivor_passes_when_one_unmarked() -> None:
    rows = [make_row(1, "/a", False), make_row(1, "/b", True)]
    assert validate_groups_have_survivor(rows) == []


def test_validate_groups_have_survivor_flags_fully_marked_group() -> None:
    rows = [make_row(1, "/a", True), make_row(1, "/b", True),
            make_row(2, "/c", False), make_row(2, "/d", True)]
    problems = validate_groups_have_survivor(rows)
    assert len(problems) == 1
    assert "Group 1" in problems[0].message
    # Both rows from group 1 should be in offending_paths
    assert set(problems[0].offending_paths) == {"/a", "/b"}


def test_validate_max_rows_passes_under_cap() -> None:
    rows = [make_row(1, f"/p{i}", True) for i in range(5)]
    assert validate_max_rows(rows, max_csv_rows=100) == []


def test_validate_max_rows_flags_overage() -> None:
    rows = [make_row(1, f"/p{i}", True) for i in range(101)]
    problems = validate_max_rows(rows, max_csv_rows=100)
    assert len(problems) == 1
    assert "101" in problems[0].message
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=. pytest tests/test_delete_duplicates.py -v
```

Expected: 4 new tests FAIL with `ImportError`.

- [ ] **Step 3: Implement validators**

Append to `dbx-cleanup/delete_duplicates.py`:

```python
@dataclass(frozen=True)
class ValidationProblem:
    code: str       # e.g. "GROUP_FULLY_MARKED"
    message: str    # human-readable
    offending_paths: tuple[str, ...]


def validate_groups_have_survivor(rows: list[CsvRow]) -> list[ValidationProblem]:
    """Validation B: each group must keep at least one row not marked for delete."""
    problems: list[ValidationProblem] = []
    by_group: dict[int, list[CsvRow]] = {}
    for r in rows:
        by_group.setdefault(r.group_id, []).append(r)
    for gid, group_rows in by_group.items():
        if all(r.marked_delete for r in group_rows):
            problems.append(ValidationProblem(
                code="GROUP_FULLY_MARKED",
                message=(f"Group {gid} has every row marked 'x'. Refusing to delete "
                         f"all copies of a file."),
                offending_paths=tuple(r.path for r in group_rows),
            ))
    return problems


def validate_max_rows(rows: list[CsvRow], max_csv_rows: int) -> list[ValidationProblem]:
    """Validation C: total marked-delete count must not exceed max_csv_rows."""
    marked = [r for r in rows if r.marked_delete]
    if len(marked) > max_csv_rows:
        return [ValidationProblem(
            code="EXCEEDS_MAX_ROWS",
            message=(f"{len(marked)} rows are marked for deletion; daily cap is "
                     f"{max_csv_rows}. Reduce the marked rows and re-run."),
            offending_paths=tuple(r.path for r in marked),
        )]
    return []
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=. pytest tests/test_delete_duplicates.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/delete_duplicates.py dbx-cleanup/tests/test_delete_duplicates.py
git commit -m "feat(delete_duplicates): add local validators B and C"
```

---

### Task 11: `delete_duplicates.py` — Dropbox-side validators (A and D)

**Files:**
- Modify: `dbx-cleanup/delete_duplicates.py`
- Modify: `dbx-cleanup/tests/test_delete_duplicates.py`

- [ ] **Step 1: Write failing tests using mocked Dropbox client**

Append to `tests/test_delete_duplicates.py`:

```python
from unittest.mock import MagicMock

from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata

from delete_duplicates import validate_paths_and_hashes


def fake_metadata(path: str, content_hash: str) -> FileMetadata:
    m = MagicMock(spec=FileMetadata)
    m.path_display = path
    m.content_hash = content_hash
    return m


def test_validate_paths_and_hashes_all_good() -> None:
    rows = [make_row(1, "/a", True, h="h1"), make_row(1, "/b", False, h="h1")]
    client = MagicMock()
    client.files_get_metadata.side_effect = lambda p: fake_metadata(p, "h1")
    problems = validate_paths_and_hashes(client, rows)
    assert problems == []
    # only checks marked rows
    client.files_get_metadata.assert_called_once_with("/a")


def test_validate_paths_and_hashes_missing_path() -> None:
    rows = [make_row(1, "/gone", True, h="h1"), make_row(1, "/b", False, h="h1")]
    client = MagicMock()
    err = ApiError("req-id", MagicMock(), "user-msg", "")
    client.files_get_metadata.side_effect = err
    problems = validate_paths_and_hashes(client, rows)
    assert len(problems) == 1
    assert problems[0].code == "PATH_NOT_FOUND"
    assert "/gone" in problems[0].offending_paths


def test_validate_paths_and_hashes_changed_hash() -> None:
    rows = [make_row(1, "/a", True, h="h1"), make_row(1, "/b", False, h="h1")]
    client = MagicMock()
    client.files_get_metadata.side_effect = lambda p: fake_metadata(p, "h_NEW")
    problems = validate_paths_and_hashes(client, rows)
    assert len(problems) == 1
    assert problems[0].code == "HASH_CHANGED"
    assert "/a" in problems[0].offending_paths
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=. pytest tests/test_delete_duplicates.py -v
```

Expected: 3 new tests FAIL with `ImportError`.

- [ ] **Step 3: Implement `validate_paths_and_hashes`**

Append to `dbx-cleanup/delete_duplicates.py`:

```python
def validate_paths_and_hashes(
    client: dropbox.Dropbox,
    rows: list[CsvRow],
) -> list[ValidationProblem]:
    """Validations A + D: for each row marked for delete, the path must still
    exist in Dropbox AND its content_hash must match the CSV (no edits since scan).
    Combined into one Dropbox call per row."""
    missing: list[str] = []
    changed: list[str] = []
    for row in rows:
        if not row.marked_delete:
            continue
        try:
            meta = with_retry(lambda: client.files_get_metadata(row.path))
        except ApiError:
            missing.append(row.path)
            continue
        if getattr(meta, "content_hash", None) != row.content_hash:
            changed.append(row.path)

    problems: list[ValidationProblem] = []
    if missing:
        problems.append(ValidationProblem(
            code="PATH_NOT_FOUND",
            message=(f"{len(missing)} path(s) marked for delete no longer exist "
                     "in Dropbox. Re-run find_duplicates.py to refresh the CSV."),
            offending_paths=tuple(missing),
        ))
    if changed:
        problems.append(ValidationProblem(
            code="HASH_CHANGED",
            message=(f"{len(changed)} file(s) have changed since the scan "
                     "(content_hash differs). Re-run find_duplicates.py to refresh "
                     "the CSV."),
            offending_paths=tuple(changed),
        ))
    return problems
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=. pytest tests/test_delete_duplicates.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/delete_duplicates.py dbx-cleanup/tests/test_delete_duplicates.py
git commit -m "feat(delete_duplicates): add path-existence and hash validators"
```

---

### Task 12: `delete_duplicates.py` — error log writer + execution + audit log + main

**Files:**
- Modify: `dbx-cleanup/delete_duplicates.py`
- Modify: `dbx-cleanup/tests/test_delete_duplicates.py`

- [ ] **Step 1: Write failing tests for the error log and execution**

Append to `tests/test_delete_duplicates.py`:

```python
from delete_duplicates import execute_deletes, write_error_log


def test_write_error_log_lists_all_problems(tmp_path: Path) -> None:
    problems = [
        ValidationProblem("GROUP_FULLY_MARKED", "Group 1 fully marked", ("/a", "/b")),
        ValidationProblem("PATH_NOT_FOUND", "1 missing", ("/gone",)),
    ]
    log_path = tmp_path / "error.log"
    write_error_log(problems, log_path)
    text = log_path.read_text()
    assert "GROUP_FULLY_MARKED" in text
    assert "PATH_NOT_FOUND" in text
    assert "/a" in text
    assert "/b" in text
    assert "/gone" in text


def test_execute_deletes_continues_on_error(tmp_path: Path) -> None:
    rows_to_delete = [make_row(1, "/a", True), make_row(1, "/b", True),
                      make_row(2, "/c", True)]
    client = MagicMock()
    # /b raises an ApiError; /a and /c succeed
    def fake_delete(path: str):
        if path == "/b":
            raise ApiError("req-id", MagicMock(), "boom", "")
        return MagicMock(metadata=MagicMock(path_display=path))
    client.files_delete_v2.side_effect = fake_delete

    log_path = tmp_path / "delete-log.csv"
    summary = execute_deletes(client, rows_to_delete, log_path)
    assert summary.success_count == 2
    assert summary.error_count == 1

    log_text = log_path.read_text()
    # CSV header + 3 rows
    assert log_text.count("\n") >= 4
    assert "/a" in log_text and "/b" in log_text and "/c" in log_text
    assert "deleted" in log_text and "error" in log_text
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=. pytest tests/test_delete_duplicates.py -v
```

Expected: 2 new tests FAIL with `ImportError`.

- [ ] **Step 3: Implement error log, executor, and main**

Append to `dbx-cleanup/delete_duplicates.py`:

```python
@dataclass(frozen=True)
class ExecutionSummary:
    success_count: int
    error_count: int
    log_path: Path


AUDIT_HEADER = ["timestamp", "path", "size_bytes", "content_hash", "status",
                "dropbox_response"]


def write_error_log(problems: list[ValidationProblem], log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w") as f:
        f.write(f"Pre-flight validation failed at {datetime.now().isoformat()}\n")
        f.write("No deletions were performed.\n\n")
        for p in problems:
            f.write(f"[{p.code}] {p.message}\n")
            for path in p.offending_paths:
                f.write(f"  - {path}\n")
            f.write("\n")


def execute_deletes(
    client: dropbox.Dropbox,
    rows: list[CsvRow],
    log_path: Path,
) -> ExecutionSummary:
    """Move each row's path to the Dropbox recycle bin via files_delete_v2.
    Continues on per-file errors. Writes one audit-log row per attempt."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    success = 0
    errors = 0
    with log_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(AUDIT_HEADER)
        for row in rows:
            ts = datetime.now().isoformat()
            try:
                resp = with_retry(lambda: client.files_delete_v2(row.path))
                # files_delete_v2 returns a DeleteResult with .metadata of the deleted entry,
                # confirming Dropbox accepted and moved the file to deleted-files (recycle bin).
                deleted_path = getattr(resp.metadata, "path_display", row.path)
                writer.writerow([ts, row.path, row.size_bytes, row.content_hash,
                                 "deleted", f"moved to recycle bin: {deleted_path}"])
                success += 1
                print(f"  deleted: {row.path}")
            except DropboxException as exc:
                # ApiError, RateLimitError (after retries exhausted), AuthError, etc.
                writer.writerow([ts, row.path, row.size_bytes, row.content_hash,
                                 "error", str(exc)])
                errors += 1
                print(f"  ERROR  : {row.path} ({exc})")
    return ExecutionSummary(success_count=success, error_count=errors, log_path=log_path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Delete user-flagged Dropbox duplicates.")
    parser.add_argument("--config", default="config.ini")
    parser.add_argument("--csv", required=True, help="Path to user-edited CSV.")
    args = parser.parse_args()

    config = load_config(Path(args.config))
    token = load_token()
    client = get_client(token)

    rows = parse_csv(Path(args.csv))
    marked_count = sum(1 for r in rows if r.marked_delete)
    if marked_count == 0:
        print("No rows marked with 'x' in the delete column. Nothing to do.")
        return 0

    print(f"Pre-flight validation on {len(rows)} rows ({marked_count} marked for delete)...")
    problems: list[ValidationProblem] = []
    problems.extend(validate_groups_have_survivor(rows))
    problems.extend(validate_max_rows(rows, config.max_csv_rows))
    problems.extend(validate_paths_and_hashes(client, rows))

    if problems:
        ts = datetime.now().strftime("%Y-%m-%d-%H%M")
        log_path = config.log_dir / f"error-{ts}.log"
        write_error_log(problems, log_path)
        print(f"\nValidation failed with {len(problems)} problem(s). "
              f"See {log_path}")
        for p in problems:
            print(f"  [{p.code}] {p.message}")
        return 2

    print("All validations passed.")
    confirmation = input(
        f"\nAbout to move {marked_count} file(s) to Dropbox recycle bin. "
        "Type 'yes' to proceed: "
    )
    if confirmation.strip() != "yes":
        print("Aborted by user.")
        return 1

    rows_to_delete = [r for r in rows if r.marked_delete]
    ts = datetime.now().strftime("%Y-%m-%d-%H%M")
    audit_path = config.log_dir / f"delete-log-{ts}.csv"
    summary = execute_deletes(client, rows_to_delete, audit_path)

    print(f"\nDone. Deleted: {summary.success_count}, Errors: {summary.error_count}")
    print(f"Audit log: {summary.log_path}")
    return 0 if summary.error_count == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=. pytest -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/delete_duplicates.py dbx-cleanup/tests/test_delete_duplicates.py
git commit -m "feat(delete_duplicates): error log, executor, audit log, main entrypoint"
```

---

### Task 13: `seed_test_data.py`

**Files:**
- Create: `dbx-cleanup/seed_test_data.py`

- [ ] **Step 1: Implement the seed script**

Create `dbx-cleanup/seed_test_data.py`:

```python
"""One-shot uploader: populate /test-duplicates/ with known fixtures
so the cleanup scripts can be exercised end-to-end against real Dropbox.

Idempotent: clears /test-duplicates/ before populating."""

from __future__ import annotations

import sys

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import WriteMode

from dbx_client import get_client, load_token, with_retry

ROOT = "/test-duplicates"


def _bytes_of_size(size: int, marker: bytes) -> bytes:
    """Build deterministic bytes of the given size. `marker` distinguishes groups."""
    block = (marker * (size // len(marker) + 1))[:size]
    return block


def reset_root(client: dropbox.Dropbox) -> None:
    try:
        with_retry(lambda: client.files_delete_v2(ROOT))
        print(f"Cleared existing {ROOT}")
    except ApiError as exc:
        # path/not_found is fine
        if "not_found" not in str(exc):
            raise
    with_retry(lambda: client.files_create_folder_v2(ROOT))


def upload(client: dropbox.Dropbox, path: str, content: bytes) -> None:
    with_retry(lambda: client.files_upload(content, path, mode=WriteMode("overwrite")))
    print(f"  uploaded {path} ({len(content)} bytes)")


def main() -> int:
    token = load_token()
    client = get_client(token)
    reset_root(client)

    # Group A: 3 copies of a 2 KB file
    a = _bytes_of_size(2 * 1024, b"GROUP_A_")
    for sub in ("A", "B", "C"):
        upload(client, f"{ROOT}/{sub}/file.txt", a)

    # Group B: 2 copies of a 5 KB file
    b = _bytes_of_size(5 * 1024, b"GROUP_B_")
    for sub in ("B", "D"):
        upload(client, f"{ROOT}/{sub}/photo.jpg", b)

    # Group C: 4 copies of a 1.5 KB file
    c = _bytes_of_size(int(1.5 * 1024), b"GROUP_C_")
    for sub in ("E", "F", "G", "H"):
        upload(client, f"{ROOT}/{sub}/doc.pdf", c)

    # Noise: unique non-duplicate files
    upload(client, f"{ROOT}/noise/unique1.dat", _bytes_of_size(3 * 1024, b"UNIQUE_1_"))
    upload(client, f"{ROOT}/noise/unique2.dat", _bytes_of_size(4 * 1024, b"UNIQUE_2_"))

    # Below-threshold: should be skipped by find_duplicates.py with config.test.ini
    tiny = _bytes_of_size(500, b"TINY_")
    upload(client, f"{ROOT}/tiny/a.txt", tiny)
    upload(client, f"{ROOT}/tiny/b.txt", tiny)

    print("\nSeed complete. Now run:")
    print(f"  python find_duplicates.py --config config.test.ini --root {ROOT}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Smoke check imports**

```bash
PYTHONPATH=. python -c "import seed_test_data; print('ok')"
```

Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add dbx-cleanup/seed_test_data.py
git commit -m "feat(dbx-cleanup): add test-data seeding script"
```

---

### Task 14: `README.md`

**Files:**
- Create: `dbx-cleanup/README.md`

- [ ] **Step 1: Write the README**

Create `dbx-cleanup/README.md`:

````markdown
# dbx-cleanup

Phase 1 scripts for finding and deleting duplicate files in Dropbox.

Two scripts:

- `find_duplicates.py` — scans Dropbox, identifies byte-identical files in different locations, writes a CSV ranked by wasted space (≤100 rows).
- `delete_duplicates.py` — ingests the user-edited CSV and moves flagged files to Dropbox's recycle bin, with strict pre-flight validations.

## One-time setup

### 1. Install Python and create a virtualenv

```bash
cd dbx-cleanup
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Create a Dropbox app and generate an access token

1. Go to https://www.dropbox.com/developers/apps and click **Create app**.
2. Choose **Scoped access** → **Full Dropbox** (so the script can scan everything you own).
3. Name it (e.g. `dbx-cleanup-personal`). Click **Create app**.
4. On the **Permissions** tab, enable:
   - `files.metadata.read`
   - `files.content.read`
   - `files.content.write`

   Click **Submit**.
5. On the **Settings** tab, scroll to **Generated access token** and click **Generate**. This produces a long-lived token tied to your account.
6. Copy `.env.example` to `.env` and paste the token:

   ```bash
   cp .env.example .env
   # edit .env, paste the token after DROPBOX_ACCESS_TOKEN=
   ```

### 3. Review settings

Open `config.ini`. Most users only adjust `min_file_size_bytes` (default 100 KB — files smaller than this are ignored to reduce noise).

## Test before unleashing

Run a full end-to-end test against a sandboxed Dropbox folder before pointing the scripts at your real files.

```bash
# 1. Seed /test-duplicates/ in your Dropbox with known fixtures
python seed_test_data.py

# 2. Find duplicates in the test folder using the test config (1 KB threshold)
python find_duplicates.py --config config.test.ini --root /test-duplicates/

# 3. Open the CSV in output/ in your spreadsheet of choice.
#    Expected: 3 groups, 9 total rows, ordered Group B (5KB wasted), Group C (4.5KB), Group A (4KB).
#    Mark 'x' in the delete column for one row in Group B, save the file.

# 4. Run the delete script
python delete_duplicates.py --config config.test.ini --csv output/duplicates-<timestamp>.csv

# 5. In the Dropbox web UI, open "Deleted files" → confirm the marked file is there.

# 6. Verify the safety check: open the CSV again, mark ALL rows of Group A with 'x'.
#    Re-run delete_duplicates.py — it should abort with a GROUP_FULLY_MARKED error
#    and write logs/error-<timestamp>.log naming Group A's rows.
```

## Real usage

```bash
# 1. Find candidate duplicates across your whole Dropbox
python find_duplicates.py

# 2. Open the CSV under output/ in your spreadsheet (Excel, Numbers, Google Sheets…).
#    Mark 'x' in the `delete` column for files you want removed.
#    Save the CSV.

# 3. Move marked files to Dropbox's recycle bin
python delete_duplicates.py --csv output/duplicates-<timestamp>.csv

# 4. Repeat. Each run handles up to 100 candidates.
```

## Output files

- `output/duplicates-YYYY-MM-DD-HHMM.csv` — candidate duplicates, columns: `group_id, filename, size_bytes, path, content_hash, last_modified, delete`. Rows are grouped, with a blank row between groups.
- `logs/delete-log-YYYY-MM-DD-HHMM.csv` — audit log of every delete attempt: timestamp, path, status (`deleted` or `error`), and the Dropbox response confirming the file is in the recycle bin.
- `logs/error-YYYY-MM-DD-HHMM.log` — written when pre-flight validation fails. Lists every offending row and the reason. No deletions occur.

## Recovering deleted files

`delete_duplicates.py` calls `files_delete_v2`, which moves files to Dropbox's "Deleted files" area (recycle bin). Restore via:

1. Web UI → **Deleted files** in the left sidebar
2. Find the file → **Restore**

Retention: 30 days on free / Plus, 180 days on Professional / Business.

## Tests

Unit tests run without hitting Dropbox:

```bash
source .venv/bin/activate
PYTHONPATH=. pytest -v
```

The full integration test against a real Dropbox account is the **Test before unleashing** section above.
````

- [ ] **Step 2: Commit**

```bash
git add dbx-cleanup/README.md
git commit -m "docs(dbx-cleanup): write README with setup, test, and usage instructions"
```

---

### Task 15: Manual end-to-end smoke test against real Dropbox

This task is performed by the user, not a subagent. It is the final check before the scripts are considered production-ready.

- [ ] **Step 1: Generate a Dropbox access token (per README)**

Set `DROPBOX_ACCESS_TOKEN` in `dbx-cleanup/.env`.

- [ ] **Step 2: Seed the test folder**

```bash
cd dbx-cleanup
source .venv/bin/activate
python seed_test_data.py
```

Expected: prints uploads for each file under `/test-duplicates/` and the suggested next command.

- [ ] **Step 3: Run find_duplicates with the test config**

```bash
python find_duplicates.py --config config.test.ini --root /test-duplicates/
```

Expected:
- "Connected to Dropbox as `<your email>`"
- "Wrote 3 groups, 9 rows to output/duplicates-…csv"
- The CSV exists and has groups in order: Group B (5 KB wasted), Group C (4.5 KB), Group A (4 KB).

- [ ] **Step 4: Inspect the CSV**

Open the CSV. Confirm:
- Header row matches `group_id,filename,size_bytes,path,content_hash,last_modified,delete`.
- 9 data rows, group IDs 1–3, blank rows between groups.
- The two files under `/test-duplicates/tiny/` (500 bytes each) are absent.

- [ ] **Step 5: Mark one row and run delete**

Mark `x` in one of Group B's rows (e.g. `/test-duplicates/D/photo.jpg`). Save.

```bash
python delete_duplicates.py --config config.test.ini --csv output/duplicates-<timestamp>.csv
```

Expected: validation passes, prompt asks for `yes`, type `yes`, file is deleted, audit log is written.

- [ ] **Step 6: Confirm file is in Dropbox recycle bin**

Open Dropbox web → **Deleted files**. Confirm `/test-duplicates/D/photo.jpg` is listed.

- [ ] **Step 7: Verify the safety check**

Re-run `find_duplicates.py --config config.test.ini --root /test-duplicates/` to refresh the CSV. Mark `x` on every row of one group (Group A or C). Save. Run `delete_duplicates.py` against the new CSV.

Expected:
- Validation reports `GROUP_FULLY_MARKED`.
- Exit code is non-zero.
- `logs/error-<timestamp>.log` exists and lists the offending paths.
- **No file was deleted.** Verify by checking Dropbox.

- [ ] **Step 8: Clean up the test folder (optional)**

In Dropbox web UI, delete `/test-duplicates/` once happy.

- [ ] **Step 9: Final commit if any fixes were made**

If the smoke test surfaced a bug, fix it, add a regression test, and commit:

```bash
git add <fixed files>
git commit -m "fix(dbx-cleanup): <describe>"
```

If everything passed cleanly, no commit needed. The Phase 1 deliverable is complete.

---

## Self-Review (post-write)

**Spec coverage:**
- Folder layout — Task 1
- Auth (long-lived token, README walkthrough) — Tasks 1, 3, 14
- Shared `dbx_client.py` (config, token, client, retry) — Tasks 2, 3, 4
- `find_duplicates.py` flow (skip, group, rank, write, scan orchestration) — Tasks 5, 6, 7, 8
- `delete_duplicates.py` flow (parse, validators A–D, error log, executor with continue-on-error, audit log, main) — Tasks 9, 10, 11, 12
- Confirmation prompt — Task 12
- Audit log format — Task 12
- `seed_test_data.py` with the spec's exact group sizes — Task 13
- README with setup, test sequence, real usage, recovery — Task 14
- Manual smoke test verifying the spec's expected ordering and the safety check — Task 15
