# Photo & Video Tagging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build five Python scripts (`count_media.py`, `get_images.py`/`get_videos.py`, `update_images.py`/`update_videos.py`) that count, batch, review, and tag photos/videos in Dropbox using native Dropbox tags, with a self-contained HTML review page and a portable JSON archive.

**Architecture:** Mirrored image/video pipelines share an engine (`get_media.py`, `update_media.py`); thin wrappers expose `get_images.py`/`get_videos.py` and `update_images.py`/`update_videos.py`. A shared `dbx_media.py` holds the pure helpers (classification, tag normalization, folder clustering, archive merge) plus the Dropbox-touching helpers (tag fetch/apply, thumbnail download). Pre-flight validation mirrors the existing `delete_duplicates.py` pattern: validate everything, abort on any failure, no half-done state.

**Tech Stack:** Python 3.10+, `dropbox` SDK 12+, `python-dotenv`, `pytest` for tests. Vanilla HTML/CSS/JS in the generated review page (no framework).

**Spec:** `docs/superpowers/specs/2026-05-10-photo-video-tagging-design.md`

---

## File Structure

```
dbx-cleanup/
├── config.ini                  # ADD [media] section
├── config.local.ini            # ADD [media] section (user-tuned)
├── config.test.ini             # ADD [media] section (batch_size=10)
├── dbx_client.py               # ADD MediaConfig, load_media_config
├── dbx_media.py                # NEW: pure helpers + Dropbox helpers + archive
├── count_media.py              # NEW: walks Dropbox, prints photo/video counts
├── get_media.py                # NEW: shared engine for get_images/get_videos
├── get_images.py               # NEW: thin wrapper (kind="image")
├── get_videos.py               # NEW: thin wrapper (kind="video")
├── update_media.py             # NEW: shared engine for update_images/update_videos
├── update_images.py            # NEW: thin wrapper (kind="image")
├── update_videos.py            # NEW: thin wrapper (kind="video")
├── seed_test_media.py          # NEW: uploads test fixtures to /test-media/
└── tests/
    ├── test_dbx_client.py      # EXTEND: cover load_media_config
    ├── test_dbx_media.py       # NEW
    ├── test_get_media.py       # NEW
    └── test_update_media.py    # NEW
```

Module responsibilities:

- `dbx_client.py` — config + auth + retry. Adds `MediaConfig` + `load_media_config`.
- `dbx_media.py` — three layers in one file (kept together because they're all small and tightly related):
  - **Pure helpers** (no Dropbox calls): `classify_media`, `normalize_tag`, `fold_to_folders`
  - **Dropbox helpers** (use `client`): `fetch_existing_tags`, `fetch_thumbnail`, `apply_tags`
  - **Archive I/O**: `load_archive`, `save_archive`, `merge_tagged`, `merge_deleted`
- `get_media.py` — engine: `run(kind)` does walk → tag-lookup → filter-untagged → fold → pack → fetch-thumbs → build-HTML → write-file.
- `update_media.py` — engine: `run(kind)` does parse-CSV → validate-all → confirm → execute → archive-merge → audit-log.
- Wrappers (`get_images.py`, etc.) are 5-line files that import the engine and call it with the right `kind`.

---

## Pre-Implementation: Verify SDK signatures

Before writing tests, the engineer should confirm the exact Dropbox SDK method names and response shapes for tags + thumbnails. These are the assumed signatures used throughout this plan:

```python
# Add one tag at a time:
client.files_tags_add(path: str, tag_text: str) -> None
# Raises ApiError if tag already exists or invalid.

# Get tags for up to 100 paths in one call:
result = client.files_tags_get_batch(paths: list[str])
# result.paths_to_tags: list[PathToTags], each .path and .tags (list[Tag], each .tag_text)

# Get thumbnail bytes:
metadata, response = client.files_get_thumbnail_v2(
    resource=PathOrLink.path(path),
    format=ThumbnailFormat.jpeg,
    size=ThumbnailSize.w480h320,   # one of: w32h32, w64h64, w128h128, w256h256,
                                    # w480h320, w640h480, w960h640, w1024h768, w2048h1536
    mode=ThumbnailMode.strict,
)
thumbnail_bytes = response.content
```

If the installed SDK differs, adjust call sites in the implementation; tests use mocks so they don't break.

---

### Task 1: Add `[media]` section to config files

**Files:**
- Modify: `dbx-cleanup/config.ini` (tracked)
- Modify: `dbx-cleanup/config.test.ini` (tracked)
- **Do NOT touch** `dbx-cleanup/config.local.ini` — it's gitignored and contains the user's personal tuning. The user will copy the new `[media]` section from `config.ini` into their `config.local.ini` themselves (documented in the README update in Task 15).

- [ ] **Step 1: Add `[media]` to `dbx-cleanup/config.ini`**

Append to the file:

```ini

[media]
# File extensions, comma-separated, lowercase, no dots.
photo_extensions = jpg,jpeg,png,heic,heif,tiff,tif,gif,webp,bmp,raw,cr2,nef,arw,dng,orf,rw2
video_extensions = mp4,mov,m4v,avi,mkv,wmv,flv,webm,mpg,mpeg,3gp,m2ts

# How many media files to include in a single tag-review batch.
# Keep modest: 50 = ~1.5 MB HTML at default thumb width. 200+ produces 20+ MB pages.
batch_size = 50

# Thumbnail width in pixels. Dropbox supports exactly these values:
#   32, 64, 128, 256, 480, 640, 960, 1024, 2048
# Anything else is rejected at config-load time.
thumbnail_width = 480

# Where the persistent JSON archive lives. Keyed by Dropbox path.
# Gitignored. Survives a Dropbox departure — keep it forever.
tag_archive_path = ./output/tag-archive.json

# Folders to skip during media scans. Independent of [scan].ignored_folders
# (which is for find_duplicates). Same prefix-match semantics: case-insensitive,
# affects the listed subtree only, not siblings or parents.
ignored_folders =
    /Old Backups
    /screenshots
```

- [ ] **Step 2: Add a smaller `[media]` block to `dbx-cleanup/config.test.ini`**

```ini

[media]
photo_extensions = jpg,jpeg,png
video_extensions = mp4,mov
batch_size = 10
thumbnail_width = 256
tag_archive_path = ./output/tag-archive-test.json
ignored_folders =
```

(Smaller batch + smaller thumb width = faster tests. PNG is included because the seed script uploads 1×1 PNG fixtures.)

- [ ] **Step 3: Commit**

```bash
git add dbx-cleanup/config.ini dbx-cleanup/config.test.ini
git commit -m "config: add [media] section for photo/video tagging"
```

---

### Task 2: `MediaConfig` dataclass + `load_media_config` in `dbx_client.py`

**Files:**
- Modify: `dbx-cleanup/dbx_client.py`
- Modify: `dbx-cleanup/tests/test_dbx_client.py`

- [ ] **Step 1: Write the failing tests**

Append to `dbx-cleanup/tests/test_dbx_client.py`:

```python
from dbx_client import MediaConfig, load_media_config


def test_load_media_config_happy_path(tmp_path: Path) -> None:
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
        "\n"
        "[media]\n"
        "photo_extensions = jpg,jpeg,png\n"
        "video_extensions = mp4,mov\n"
        "batch_size = 50\n"
        "thumbnail_width = 480\n"
        "tag_archive_path = ./output/tag-archive.json\n"
        "ignored_folders =\n"
        "    /Old Backups\n"
        "    /Screenshots/\n"
    )

    mc = load_media_config(cfg_path)

    assert mc.photo_extensions == frozenset({"jpg", "jpeg", "png"})
    assert mc.video_extensions == frozenset({"mp4", "mov"})
    assert mc.batch_size == 50
    assert mc.thumbnail_width == 480
    assert mc.tag_archive_path == Path("./output/tag-archive.json")
    assert mc.csv_output_dir == Path("./output")
    assert mc.log_dir == Path("./logs")
    assert mc.ignored_folders == ("/old backups", "/screenshots")


def test_load_media_config_rejects_bad_thumbnail_width(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\nmin_file_size_bytes=1\nskip_shared_not_owned=true\nskip_hidden=true\n"
        "early_exit_row_threshold=1\nmax_csv_rows=1\n\n"
        "[paths]\ncsv_output_dir=./o\nlog_dir=./l\n\n"
        "[media]\nphoto_extensions=jpg\nvideo_extensions=mp4\nbatch_size=10\n"
        "thumbnail_width=333\ntag_archive_path=./a.json\nignored_folders=\n"
    )
    with pytest.raises(ValueError, match="thumbnail_width must be one of"):
        load_media_config(cfg_path)


def test_load_media_config_rejects_zero_batch_size(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\nmin_file_size_bytes=1\nskip_shared_not_owned=true\nskip_hidden=true\n"
        "early_exit_row_threshold=1\nmax_csv_rows=1\n\n"
        "[paths]\ncsv_output_dir=./o\nlog_dir=./l\n\n"
        "[media]\nphoto_extensions=jpg\nvideo_extensions=mp4\nbatch_size=0\n"
        "thumbnail_width=480\ntag_archive_path=./a.json\nignored_folders=\n"
    )
    with pytest.raises(ValueError, match="batch_size must be positive"):
        load_media_config(cfg_path)


def test_load_media_config_rejects_empty_extension_list(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\nmin_file_size_bytes=1\nskip_shared_not_owned=true\nskip_hidden=true\n"
        "early_exit_row_threshold=1\nmax_csv_rows=1\n\n"
        "[paths]\ncsv_output_dir=./o\nlog_dir=./l\n\n"
        "[media]\nphoto_extensions=\nvideo_extensions=mp4\nbatch_size=10\n"
        "thumbnail_width=480\ntag_archive_path=./a.json\nignored_folders=\n"
    )
    with pytest.raises(ValueError, match="photo_extensions must not be empty"):
        load_media_config(cfg_path)


def test_load_media_config_independent_ignored_folders(tmp_path: Path) -> None:
    """[media].ignored_folders and [scan].ignored_folders parsed independently."""
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\nmin_file_size_bytes=1\nskip_shared_not_owned=true\nskip_hidden=true\n"
        "early_exit_row_threshold=1\nmax_csv_rows=1\n"
        "ignored_folders =\n    /scan-only\n\n"
        "[paths]\ncsv_output_dir=./o\nlog_dir=./l\n\n"
        "[media]\nphoto_extensions=jpg\nvideo_extensions=mp4\nbatch_size=10\n"
        "thumbnail_width=480\ntag_archive_path=./a.json\n"
        "ignored_folders =\n    /media-only\n"
    )
    scan_cfg = load_config(cfg_path)
    media_cfg = load_media_config(cfg_path)
    assert scan_cfg.ignored_folders == ("/scan-only",)
    assert media_cfg.ignored_folders == ("/media-only",)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_dbx_client.py -v
```

Expected: `ImportError` or 5 new tests failing with `MediaConfig`/`load_media_config` not defined.

- [ ] **Step 3: Implement `MediaConfig` and `load_media_config` in `dbx_client.py`**

Append to `dbx-cleanup/dbx_client.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_dbx_client.py -v
```

Expected: all tests pass (existing + 5 new).

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/dbx_client.py dbx-cleanup/tests/test_dbx_client.py
git commit -m "feat(dbx_client): add MediaConfig and load_media_config"
```

---

### Task 3: `dbx_media.py` — pure helpers (classify, normalize_tag, fold_to_folders)

**Files:**
- Create: `dbx-cleanup/dbx_media.py`
- Create: `dbx-cleanup/tests/test_dbx_media.py`

- [ ] **Step 1: Write the failing tests**

Create `dbx-cleanup/tests/test_dbx_media.py`:

```python
from __future__ import annotations

import pytest

from dbx_media import classify_media, normalize_tag, fold_to_folders


def test_classify_media_photo_lowercase() -> None:
    assert classify_media("/a/b.jpg", frozenset({"jpg", "png"}), frozenset({"mp4"})) == "photo"


def test_classify_media_photo_uppercase() -> None:
    assert classify_media("/a/B.JPG", frozenset({"jpg"}), frozenset({"mp4"})) == "photo"


def test_classify_media_video() -> None:
    assert classify_media("/a/b.MoV", frozenset({"jpg"}), frozenset({"mov"})) == "video"


def test_classify_media_other() -> None:
    assert classify_media("/a/b.txt", frozenset({"jpg"}), frozenset({"mp4"})) == "other"


def test_classify_media_no_extension() -> None:
    assert classify_media("/a/README", frozenset({"jpg"}), frozenset({"mp4"})) == "other"


def test_classify_media_dotfile_no_extension() -> None:
    """`.gitignore` has no extension, just a hidden name."""
    assert classify_media("/a/.gitignore", frozenset({"jpg"}), frozenset({"mp4"})) == "other"


def test_normalize_tag_strips_hash_and_lowercases() -> None:
    assert normalize_tag("#Diwali") == "diwali"


def test_normalize_tag_spaces_to_hyphens() -> None:
    assert normalize_tag("Diwali 2019") == "diwali-2019"


def test_normalize_tag_strips_whitespace() -> None:
    assert normalize_tag("  seema  ") == "seema"


def test_normalize_tag_collapses_multiple_spaces() -> None:
    assert normalize_tag("a   b   c") == "a-b-c"


def test_normalize_tag_keeps_existing_hyphens() -> None:
    assert normalize_tag("diwali-2019") == "diwali-2019"


def test_normalize_tag_rejects_invalid_chars() -> None:
    with pytest.raises(ValueError, match="invalid tag"):
        normalize_tag("subject!")


def test_normalize_tag_rejects_too_long() -> None:
    with pytest.raises(ValueError, match="invalid tag"):
        normalize_tag("a" * 33)


def test_normalize_tag_accepts_32_chars() -> None:
    """32 is the max length per Dropbox rules — exactly 32 is fine."""
    assert normalize_tag("a" * 32) == "a" * 32


def test_normalize_tag_rejects_empty() -> None:
    with pytest.raises(ValueError, match="invalid tag"):
        normalize_tag("")


def test_normalize_tag_rejects_whitespace_only() -> None:
    with pytest.raises(ValueError, match="invalid tag"):
        normalize_tag("   ")


def test_fold_to_folders_basic() -> None:
    paths = ["/x/a.jpg", "/x/b.jpg", "/y/c.jpg"]
    result = fold_to_folders(paths)
    assert result == [("/x", ["/x/a.jpg", "/x/b.jpg"]),
                      ("/y", ["/y/c.jpg"])]


def test_fold_to_folders_sorted_by_cluster_size_desc() -> None:
    """Bigger clusters come first."""
    paths = ["/small/a.jpg", "/big/1.jpg", "/big/2.jpg", "/big/3.jpg"]
    result = fold_to_folders(paths)
    folders = [f for f, _ in result]
    assert folders == ["/big", "/small"]


def test_fold_to_folders_preserves_input_order_within_cluster() -> None:
    paths = ["/x/z.jpg", "/x/a.jpg", "/x/m.jpg"]
    result = fold_to_folders(paths)
    assert result == [("/x", ["/x/z.jpg", "/x/a.jpg", "/x/m.jpg"])]


def test_fold_to_folders_empty() -> None:
    assert fold_to_folders([]) == []


def test_fold_to_folders_root_level() -> None:
    """File at root has parent ''."""
    assert fold_to_folders(["/a.jpg"]) == [("", ["/a.jpg"])]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_dbx_media.py -v
```

Expected: `ImportError: No module named 'dbx_media'`.

- [ ] **Step 3: Implement pure helpers in `dbx_media.py`**

Create `dbx-cleanup/dbx_media.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_dbx_media.py -v
```

Expected: all 21 tests pass.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/dbx_media.py dbx-cleanup/tests/test_dbx_media.py
git commit -m "feat(dbx_media): add pure helpers (classify, normalize_tag, fold_to_folders)"
```

---

### Task 4: `dbx_media.py` — Dropbox helpers (tags + thumbnail)

**Files:**
- Modify: `dbx-cleanup/dbx_media.py`
- Modify: `dbx-cleanup/tests/test_dbx_media.py`

- [ ] **Step 1: Write the failing tests**

Append to `dbx-cleanup/tests/test_dbx_media.py`:

```python
from unittest.mock import MagicMock

from dbx_media import fetch_existing_tags, fetch_thumbnail, apply_tags


def _path_to_tags(path: str, tag_texts: list[str]) -> MagicMock:
    """Build a fake PathToTags result. tags is a list of objects with .tag_text."""
    pt = MagicMock()
    pt.path = path
    pt.tags = [MagicMock(tag_text=t) for t in tag_texts]
    return pt


def test_fetch_existing_tags_single_batch() -> None:
    """≤100 paths: one API call, results merged into dict."""
    client = MagicMock()
    client.files_tags_get_batch.return_value = MagicMock(paths_to_tags=[
        _path_to_tags("/a.jpg", []),
        _path_to_tags("/b.jpg", ["existing"]),
    ])
    result = fetch_existing_tags(client, ["/a.jpg", "/b.jpg"])
    assert result == {"/a.jpg": [], "/b.jpg": ["existing"]}
    assert client.files_tags_get_batch.call_count == 1
    client.files_tags_get_batch.assert_called_with(["/a.jpg", "/b.jpg"])


def test_fetch_existing_tags_chunks_above_100() -> None:
    """>100 paths: split into chunks of 100."""
    paths = [f"/p{i}.jpg" for i in range(250)]
    client = MagicMock()
    client.files_tags_get_batch.side_effect = [
        MagicMock(paths_to_tags=[_path_to_tags(p, []) for p in paths[0:100]]),
        MagicMock(paths_to_tags=[_path_to_tags(p, []) for p in paths[100:200]]),
        MagicMock(paths_to_tags=[_path_to_tags(p, []) for p in paths[200:250]]),
    ]
    result = fetch_existing_tags(client, paths)
    assert len(result) == 250
    assert client.files_tags_get_batch.call_count == 3
    # Verify each chunk size
    call_lengths = [len(call.args[0]) for call in client.files_tags_get_batch.call_args_list]
    assert call_lengths == [100, 100, 50]


def test_fetch_existing_tags_empty_input() -> None:
    """Zero paths: no API call, empty dict."""
    client = MagicMock()
    result = fetch_existing_tags(client, [])
    assert result == {}
    assert client.files_tags_get_batch.call_count == 0


def test_fetch_thumbnail_returns_bytes() -> None:
    client = MagicMock()
    response = MagicMock()
    response.content = b"\xff\xd8\xff\xe0fake-jpeg-bytes"
    client.files_get_thumbnail_v2.return_value = (MagicMock(), response)
    result = fetch_thumbnail(client, "/photo.jpg", 480)
    assert result == b"\xff\xd8\xff\xe0fake-jpeg-bytes"


def test_fetch_thumbnail_rejects_unsupported_width() -> None:
    client = MagicMock()
    with pytest.raises(ValueError, match="thumbnail width 333 not supported"):
        fetch_thumbnail(client, "/photo.jpg", 333)


def test_apply_tags_adds_each() -> None:
    client = MagicMock()
    apply_tags(client, "/photo.jpg", ["a", "b", "c"])
    assert client.files_tags_add.call_count == 3
    client.files_tags_add.assert_any_call("/photo.jpg", "a")
    client.files_tags_add.assert_any_call("/photo.jpg", "b")
    client.files_tags_add.assert_any_call("/photo.jpg", "c")


def test_apply_tags_empty_list_is_noop() -> None:
    client = MagicMock()
    apply_tags(client, "/photo.jpg", [])
    assert client.files_tags_add.call_count == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_dbx_media.py -v
```

Expected: `ImportError` for new symbols.

- [ ] **Step 3: Implement Dropbox helpers in `dbx_media.py`**

Append to `dbx-cleanup/dbx_media.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_dbx_media.py -v
```

Expected: all tests pass (7 new + 21 existing).

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/dbx_media.py dbx-cleanup/tests/test_dbx_media.py
git commit -m "feat(dbx_media): add Dropbox tag and thumbnail helpers"
```

---

### Task 5: `dbx_media.py` — tag archive load/save/merge

**Files:**
- Modify: `dbx-cleanup/dbx_media.py`
- Modify: `dbx-cleanup/tests/test_dbx_media.py`

- [ ] **Step 1: Write the failing tests**

Append to `dbx-cleanup/tests/test_dbx_media.py`:

```python
import json
from pathlib import Path

from dbx_media import load_archive, save_archive, merge_tagged, merge_deleted


def test_load_archive_missing_file_returns_empty(tmp_path: Path) -> None:
    """When the JSON file doesn't exist yet, return empty dict (first-run case)."""
    assert load_archive(tmp_path / "tag-archive.json") == {}


def test_load_archive_reads_existing(tmp_path: Path) -> None:
    p = tmp_path / "tag-archive.json"
    p.write_text(json.dumps({
        "/a.jpg": {"content_hash": "h1", "tags": ["x"], "last_updated": "2026-05-10T10:00:00"}
    }))
    archive = load_archive(p)
    assert archive == {
        "/a.jpg": {"content_hash": "h1", "tags": ["x"], "last_updated": "2026-05-10T10:00:00"}
    }


def test_save_archive_creates_parent_dir(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "deeper" / "tag-archive.json"
    save_archive(target, {"/a.jpg": {"content_hash": "h", "tags": [], "last_updated": "now"}})
    assert target.exists()
    assert json.loads(target.read_text()) == {
        "/a.jpg": {"content_hash": "h", "tags": [], "last_updated": "now"}
    }


def test_save_archive_writes_sorted_keys_and_indented(tmp_path: Path) -> None:
    """Sorted keys + indent=2 makes the file human-readable and diff-friendly."""
    target = tmp_path / "tag-archive.json"
    save_archive(target, {
        "/z.jpg": {"content_hash": "h2", "tags": [], "last_updated": "now"},
        "/a.jpg": {"content_hash": "h1", "tags": [], "last_updated": "now"},
    })
    text = target.read_text()
    assert text.index('"/a.jpg"') < text.index('"/z.jpg"')
    assert "\n  " in text  # indented


def test_merge_tagged_new_entry() -> None:
    archive: dict[str, dict] = {}
    merge_tagged(archive, "/a.jpg", "hash1", ["x", "y"], "2026-05-10T10:00:00")
    assert archive == {
        "/a.jpg": {"content_hash": "hash1", "tags": ["x", "y"],
                   "last_updated": "2026-05-10T10:00:00"}
    }


def test_merge_tagged_unions_existing_tags() -> None:
    archive = {
        "/a.jpg": {"content_hash": "h", "tags": ["a", "b"], "last_updated": "older"}
    }
    merge_tagged(archive, "/a.jpg", "h", ["b", "c"], "newer")
    assert archive["/a.jpg"]["tags"] == ["a", "b", "c"]  # sorted union
    assert archive["/a.jpg"]["last_updated"] == "newer"


def test_merge_tagged_updates_content_hash_if_changed() -> None:
    """If the file's content_hash changed (re-tagged after edit), update it."""
    archive = {
        "/a.jpg": {"content_hash": "old_hash", "tags": ["x"], "last_updated": "older"}
    }
    merge_tagged(archive, "/a.jpg", "new_hash", ["y"], "newer")
    assert archive["/a.jpg"]["content_hash"] == "new_hash"
    assert archive["/a.jpg"]["tags"] == ["x", "y"]


def test_merge_deleted_with_existing_entry() -> None:
    archive = {
        "/a.jpg": {"content_hash": "h", "tags": ["x"], "last_updated": "older"}
    }
    merge_deleted(archive, "/a.jpg", "2026-05-10T11:00:00")
    assert archive["/a.jpg"] == {
        "content_hash": "h",
        "tags": ["x"],
        "last_updated": "older",
        "deleted_at": "2026-05-10T11:00:00",
    }


def test_merge_deleted_no_prior_entry_is_noop() -> None:
    """Per spec: deleting a never-tagged path does NOT create an archive entry."""
    archive: dict[str, dict] = {}
    merge_deleted(archive, "/never-tagged.jpg", "2026-05-10T11:00:00")
    assert archive == {}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_dbx_media.py -v
```

Expected: `ImportError` for the four archive symbols.

- [ ] **Step 3: Implement archive I/O in `dbx_media.py`**

Append to `dbx-cleanup/dbx_media.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_dbx_media.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/dbx_media.py dbx-cleanup/tests/test_dbx_media.py
git commit -m "feat(dbx_media): add tag archive load/save/merge"
```

---

### Task 6: `count_media.py` — walk Dropbox and print photo/video counts

**Files:**
- Create: `dbx-cleanup/count_media.py`

`count_media.py` has only one piece of testable logic — the extension classification, which is already covered by `test_dbx_media.py::test_classify_media_*`. The walker itself is a thin orchestration around Dropbox SDK calls; we test it end-to-end via the integration test in Task 14.

- [ ] **Step 1: Create `count_media.py`**

```python
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
```

- [ ] **Step 2: Verify it loads (no syntax errors)**

```bash
cd dbx-cleanup && PYTHONPATH=. python -c "import count_media"
```

Expected: no output, no exceptions.

- [ ] **Step 3: Run existing test suite to confirm nothing else broke**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest -v
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add dbx-cleanup/count_media.py
git commit -m "feat(count_media): walk Dropbox and print photo/video counts"
```

---

### Task 7: `get_media.py` — `filter_untagged` + `select_batch` packing

**Files:**
- Create: `dbx-cleanup/get_media.py`
- Create: `dbx-cleanup/tests/test_get_media.py`

- [ ] **Step 1: Write the failing tests**

Create `dbx-cleanup/tests/test_get_media.py`:

```python
from __future__ import annotations

import pytest

from get_media import filter_untagged, select_batch


def test_filter_untagged_keeps_only_empty() -> None:
    candidates = ["/a.jpg", "/b.jpg", "/c.jpg"]
    tags = {"/a.jpg": [], "/b.jpg": ["existing"], "/c.jpg": []}
    assert filter_untagged(candidates, tags) == ["/a.jpg", "/c.jpg"]


def test_filter_untagged_treats_missing_as_untagged() -> None:
    """If a path didn't come back in the tags dict (shouldn't happen, but be defensive),
    treat as untagged."""
    candidates = ["/a.jpg", "/b.jpg"]
    tags = {"/a.jpg": []}
    assert filter_untagged(candidates, tags) == ["/a.jpg", "/b.jpg"]


def test_filter_untagged_empty_input() -> None:
    assert filter_untagged([], {}) == []


def test_select_batch_whole_clusters_fit() -> None:
    """Both clusters fit within budget — take both whole."""
    folded = [("/x", ["/x/a", "/x/b"]), ("/y", ["/y/c"])]
    assert select_batch(folded, batch_size=5) == ["/x/a", "/x/b", "/y/c"]


def test_select_batch_exact_fit() -> None:
    folded = [("/x", ["/x/a", "/x/b", "/x/c"])]
    assert select_batch(folded, batch_size=3) == ["/x/a", "/x/b", "/x/c"]


def test_select_batch_partial_cluster_fills_budget() -> None:
    """First cluster takes 3 of the 5-budget; second cluster has 4 files,
    only 2 fit — take partial slice."""
    folded = [("/x", ["/x/1", "/x/2", "/x/3"]),
              ("/y", ["/y/1", "/y/2", "/y/3", "/y/4"])]
    assert select_batch(folded, batch_size=5) == ["/x/1", "/x/2", "/x/3", "/y/1", "/y/2"]


def test_select_batch_huge_folder_partial_slice() -> None:
    """A single cluster bigger than batch_size — take a slice of it."""
    folded = [("/x", [f"/x/{i}" for i in range(200)])]
    result = select_batch(folded, batch_size=50)
    assert len(result) == 50
    assert result == [f"/x/{i}" for i in range(50)]


def test_select_batch_empty_input() -> None:
    assert select_batch([], batch_size=50) == []


def test_select_batch_zero_budget_returns_empty() -> None:
    folded = [("/x", ["/x/a"])]
    assert select_batch(folded, batch_size=0) == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_get_media.py -v
```

Expected: `ImportError: No module named 'get_media'`.

- [ ] **Step 3: Create `get_media.py` skeleton with the two pure functions**

Create `dbx-cleanup/get_media.py`:

```python
"""Engine for get_images.py and get_videos.py.

Walks Dropbox, looks up existing tags, filters to untagged, folder-clusters,
packs into a batch sized by config, downloads thumbnails, and writes a
self-contained HTML review page.
"""

from __future__ import annotations

from typing import Iterable


def filter_untagged(
    candidates: list[str],
    existing_tags: dict[str, list[str]],
) -> list[str]:
    """Return paths whose tag list is empty (or absent from the dict).
    Order is preserved from `candidates`."""
    return [p for p in candidates if not existing_tags.get(p)]


def select_batch(
    folded: list[tuple[str, list[str]]],
    batch_size: int,
) -> list[str]:
    """Pack folder clusters into a batch.

    Walk clusters in order (caller has already sorted by cluster size desc).
    For each cluster:
      - if the whole cluster fits in remaining budget, take it whole;
      - else if any budget remains, take a partial slice of it and stop;
      - else stop.

    This preserves "biggest clusters first" while filling the page when the
    last cluster doesn't quite fit whole."""
    if batch_size <= 0:
        return []
    out: list[str] = []
    remaining = batch_size
    for _folder, paths in folded:
        if remaining <= 0:
            break
        if len(paths) <= remaining:
            out.extend(paths)
            remaining -= len(paths)
        else:
            out.extend(paths[:remaining])
            remaining = 0
            break
    return out
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_get_media.py -v
```

Expected: all 9 tests pass.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/get_media.py dbx-cleanup/tests/test_get_media.py
git commit -m "feat(get_media): add filter_untagged and select_batch packing"
```

---

### Task 8: `get_media.py` — `build_html` (self-contained review page)

**Files:**
- Modify: `dbx-cleanup/get_media.py`
- Modify: `dbx-cleanup/tests/test_get_media.py`

- [ ] **Step 1: Write the failing tests**

Append to `dbx-cleanup/tests/test_get_media.py`:

```python
import base64

from get_media import build_html, BatchEntry


def test_build_html_has_title_and_export_button() -> None:
    html = build_html(entries=[], kind="image", timestamp="2026-05-10 14:32")
    assert "<title>" in html
    assert "Tag batch" in html
    assert "2026-05-10 14:32" in html
    assert 'id="export"' in html


def test_build_html_empty_entries_shows_friendly_message() -> None:
    html = build_html(entries=[], kind="image", timestamp="2026-05-10 14:32")
    assert "No untagged" in html


def test_build_html_groups_by_folder() -> None:
    entries = [
        BatchEntry(path="/x/a.jpg", filename="a.jpg", content_hash="h1",
                   existing_tags=[], thumbnail_bytes=b"\xff\xd8fake"),
        BatchEntry(path="/x/b.jpg", filename="b.jpg", content_hash="h2",
                   existing_tags=["existing"], thumbnail_bytes=b"\xff\xd8fake"),
        BatchEntry(path="/y/c.jpg", filename="c.jpg", content_hash="h3",
                   existing_tags=[], thumbnail_bytes=b"\xff\xd8fake"),
    ]
    html = build_html(entries=entries, kind="image", timestamp="2026-05-10 14:32")

    # Two folder sections
    assert html.count('<section class="folder"') == 2
    assert 'data-folder="/x"' in html
    assert 'data-folder="/y"' in html

    # Bulk-apply controls per folder
    assert html.count('class="bulk-tags"') == 2
    assert html.count('class="bulk-apply"') == 2

    # One row per entry
    assert html.count('class="row"') == 3
    assert 'data-path="/x/a.jpg"' in html
    assert 'data-hash="h1"' in html
    assert 'data-existing=""' in html
    assert 'data-existing="existing"' in html

    # Filenames shown
    assert "a.jpg" in html
    assert "b.jpg" in html
    assert "c.jpg" in html


def test_build_html_embeds_thumbnails_as_base64() -> None:
    fake_jpeg = b"\xff\xd8\xff\xe0FAKE"
    entries = [BatchEntry(
        path="/x/a.jpg", filename="a.jpg", content_hash="h", existing_tags=[],
        thumbnail_bytes=fake_jpeg,
    )]
    html = build_html(entries=entries, kind="image", timestamp="2026-05-10 14:32")
    expected_b64 = base64.b64encode(fake_jpeg).decode("ascii")
    assert f"data:image/jpeg;base64,{expected_b64}" in html


def test_build_html_includes_export_js() -> None:
    """The JS in the page should call URL.createObjectURL on a Blob; smoke check
    that the export logic is wired."""
    html = build_html(entries=[], kind="image", timestamp="2026-05-10 14:32")
    assert "createObjectURL" in html
    assert "Blob" in html


def test_build_html_escapes_html_special_chars() -> None:
    """If a path contains < or & or quotes, output must escape them."""
    entries = [BatchEntry(
        path="/x/<weird&name>.jpg", filename="<weird&name>.jpg",
        content_hash="h", existing_tags=[], thumbnail_bytes=b"x",
    )]
    html = build_html(entries=entries, kind="image", timestamp="2026-05-10 14:32")
    # The raw string should NOT appear unescaped in the HTML body.
    assert "<weird&name>" not in html or "&lt;weird&amp;name&gt;" in html
```

- [ ] **Step 2: Run tests to verify they fail**

Expected: `ImportError` for `build_html` and `BatchEntry`.

- [ ] **Step 3: Implement `BatchEntry` and `build_html` in `get_media.py`**

Append to `dbx-cleanup/get_media.py`:

```python
import base64
import html as html_lib
from dataclasses import dataclass, field
from typing import Literal


@dataclass
class BatchEntry:
    path: str
    filename: str
    content_hash: str
    existing_tags: list[str]
    thumbnail_bytes: bytes


_HTML_TEMPLATE_HEAD = """\
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif;
            margin: 1.5rem; max-width: 900px; }}
    header {{ position: sticky; top: 0; background: white; padding: 0.5rem 0;
              border-bottom: 1px solid #ddd; z-index: 10; }}
    h1 {{ margin: 0 0 0.5rem; font-size: 1.2rem; }}
    button {{ font-size: 1rem; padding: 0.4rem 0.8rem; cursor: pointer; }}
    .folder {{ margin: 2rem 0; }}
    .folder h2 {{ font-size: 1rem; color: #333; }}
    .bulk {{ margin: 0.5rem 0 1rem; padding: 0.5rem; background: #f5f5f5;
             border-radius: 4px; }}
    .bulk input {{ width: 60%; padding: 0.3rem; }}
    .row {{ display: flex; gap: 1rem; margin: 1rem 0; padding: 0.5rem;
             border: 1px solid #eee; border-radius: 4px; }}
    .row img {{ max-width: 480px; height: auto; }}
    .meta {{ flex: 1; }}
    .meta code {{ font-weight: bold; }}
    .meta label {{ display: block; margin: 0.4rem 0; }}
    .meta input[type=text] {{ width: 100%; padding: 0.3rem; }}
    .existing {{ color: #666; }}
    .empty {{ color: #888; font-style: italic; padding: 2rem; text-align: center; }}
  </style>
</head>
<body>
  <header>
    <h1>{title}</h1>
    <button id="export">Export edited CSV</button>
  </header>
"""

_HTML_TEMPLATE_TAIL = """\
  <script>
    // Per-folder "Apply to all" copies the bulk-tags input into every new-tags
    // input within the same .folder section.
    document.querySelectorAll('.bulk-apply').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var folder = btn.closest('.folder');
        var value = folder.querySelector('.bulk-tags').value;
        folder.querySelectorAll('.new-tags').forEach(function (input) {
          input.value = value;
        });
      });
    });

    // Export: build a CSV string from each .row and trigger a download.
    document.getElementById('export').addEventListener('click', function () {
      var csv = 'path,content_hash,filename,existing_tags,new_tags,delete\\n';
      document.querySelectorAll('.row').forEach(function (row) {
        var path = row.dataset.path;
        var hash = row.dataset.hash;
        var filename = row.querySelector('.filename').textContent;
        var existing = row.dataset.existing;
        var newTags = row.querySelector('.new-tags').value;
        var del = row.querySelector('.del').checked ? 'x' : '';
        // CSV quoting: wrap in double quotes if comma, quote, or newline present.
        function q(s) {
          if (s === '' || s == null) return '';
          if (/[,"\\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
          return s;
        }
        csv += [q(path), q(hash), q(filename), q(existing), q(newTags), q(del)].join(',') + '\\n';
      });
      var blob = new Blob([csv], { type: 'text/csv' });
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url;
      // Replace .html with .edited.csv in the current page filename
      var name = location.pathname.split('/').pop().replace(/\\.html$/, '.edited.csv');
      a.download = name || 'tag-batch.edited.csv';
      a.click();
      URL.revokeObjectURL(url);
    });
  </script>
</body>
</html>
"""


def _esc(s: str) -> str:
    """Escape for HTML text and attributes. quote=True covers " in attributes."""
    return html_lib.escape(s, quote=True)


def build_html(
    entries: list[BatchEntry],
    kind: Literal["image", "video"],
    timestamp: str,
) -> str:
    """Render entries to a self-contained HTML page. Empty entries → friendly
    'nothing to tag' message instead of a folder list."""
    kind_label = "photos" if kind == "image" else "videos"
    title = f"Tag batch — {len(entries)} {kind_label} from {timestamp}"

    parts: list[str] = [_HTML_TEMPLATE_HEAD.format(title=_esc(title))]

    if not entries:
        parts.append('  <p class="empty">No untagged ' + kind_label + ' in scope.</p>\n')
        parts.append(_HTML_TEMPLATE_TAIL)
        return "".join(parts)

    # Group by parent folder, preserving input order across folders.
    folder_groups: list[tuple[str, list[BatchEntry]]] = []
    current_folder: str | None = None
    current_list: list[BatchEntry] = []
    for e in entries:
        parent = e.path.rsplit("/", 1)[0]
        if parent != current_folder:
            if current_folder is not None:
                folder_groups.append((current_folder, current_list))
            current_folder = parent
            current_list = [e]
        else:
            current_list.append(e)
    if current_folder is not None:
        folder_groups.append((current_folder, current_list))

    for folder, group in folder_groups:
        parts.append(f'  <section class="folder" data-folder="{_esc(folder)}">\n')
        parts.append(f'    <h2>{_esc(folder)} <small>({len(group)} files)</small></h2>\n')
        parts.append('    <div class="bulk">\n')
        parts.append('      Apply to all in folder:\n')
        parts.append('      <input class="bulk-tags" placeholder="e.g. event-2019, family">\n')
        parts.append('      <button class="bulk-apply">Apply</button>\n')
        parts.append('    </div>\n')
        for e in group:
            existing_str = ",".join(e.existing_tags)
            existing_display = ", ".join(e.existing_tags) if e.existing_tags else "(none)"
            b64 = base64.b64encode(e.thumbnail_bytes).decode("ascii")
            parts.append(
                f'    <div class="row" data-path="{_esc(e.path)}" '
                f'data-hash="{_esc(e.content_hash)}" '
                f'data-existing="{_esc(existing_str)}">\n'
            )
            parts.append(f'      <img src="data:image/jpeg;base64,{b64}">\n')
            parts.append('      <div class="meta">\n')
            parts.append(f'        <code class="filename">{_esc(e.filename)}</code>\n')
            parts.append(f'        <div>existing: <span class="existing">{_esc(existing_display)}</span></div>\n')
            parts.append('        <label>new tags: <input type="text" class="new-tags" '
                         'placeholder="comma-separated, e.g. seema, performance"></label>\n')
            parts.append('        <label><input type="checkbox" class="del"> mark for deletion</label>\n')
            parts.append('      </div>\n')
            parts.append('    </div>\n')
        parts.append('  </section>\n')

    parts.append(_HTML_TEMPLATE_TAIL)
    return "".join(parts)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_get_media.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/get_media.py dbx-cleanup/tests/test_get_media.py
git commit -m "feat(get_media): add build_html for self-contained review page"
```

---

### Task 9: `get_media.py` — `main` orchestrator + `get_images.py`/`get_videos.py` wrappers

**Files:**
- Modify: `dbx-cleanup/get_media.py`
- Create: `dbx-cleanup/get_images.py`
- Create: `dbx-cleanup/get_videos.py`

The orchestration `main()` ties together: config load, Dropbox walk, tag lookup, filtering, folder-fold, batch-select, thumbnail fetch, HTML build, file write. Tests are coarse-grained (mocked client end-to-end through `run`).

- [ ] **Step 1: Write the failing test**

Append to `dbx-cleanup/tests/test_get_media.py`:

```python
from unittest.mock import MagicMock
from pathlib import Path

from dropbox.files import FileMetadata


def _fake_file_meta(name: str, path: str, content_hash: str) -> MagicMock:
    """Build a MagicMock(spec=FileMetadata) so isinstance() returns True
    in the production code under test."""
    m = MagicMock(spec=FileMetadata)
    m.name = name
    m.path_display = path
    m.content_hash = content_hash
    m.size = 1000
    return m


def _path_to_tags_mock(path: str, tag_texts: list[str]) -> MagicMock:
    pt = MagicMock()
    pt.path = path
    pt.tags = [MagicMock(tag_text=t) for t in tag_texts]
    return pt


def test_run_end_to_end_with_mocks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Full orchestration: walk → tag-lookup → filter → fold → pack → thumb → html."""
    from get_media import run

    # Config file in tmp_path so load_media_config works.
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\nmin_file_size_bytes=1\nskip_shared_not_owned=true\nskip_hidden=true\n"
        "early_exit_row_threshold=1\nmax_csv_rows=1\nignored_folders=\n\n"
        "[paths]\n"
        f"csv_output_dir={tmp_path}/output\n"
        f"log_dir={tmp_path}/logs\n\n"
        "[media]\nphoto_extensions=jpg\nvideo_extensions=mp4\nbatch_size=5\n"
        "thumbnail_width=480\n"
        f"tag_archive_path={tmp_path}/archive.json\n"
        "ignored_folders=\n"
    )

    client = MagicMock()
    # files_list_folder result: three jpgs and one txt in a single page.
    page = MagicMock()
    page.entries = [
        _fake_file_meta("a.jpg", "/x/a.jpg", "h1"),
        _fake_file_meta("b.jpg", "/x/b.jpg", "h2"),
        _fake_file_meta("c.jpg", "/y/c.jpg", "h3"),
        # d.txt isn't a MagicMock(spec=FileMetadata), so isinstance() fails -> skipped.
        MagicMock(spec=object),
    ]
    page.has_more = False
    client.files_list_folder.return_value = page

    # files_tags_get_batch: a.jpg untagged, b.jpg already tagged, c.jpg untagged.
    client.files_tags_get_batch.return_value = MagicMock(paths_to_tags=[
        _path_to_tags_mock("/x/a.jpg", []),
        _path_to_tags_mock("/x/b.jpg", ["existing"]),
        _path_to_tags_mock("/y/c.jpg", []),
    ])

    # files_get_thumbnail_v2: return fake bytes for any path.
    thumb_response = MagicMock()
    thumb_response.content = b"\xff\xd8FAKE"
    client.files_get_thumbnail_v2.return_value = (MagicMock(), thumb_response)

    # Mock auth + client builder at the get_media import points.
    monkeypatch.setattr("get_media.get_client", lambda token: client)
    monkeypatch.setattr("get_media.load_token", lambda: "fake-token")

    # CLI args.
    monkeypatch.setattr("sys.argv", ["get_images.py", "--config", str(cfg_path), "--root", "/"])

    rc = run(kind="image")
    assert rc == 0

    # An HTML file should be produced under csv_output_dir.
    html_files = list((tmp_path / "output").glob("tag-batch-*.html"))
    assert len(html_files) == 1
    html_text = html_files[0].read_text()
    # b.jpg was already tagged -> excluded; a.jpg and c.jpg included.
    assert "/x/a.jpg" in html_text
    assert "/x/b.jpg" not in html_text
    assert "/y/c.jpg" in html_text
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_get_media.py::test_run_end_to_end_with_mocks -v
```

Expected: `ImportError: cannot import name 'run' from 'get_media'`.

- [ ] **Step 3: Implement `run` in `get_media.py`**

Append to `dbx-cleanup/get_media.py`:

```python
import argparse
import sys
from datetime import datetime
from pathlib import Path

import dropbox  # noqa: F401  (kept for type clarity at the run() boundary)
from dropbox.exceptions import AuthError
from dropbox.files import FileMetadata, ListFolderResult

from dbx_client import MissingTokenError, get_client, load_media_config, load_token, with_retry
from dbx_media import classify_media, fetch_existing_tags, fetch_thumbnail, fold_to_folders


def _walk_candidates(
    client,
    root: str,
    photo_exts: frozenset[str],
    video_exts: frozenset[str],
    kind: Literal["image", "video"],
    ignored_folders: tuple[str, ...],
) -> list[tuple[str, str]]:
    """Walk Dropbox under `root`, return [(path, content_hash), ...] for files
    matching `kind`, excluding hidden segments and ignored folders."""
    want = "photo" if kind == "image" else "video"
    list_path = "" if root == "/" else root.rstrip("/")
    out: list[tuple[str, str]] = []
    result: ListFolderResult = with_retry(
        lambda: client.files_list_folder(list_path, recursive=True)
    )
    while True:
        for entry in result.entries:
            if not isinstance(entry, FileMetadata):
                continue
            if entry.content_hash is None:
                continue
            if any(seg.startswith(".") for seg in entry.path_display.split("/")):
                continue
            path_lower = entry.path_display.lower()
            if any(path_lower == f or path_lower.startswith(f + "/") for f in ignored_folders):
                continue
            if classify_media(entry.path_display, photo_exts, video_exts) == want:
                out.append((entry.path_display, entry.content_hash))
        if not result.has_more:
            break
        cursor = result.cursor
        result = with_retry(lambda c=cursor: client.files_list_folder_continue(c))
    return out


def run(kind: Literal["image", "video"]) -> int:
    parser = argparse.ArgumentParser(
        description=f"Build a tag-review batch for {'photos' if kind == 'image' else 'videos'}."
    )
    parser.add_argument("--config", default="config.ini")
    parser.add_argument("--root", default="/", help="Dropbox path to scan (default: /)")
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
        print(f"Dropbox auth failed: {exc}.", file=sys.stderr); return 1

    print(f"Walking Dropbox under {root}...")
    candidates = _walk_candidates(client, root, mc.photo_extensions, mc.video_extensions,
                                   kind, mc.ignored_folders)
    print(f"  found {len(candidates)} candidate {'photos' if kind == 'image' else 'videos'}")

    if not candidates:
        return _write_empty_html(mc, kind)

    print("Looking up existing tags...")
    paths = [p for p, _ in candidates]
    tags_by_path = fetch_existing_tags(client, paths)
    untagged_paths = filter_untagged(paths, tags_by_path)
    print(f"  {len(untagged_paths)} untagged")

    if not untagged_paths:
        return _write_empty_html(mc, kind)

    # Map back to (path, content_hash) only for untagged.
    hash_by_path = {p: h for p, h in candidates}
    folded = fold_to_folders(untagged_paths)
    selected_paths = select_batch(folded, mc.batch_size)
    print(f"  selected {len(selected_paths)} for this batch (batch_size={mc.batch_size})")

    print("Fetching thumbnails...")
    entries: list[BatchEntry] = []
    for i, p in enumerate(selected_paths, start=1):
        try:
            thumb = fetch_thumbnail(client, p, mc.thumbnail_width)
        except AuthError:
            raise
        except Exception as exc:
            print(f"  WARN: thumbnail failed for {p}: {exc}", file=sys.stderr)
            continue
        entries.append(BatchEntry(
            path=p,
            filename=p.rsplit("/", 1)[-1],
            content_hash=hash_by_path[p],
            existing_tags=tags_by_path.get(p, []),
            thumbnail_bytes=thumb,
        ))
        if i % 10 == 0:
            print(f"    {i}/{len(selected_paths)}")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    file_ts = datetime.now().strftime("%Y-%m-%d-%H%M")
    html = build_html(entries=entries, kind=kind, timestamp=timestamp)

    suffix = "images" if kind == "image" else "videos"
    out_path = mc.csv_output_dir / f"tag-batch-{suffix}-{file_ts}.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html)
    print(f"\nWrote {len(entries)} {suffix} to {out_path}")
    print(f"Open in browser, edit tags, click 'Export', then run:")
    print(f"  python update_{suffix}.py --config {args.config} --csv {out_path.with_suffix('.edited.csv')}")
    return 0


def _write_empty_html(mc, kind: Literal["image", "video"]) -> int:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    file_ts = datetime.now().strftime("%Y-%m-%d-%H%M")
    suffix = "images" if kind == "image" else "videos"
    html = build_html(entries=[], kind=kind, timestamp=timestamp)
    out_path = mc.csv_output_dir / f"tag-batch-{suffix}-{file_ts}.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html)
    print(f"No untagged {suffix} in scope. Wrote empty page to {out_path}")
    return 0
```

Also add `from typing import Literal` to the top imports if not already present (after Step 3 of Task 8, `Literal` may already be imported).

- [ ] **Step 4: Create the wrapper `get_images.py`**

```python
"""Build a tag-review batch for untagged photos. See get_media.run() for details."""

import sys
from get_media import run

if __name__ == "__main__":
    sys.exit(run(kind="image"))
```

- [ ] **Step 5: Create the wrapper `get_videos.py`**

```python
"""Build a tag-review batch for untagged videos. See get_media.run() for details."""

import sys
from get_media import run

if __name__ == "__main__":
    sys.exit(run(kind="video"))
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_get_media.py -v
```

Expected: all tests pass.

- [ ] **Step 7: Smoke-check the wrappers load**

```bash
cd dbx-cleanup && PYTHONPATH=. python -c "import get_images; import get_videos"
```

Expected: no output, no exceptions.

- [ ] **Step 8: Commit**

```bash
git add dbx-cleanup/get_media.py dbx-cleanup/get_images.py dbx-cleanup/get_videos.py \
        dbx-cleanup/tests/test_get_media.py
git commit -m "feat(get_media): main orchestrator + get_images/get_videos wrappers"
```

---

### Task 10: `update_media.py` — CSV parser

**Files:**
- Create: `dbx-cleanup/update_media.py`
- Create: `dbx-cleanup/tests/test_update_media.py`

- [ ] **Step 1: Write the failing tests**

Create `dbx-cleanup/tests/test_update_media.py`:

```python
from __future__ import annotations

from pathlib import Path

import pytest

from update_media import parse_csv, EditedRow


def test_parse_csv_happy_path(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,existing_tags,new_tags,delete\n"
        "/x/a.jpg,h1,a.jpg,,seema,\n"
        "/x/b.jpg,h2,b.jpg,existing,,x\n"
    )
    rows = parse_csv(csv_path)
    assert rows == [
        EditedRow(path="/x/a.jpg", content_hash="h1", filename="a.jpg",
                  existing_tags=[], new_tags=["seema"], marked_delete=False),
        EditedRow(path="/x/b.jpg", content_hash="h2", filename="b.jpg",
                  existing_tags=["existing"], new_tags=[], marked_delete=True),
    ]


def test_parse_csv_existing_tags_comma_split(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,existing_tags,new_tags,delete\n"
        '"/x/a.jpg",h1,a.jpg,"tag-a,tag-b","new1,new2",\n'
    )
    rows = parse_csv(csv_path)
    assert rows[0].existing_tags == ["tag-a", "tag-b"]
    assert rows[0].new_tags == ["new1", "new2"]


def test_parse_csv_strips_whitespace_in_tags(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,existing_tags,new_tags,delete\n"
        '/x/a.jpg,h,a.jpg,,"  seema , performance  ",\n'
    )
    rows = parse_csv(csv_path)
    assert rows[0].new_tags == ["seema", "performance"]


def test_parse_csv_tolerates_blank_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,existing_tags,new_tags,delete\n"
        "/x/a.jpg,h1,a.jpg,,seema,\n"
        "\n"
        "/x/b.jpg,h2,b.jpg,,family,\n"
    )
    rows = parse_csv(csv_path)
    assert len(rows) == 2


def test_parse_csv_missing_required_column(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,new_tags,delete\n"  # missing existing_tags
        "/x/a.jpg,h,a.jpg,seema,\n"
    )
    with pytest.raises(ValueError, match="missing required columns"):
        parse_csv(csv_path)


def test_parse_csv_delete_case_insensitive(tmp_path: Path) -> None:
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "path,content_hash,filename,existing_tags,new_tags,delete\n"
        "/x/a.jpg,h,a.jpg,,,X\n"
        "/x/b.jpg,h,b.jpg,,, x \n"
    )
    rows = parse_csv(csv_path)
    assert rows[0].marked_delete is True
    assert rows[1].marked_delete is True
```

- [ ] **Step 2: Run tests to verify they fail**

Expected: `ImportError`.

- [ ] **Step 3: Implement `parse_csv` in `update_media.py`**

Create `dbx-cleanup/update_media.py`:

```python
"""Engine for update_images.py and update_videos.py.

Reads the edited CSV produced by the HTML 'Export' button, validates everything,
prompts the user, applies native Dropbox tags or deletes flagged files,
merges results into the local JSON archive, and writes an audit log.
"""

from __future__ import annotations

import csv as csv_lib
from dataclasses import dataclass, field
from pathlib import Path


REQUIRED_COLUMNS = {"path", "content_hash", "filename",
                    "existing_tags", "new_tags", "delete"}


@dataclass(frozen=True)
class EditedRow:
    path: str
    content_hash: str
    filename: str
    existing_tags: list[str]
    new_tags: list[str]
    marked_delete: bool


def _split_tags(raw: str) -> list[str]:
    """Split a comma-joined tag string into a list, stripping whitespace.
    Empty input or all-whitespace returns []."""
    if not raw or not raw.strip():
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


def parse_csv(csv_path: Path) -> list[EditedRow]:
    """Parse the edited CSV exported from the HTML review page.
    Raises ValueError for missing required columns.
    Blank separator rows are tolerated."""
    rows: list[EditedRow] = []
    with csv_path.open(encoding="utf-8-sig") as f:
        reader = csv_lib.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - fieldnames
        if missing:
            raise ValueError(f"{csv_path}: CSV is missing required columns: "
                             f"{sorted(missing)}")
        for raw in reader:
            if not raw.get("path"):
                continue  # blank separator row
            rows.append(EditedRow(
                path=raw["path"],
                content_hash=raw["content_hash"],
                filename=raw["filename"],
                existing_tags=_split_tags(raw.get("existing_tags", "")),
                new_tags=_split_tags(raw.get("new_tags", "")),
                marked_delete=raw.get("delete", "").strip().lower() == "x",
            ))
    return rows
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_update_media.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/update_media.py dbx-cleanup/tests/test_update_media.py
git commit -m "feat(update_media): add CSV parser"
```

---

### Task 11: `update_media.py` — local validators (CONFLICT, INVALID_TAG, TOO_MANY_TAGS, EXCEEDS_MAX_ROWS)

**Files:**
- Modify: `dbx-cleanup/update_media.py`
- Modify: `dbx-cleanup/tests/test_update_media.py`

- [ ] **Step 1: Write the failing tests**

Append to `dbx-cleanup/tests/test_update_media.py`:

```python
from update_media import (
    ValidationProblem,
    validate_conflict_tag_and_delete,
    validate_max_rows,
    validate_tag_normalization_and_count,
)


def _row(path: str, new_tags: list[str], delete: bool = False,
         existing: list[str] | None = None) -> EditedRow:
    return EditedRow(path=path, content_hash="h", filename=path.rsplit("/", 1)[-1],
                     existing_tags=existing or [], new_tags=new_tags,
                     marked_delete=delete)


def test_validate_conflict_clean() -> None:
    rows = [_row("/a.jpg", ["seema"]), _row("/b.jpg", [], delete=True)]
    assert validate_conflict_tag_and_delete(rows) == []


def test_validate_conflict_detects_both_populated() -> None:
    rows = [_row("/a.jpg", ["seema"], delete=True)]
    problems = validate_conflict_tag_and_delete(rows)
    assert len(problems) == 1
    assert problems[0].code == "CONFLICT_TAG_AND_DELETE"
    assert "/a.jpg" in problems[0].offending_paths


def test_validate_max_rows_within_limit() -> None:
    rows = [_row(f"/p{i}.jpg", ["x"]) for i in range(10)]
    assert validate_max_rows(rows, max_csv_rows=10) == []


def test_validate_max_rows_exceeds() -> None:
    rows = [_row(f"/p{i}.jpg", ["x"]) for i in range(11)]
    problems = validate_max_rows(rows, max_csv_rows=10)
    assert len(problems) == 1
    assert problems[0].code == "EXCEEDS_MAX_ROWS"


def test_validate_tag_invalid_chars() -> None:
    rows = [_row("/a.jpg", ["valid", "in!valid"])]
    problems = validate_tag_normalization_and_count(rows)
    codes = [p.code for p in problems]
    assert "INVALID_TAG" in codes


def test_validate_tag_too_long() -> None:
    rows = [_row("/a.jpg", ["a" * 33])]
    problems = validate_tag_normalization_and_count(rows)
    assert any(p.code == "INVALID_TAG" for p in problems)


def test_validate_tag_count_at_limit() -> None:
    """existing 15 + new 5 = 20: ok (exactly at Dropbox cap)."""
    rows = [_row("/a.jpg", [f"new{i}" for i in range(5)],
                 existing=[f"old{i}" for i in range(15)])]
    problems = validate_tag_normalization_and_count(rows)
    assert all(p.code != "TOO_MANY_TAGS" for p in problems)


def test_validate_tag_count_over_limit() -> None:
    """existing 18 + new 5 = 23: rejects."""
    rows = [_row("/a.jpg", [f"new{i}" for i in range(5)],
                 existing=[f"old{i}" for i in range(18)])]
    problems = validate_tag_normalization_and_count(rows)
    assert any(p.code == "TOO_MANY_TAGS" for p in problems)


def test_validate_tag_count_dedupes_against_existing() -> None:
    """existing 18 + new 5 where 3 of the 5 already exist: union = 20, ok."""
    rows = [_row("/a.jpg", ["old0", "old1", "old2", "n1", "n2"],
                 existing=[f"old{i}" for i in range(18)])]
    problems = validate_tag_normalization_and_count(rows)
    assert all(p.code != "TOO_MANY_TAGS" for p in problems)


def test_validate_tag_normalizes_before_validating() -> None:
    """User input '#Diwali 2019' is valid post-normalization."""
    rows = [_row("/a.jpg", ["#Diwali 2019", "Seema"])]
    problems = validate_tag_normalization_and_count(rows)
    assert all(p.code != "INVALID_TAG" for p in problems)
```

- [ ] **Step 2: Run tests to verify they fail**

Expected: `ImportError`.

- [ ] **Step 3: Implement validators in `update_media.py`**

Append to `dbx-cleanup/update_media.py`:

```python
from dbx_media import normalize_tag


@dataclass(frozen=True)
class ValidationProblem:
    code: str
    message: str
    offending_paths: tuple[str, ...]


def validate_conflict_tag_and_delete(rows: list[EditedRow]) -> list[ValidationProblem]:
    """No row may have both new_tags non-empty AND marked_delete=True."""
    bad = [r for r in rows if r.marked_delete and r.new_tags]
    if not bad:
        return []
    return [ValidationProblem(
        code="CONFLICT_TAG_AND_DELETE",
        message=(f"{len(bad)} row(s) have both new tags AND delete=x. "
                 "A single row cannot be both tagged and deleted."),
        offending_paths=tuple(r.path for r in bad),
    )]


def validate_max_rows(rows: list[EditedRow], max_csv_rows: int) -> list[ValidationProblem]:
    """Total rows in CSV must not exceed the configured batch_size cap.
    Catches hand-edited CSVs that ballooned past the intended batch."""
    if len(rows) <= max_csv_rows:
        return []
    return [ValidationProblem(
        code="EXCEEDS_MAX_ROWS",
        message=(f"{len(rows)} rows in CSV; cap is {max_csv_rows}. "
                 "Reduce the file or raise [media].batch_size."),
        offending_paths=tuple(r.path for r in rows),
    )]


def validate_tag_normalization_and_count(
    rows: list[EditedRow],
) -> list[ValidationProblem]:
    """For each row's new_tags:
      1. Normalize each (strips #, lowercases, spaces->hyphens, validates)
      2. Compute the would-be union with existing_tags
      3. Reject if any normalization fails OR if union size > 20.
    Returns problems with row context."""
    invalid: list[tuple[str, str]] = []  # (path, offending_tag)
    too_many: list[tuple[str, int]] = []
    for r in rows:
        normalized_new: list[str] = []
        for raw in r.new_tags:
            try:
                normalized_new.append(normalize_tag(raw))
            except ValueError:
                invalid.append((r.path, raw))
        if r.marked_delete:
            continue  # tag count check doesn't apply to delete-only rows
        union = set(r.existing_tags) | set(normalized_new)
        if len(union) > 20:
            too_many.append((r.path, len(union)))

    problems: list[ValidationProblem] = []
    if invalid:
        msg_parts = [f"{p}: {t!r}" for p, t in invalid]
        problems.append(ValidationProblem(
            code="INVALID_TAG",
            message=("Tags failing Dropbox's rules (a-z, 0-9, hyphens; 1-32 chars). "
                     "Offending: " + "; ".join(msg_parts)),
            offending_paths=tuple({p for p, _ in invalid}),
        ))
    if too_many:
        msg_parts = [f"{p} would have {n} tags" for p, n in too_many]
        problems.append(ValidationProblem(
            code="TOO_MANY_TAGS",
            message=("Dropbox allows max 20 tags per file. " + "; ".join(msg_parts)),
            offending_paths=tuple(p for p, _ in too_many),
        ))
    return problems
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_update_media.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/update_media.py dbx-cleanup/tests/test_update_media.py
git commit -m "feat(update_media): local validators (CONFLICT, INVALID_TAG, TOO_MANY_TAGS, EXCEEDS_MAX_ROWS)"
```

---

### Task 12: `update_media.py` — Dropbox-side validators (PATH_NOT_FOUND, HASH_CHANGED)

**Files:**
- Modify: `dbx-cleanup/update_media.py`
- Modify: `dbx-cleanup/tests/test_update_media.py`

- [ ] **Step 1: Write the failing tests**

Append to `dbx-cleanup/tests/test_update_media.py`:

```python
from unittest.mock import MagicMock
from dropbox.exceptions import ApiError

from update_media import validate_paths_and_hashes


def test_validate_paths_and_hashes_all_ok() -> None:
    client = MagicMock()
    # Each get_metadata returns the matching hash for the path.
    def _meta(path: str) -> MagicMock:
        m = MagicMock(); m.content_hash = {"/a.jpg": "h1", "/b.jpg": "h2"}[path]; return m
    client.files_get_metadata.side_effect = _meta
    rows = [_row("/a.jpg", ["x"]), _row("/b.jpg", ["y"])]
    rows = [EditedRow(r.path, {"/a.jpg": "h1", "/b.jpg": "h2"}[r.path],
                      r.filename, r.existing_tags, r.new_tags, r.marked_delete)
            for r in rows]
    assert validate_paths_and_hashes(client, rows) == []


def test_validate_paths_path_not_found() -> None:
    client = MagicMock()
    client.files_get_metadata.side_effect = ApiError(
        "req-id", MagicMock(__str__=lambda self: "path/not_found"), "user", "user")
    rows = [EditedRow("/missing.jpg", "h", "missing.jpg", [], ["x"], False)]
    problems = validate_paths_and_hashes(client, rows)
    assert len(problems) == 1
    assert problems[0].code == "PATH_NOT_FOUND"
    assert "/missing.jpg" in problems[0].offending_paths


def test_validate_paths_hash_changed() -> None:
    client = MagicMock()
    meta = MagicMock(); meta.content_hash = "different_hash"
    client.files_get_metadata.return_value = meta
    rows = [EditedRow("/a.jpg", "csv_hash", "a.jpg", [], ["x"], False)]
    problems = validate_paths_and_hashes(client, rows)
    assert len(problems) == 1
    assert problems[0].code == "HASH_CHANGED"


def test_validate_paths_skips_unmarked_unchanged_rows() -> None:
    """A row with no new_tags and no delete flag is a no-op — skip the API call."""
    client = MagicMock()
    rows = [EditedRow("/a.jpg", "h", "a.jpg", [], [], False)]
    problems = validate_paths_and_hashes(client, rows)
    assert problems == []
    assert client.files_get_metadata.call_count == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Expected: `ImportError: cannot import name 'validate_paths_and_hashes'`.

- [ ] **Step 3: Implement the Dropbox-side validator**

Append to `dbx-cleanup/update_media.py`:

```python
from dropbox.exceptions import ApiError

from dbx_client import with_retry


def validate_paths_and_hashes(client, rows: list[EditedRow]) -> list[ValidationProblem]:
    """For each row that will result in a Dropbox action (tag-add or delete),
    verify the path exists AND content_hash matches what's in the CSV.
    Combined into one API call per row (files_get_metadata)."""
    missing: list[str] = []
    changed: list[str] = []
    for row in rows:
        if not row.marked_delete and not row.new_tags:
            continue  # no-op row, skip API call
        try:
            meta = with_retry(lambda r=row: client.files_get_metadata(r.path))
        except ApiError as exc:
            if "not_found" in str(exc.error):
                missing.append(row.path)
                continue
            raise
        if getattr(meta, "content_hash", None) != row.content_hash:
            changed.append(row.path)

    problems: list[ValidationProblem] = []
    if missing:
        problems.append(ValidationProblem(
            code="PATH_NOT_FOUND",
            message=(f"{len(missing)} path(s) no longer exist in Dropbox. "
                     "Re-run get_images.py / get_videos.py to refresh the batch."),
            offending_paths=tuple(missing),
        ))
    if changed:
        problems.append(ValidationProblem(
            code="HASH_CHANGED",
            message=(f"{len(changed)} file(s) have changed since the scan. "
                     "Re-run get_images.py / get_videos.py to refresh."),
            offending_paths=tuple(changed),
        ))
    return problems
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest tests/test_update_media.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add dbx-cleanup/update_media.py dbx-cleanup/tests/test_update_media.py
git commit -m "feat(update_media): Dropbox-side validators (PATH_NOT_FOUND, HASH_CHANGED)"
```

---

### Task 13: `update_media.py` — execute, archive merge, audit log, `main` + wrappers

**Files:**
- Modify: `dbx-cleanup/update_media.py`
- Modify: `dbx-cleanup/tests/test_update_media.py`
- Create: `dbx-cleanup/update_images.py`
- Create: `dbx-cleanup/update_videos.py`

- [ ] **Step 1: Write the failing tests**

Append to `dbx-cleanup/tests/test_update_media.py`:

```python
from update_media import execute_actions, write_error_log


def test_execute_tags_a_row() -> None:
    client = MagicMock()
    archive: dict[str, dict] = {}
    rows = [EditedRow("/a.jpg", "h", "a.jpg", [], ["seema", "performance"], False)]
    audit_path = Path("/tmp/test-tag-log.csv")  # tmp_path not available outside fixture
    # Use an in-memory write target instead:
    # We rewrite this test more carefully below.


def test_execute_tags_new_tag_only(tmp_path: Path) -> None:
    """Row with new_tags only -> apply_tags called with deduped list."""
    client = MagicMock()
    archive: dict[str, dict] = {}
    rows = [EditedRow("/a.jpg", "h", "a.jpg",
                      existing_tags=["already"],
                      new_tags=["already", "seema"],  # already dup, only seema new
                      marked_delete=False)]
    audit_path = tmp_path / "tag-log.csv"
    summary = execute_actions(client, rows, archive, audit_path)
    # apply_tags should have been called for "seema" only
    client.files_tags_add.assert_called_once_with("/a.jpg", "seema")
    assert summary.tagged_count == 1
    assert summary.deleted_count == 0
    assert summary.skipped_count == 0
    assert summary.error_count == 0
    # Archive should have the merged tags
    assert set(archive["/a.jpg"]["tags"]) == {"already", "seema"}
    # Audit log should exist with a tagged row
    assert audit_path.exists()
    contents = audit_path.read_text()
    assert "/a.jpg" in contents
    assert "tagged" in contents


def test_execute_deletes_a_row(tmp_path: Path) -> None:
    client = MagicMock()
    archive = {"/a.jpg": {"content_hash": "h", "tags": ["x"], "last_updated": "older"}}
    rows = [EditedRow("/a.jpg", "h", "a.jpg", ["x"], [], marked_delete=True)]
    audit_path = tmp_path / "tag-log.csv"
    summary = execute_actions(client, rows, archive, audit_path)
    client.files_delete_v2.assert_called_once_with("/a.jpg")
    assert summary.deleted_count == 1
    assert "deleted_at" in archive["/a.jpg"]


def test_execute_skips_noop_row(tmp_path: Path) -> None:
    client = MagicMock()
    archive: dict[str, dict] = {}
    rows = [EditedRow("/a.jpg", "h", "a.jpg", [], [], marked_delete=False)]
    audit_path = tmp_path / "tag-log.csv"
    summary = execute_actions(client, rows, archive, audit_path)
    assert summary.skipped_count == 1
    assert client.files_tags_add.call_count == 0
    assert client.files_delete_v2.call_count == 0


def test_execute_continues_on_per_row_error(tmp_path: Path) -> None:
    """One failing tag_add doesn't abort the rest."""
    client = MagicMock()
    # First call raises, second succeeds.
    client.files_tags_add.side_effect = [
        ApiError("rid", MagicMock(__str__=lambda self: "tag/conflict"), "u", "u"),
        None,
    ]
    archive: dict[str, dict] = {}
    rows = [
        EditedRow("/a.jpg", "h", "a.jpg", [], ["fail"], False),
        EditedRow("/b.jpg", "h", "b.jpg", [], ["ok"], False),
    ]
    audit_path = tmp_path / "tag-log.csv"
    summary = execute_actions(client, rows, archive, audit_path)
    assert summary.error_count == 1
    assert summary.tagged_count == 1
    assert "/b.jpg" in archive  # success → archive updated
    assert "/a.jpg" not in archive  # failure → archive NOT updated


def test_write_error_log(tmp_path: Path) -> None:
    log_path = tmp_path / "error.log"
    problems = [
        ValidationProblem(code="X", message="something happened",
                          offending_paths=("/a.jpg", "/b.jpg")),
    ]
    write_error_log(problems, log_path)
    contents = log_path.read_text()
    assert "Pre-flight validation failed" in contents
    assert "[X]" in contents
    assert "/a.jpg" in contents
    assert "/b.jpg" in contents
```

- [ ] **Step 2: Run tests to verify they fail**

Expected: `ImportError`.

- [ ] **Step 3: Implement `ExecutionSummary`, `execute_actions`, `write_error_log` in `update_media.py`**

Append to `dbx-cleanup/update_media.py`:

```python
from datetime import datetime

from dropbox.exceptions import AuthError, DropboxException

from dbx_media import apply_tags, merge_tagged, merge_deleted


@dataclass(frozen=True)
class ExecutionSummary:
    tagged_count: int
    deleted_count: int
    skipped_count: int
    error_count: int
    tags_added_total: int
    log_path: Path


AUDIT_HEADER = ["timestamp", "path", "action", "tags_added",
                "tags_skipped_already_present", "dropbox_response"]


def write_error_log(problems: list[ValidationProblem], log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w") as f:
        f.write(f"Pre-flight validation failed at {datetime.now().isoformat()}\n")
        f.write("No Dropbox writes were performed.\n\n")
        for p in problems:
            f.write(f"[{p.code}] {p.message}\n")
            for path in p.offending_paths:
                f.write(f"  - {path}\n")
            f.write("\n")


def execute_actions(
    client,
    rows: list[EditedRow],
    archive: dict[str, dict],
    log_path: Path,
) -> ExecutionSummary:
    """For each row: delete OR add new tags (deduped) OR skip.
    Updates the archive dict in place for successful actions.
    Writes one audit-log row per attempt."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    tagged = 0
    deleted = 0
    skipped = 0
    errors = 0
    tags_added_total = 0

    with log_path.open("w", newline="") as f:
        writer = csv_lib.writer(f)
        writer.writerow(AUDIT_HEADER)
        for row in rows:
            ts = datetime.now().isoformat()
            if row.marked_delete:
                try:
                    with_retry(lambda r=row: client.files_delete_v2(r.path))
                    merge_deleted(archive, row.path, ts)
                    deleted += 1
                    writer.writerow([ts, row.path, "deleted", "", "",
                                     "moved to recycle bin"])
                    print(f"  deleted: {row.path}")
                except AuthError:
                    raise
                except DropboxException as exc:
                    errors += 1
                    writer.writerow([ts, row.path, "error", "", "", str(exc)])
                    print(f"  ERROR  : {row.path} ({exc})")
            elif row.new_tags:
                # Normalize new tags (validators already verified these pass).
                # Dedupe against existing.
                from dbx_media import normalize_tag
                normalized_new = [normalize_tag(t) for t in row.new_tags]
                already = set(row.existing_tags)
                to_add = [t for t in normalized_new if t not in already]
                already_present = [t for t in normalized_new if t in already]
                try:
                    apply_tags(client, row.path, to_add)
                    # Archive merge with union (includes both existing and new).
                    merge_tagged(archive, row.path, row.content_hash,
                                 sorted(set(row.existing_tags) | set(normalized_new)), ts)
                    tagged += 1
                    tags_added_total += len(to_add)
                    writer.writerow([ts, row.path, "tagged",
                                     "|".join(to_add), "|".join(already_present),
                                     "ok"])
                    print(f"  tagged : {row.path}  +[{', '.join(to_add)}]")
                except AuthError:
                    raise
                except DropboxException as exc:
                    errors += 1
                    writer.writerow([ts, row.path, "error",
                                     "|".join(to_add), "|".join(already_present),
                                     str(exc)])
                    print(f"  ERROR  : {row.path} ({exc})")
            else:
                skipped += 1
                writer.writerow([ts, row.path, "skipped", "", "", ""])

    return ExecutionSummary(
        tagged_count=tagged, deleted_count=deleted,
        skipped_count=skipped, error_count=errors,
        tags_added_total=tags_added_total, log_path=log_path,
    )
```

- [ ] **Step 4: Add `main` + `run` to `update_media.py`**

Append:

```python
import argparse
import sys

from dropbox.exceptions import AuthError
from typing import Literal

from dbx_client import MissingTokenError, get_client, load_media_config, load_token
from dbx_media import load_archive, save_archive


def run(kind: Literal["image", "video"]) -> int:
    parser = argparse.ArgumentParser(
        description=f"Apply edited {'image' if kind == 'image' else 'video'} tags to Dropbox."
    )
    parser.add_argument("--config", default="config.ini")
    parser.add_argument("--csv", required=True,
                        help="Path to the .edited.csv exported from the HTML review page.")
    args = parser.parse_args()

    try:
        mc = load_media_config(Path(args.config))
        token = load_token()
        client = get_client(token)
    except FileNotFoundError as exc:
        print(f"Config error: {exc}", file=sys.stderr); return 1
    except MissingTokenError as exc:
        print(f"Token error: {exc}", file=sys.stderr); return 1
    except AuthError as exc:
        print(f"Dropbox auth failed: {exc}.", file=sys.stderr); return 1

    try:
        rows = parse_csv(Path(args.csv))
    except (FileNotFoundError, ValueError) as exc:
        print(f"CSV error: {exc}", file=sys.stderr); return 1

    actionable = sum(1 for r in rows if r.marked_delete or r.new_tags)
    if actionable == 0:
        print("No rows with new tags or delete marks. Nothing to do.")
        return 0

    print(f"Pre-flight validation on {len(rows)} rows ({actionable} actionable)...")
    problems: list[ValidationProblem] = []
    problems.extend(validate_conflict_tag_and_delete(rows))
    problems.extend(validate_max_rows(rows, mc.batch_size))
    problems.extend(validate_tag_normalization_and_count(rows))
    problems.extend(validate_paths_and_hashes(client, rows))

    if problems:
        ts = datetime.now().strftime("%Y-%m-%d-%H%M")
        log_path = mc.log_dir / f"error-{ts}.log"
        write_error_log(problems, log_path)
        print(f"\nValidation failed with {len(problems)} problem(s). See {log_path}")
        for p in problems:
            print(f"  [{p.code}] {p.message}")
        return 2

    print("All validations passed.")
    confirm = input(f"\nAbout to apply {actionable} change(s) to Dropbox. Type 'yes': ")
    if confirm.strip() != "yes":
        print("Aborted by user."); return 1

    archive = load_archive(mc.tag_archive_path)
    ts = datetime.now().strftime("%Y-%m-%d-%H%M")
    audit_path = mc.log_dir / f"tag-log-{ts}.csv"
    try:
        summary = execute_actions(client, rows, archive, audit_path)
    except AuthError as exc:
        print(f"\nDropbox auth failed mid-batch: {exc}. Audit log at {audit_path}.",
              file=sys.stderr)
        save_archive(mc.tag_archive_path, archive)  # save partial progress
        return 1

    save_archive(mc.tag_archive_path, archive)
    print(f"\nDone. Tagged: {summary.tagged_count} "
          f"({summary.tags_added_total} tags added), "
          f"Deleted: {summary.deleted_count}, "
          f"Skipped: {summary.skipped_count}, "
          f"Errors: {summary.error_count}")
    print(f"Audit log: {summary.log_path}")
    print(f"Archive:   {mc.tag_archive_path} ({len(archive)} paths total)")
    return 0 if summary.error_count == 0 else 3
```

- [ ] **Step 5: Create `update_images.py`**

```python
"""Apply edited photo tags to Dropbox. See update_media.run() for details."""

import sys
from update_media import run

if __name__ == "__main__":
    sys.exit(run(kind="image"))
```

- [ ] **Step 6: Create `update_videos.py`**

```python
"""Apply edited video tags to Dropbox. See update_media.run() for details."""

import sys
from update_media import run

if __name__ == "__main__":
    sys.exit(run(kind="video"))
```

- [ ] **Step 7: Run all tests**

```bash
cd dbx-cleanup && PYTHONPATH=. pytest -v
```

Expected: all tests pass.

- [ ] **Step 8: Smoke-check wrappers load**

```bash
cd dbx-cleanup && PYTHONPATH=. python -c "import update_images; import update_videos"
```

Expected: no output, no exceptions.

- [ ] **Step 9: Commit**

```bash
git add dbx-cleanup/update_media.py dbx-cleanup/update_images.py \
        dbx-cleanup/update_videos.py dbx-cleanup/tests/test_update_media.py
git commit -m "feat(update_media): execute, archive merge, audit log, main + wrappers"
```

---

### Task 14: `seed_test_media.py` — test fixture uploader

**Files:**
- Create: `dbx-cleanup/seed_test_media.py`

- [ ] **Step 1: Create the seed script**

```python
"""One-shot uploader: populate /test-media/ with known fixtures so the
tagging scripts can be exercised end-to-end against real Dropbox.

Idempotent: clears /test-media/ before populating.

The "photos" are tiny 1×1 PNGs (67 bytes each, well-known canonical encoding).
PNG works fine with Dropbox's tagging and thumbnail APIs — and the project's
default config has `png` in [media].photo_extensions, as does config.test.ini.

The "video" is a minimal MP4 ftyp atom. Dropbox accepts the upload and the
file appears in count_media + get_videos batches, but thumbnail generation
may fail (logged WARN by get_videos); that's an acceptable test of the
error-tolerance path."""

from __future__ import annotations

import base64
import sys

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import WriteMode

from dbx_client import get_client, load_token, with_retry

ROOT = "/test-media"

# 1×1 white PNG (67 bytes). Canonical/well-known minimal PNG; verifiable
# by base64-decoding and feeding to any PNG decoder.
TINY_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVQI12P4//8/AAX+Av58GHV5AAAAAElFTkSuQmCC"
)

# Minimal MP4 (28 bytes): ftyp box declaring isom brand. Dropbox accepts
# this as an .mp4 upload; thumbnail generation will likely fail (handled
# by get_videos as a per-file WARN, not a fatal).
TINY_MP4 = bytes.fromhex(
    "0000001c66747970697336366d00000000697336366d6d70343200000008"
)


def reset_root(client: dropbox.Dropbox) -> None:
    try:
        with_retry(lambda: client.files_delete_v2(ROOT))
        print(f"Cleared existing {ROOT}")
    except ApiError as exc:
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

    upload(client, f"{ROOT}/eventA/photo1.png", TINY_PNG)
    upload(client, f"{ROOT}/eventA/photo2.png", TINY_PNG)
    upload(client, f"{ROOT}/eventA/photo3.png", TINY_PNG)
    upload(client, f"{ROOT}/eventB/photo4.png", TINY_PNG)
    upload(client, f"{ROOT}/eventB/video1.mp4", TINY_MP4)
    upload(client, f"{ROOT}/other/doc.pdf", b"%PDF-1.4\n%minimal\n")

    # Pre-tag photo3 so it gets excluded from the next get_images batch.
    print("\nPre-tagging /eventA/photo3.png with 'already-tagged'...")
    with_retry(lambda: client.files_tags_add(f"{ROOT}/eventA/photo3.png", "already-tagged"))

    print("\nSeed complete. Now run:")
    print(f"  python count_media.py --config config.test.ini --root {ROOT}")
    print(f"  python get_images.py --config config.test.ini --root {ROOT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Verify the script loads**

```bash
cd dbx-cleanup && PYTHONPATH=. python -c "import seed_test_media"
```

Expected: no errors. (Don't run it — that would hit real Dropbox; the integration test in Task 16 is where it actually runs.)

- [ ] **Step 3: Commit**

```bash
git add dbx-cleanup/seed_test_media.py
git commit -m "feat(seed_test_media): test fixture uploader for tagging tests"
```

---

### Task 15: README updates

**Files:**
- Modify: `dbx-cleanup/README.md`
- Modify: `README.md` (root)

- [ ] **Step 1: Add a "Tagging photos and videos" section to `dbx-cleanup/README.md`**

Append after the existing duplicates content, before the "Tests" section:

```markdown
## Tagging photos and videos

Five additional scripts let you assign native Dropbox tags to photos and videos in batches, with a self-contained HTML review page and a portable JSON archive.

- `count_media.py` — prints total photo/video counts. Read-only.
- `get_images.py` / `get_videos.py` — produce an HTML page with thumbnails + tag-input fields for the next batch of untagged media.
- `update_images.py` / `update_videos.py` — apply edited tags to Dropbox and optionally delete flagged files; merge into a local JSON archive.

### How it works

**`get_images.py`** (and the video equivalent):

1. Walks Dropbox under `--root` (default `/`), filtering by extensions in `[media].photo_extensions` and skipping `[media].ignored_folders` + hidden paths.
2. Looks up every candidate's current native Dropbox tags via `files_tags_get_batch` (chunked at 100 paths/call).
3. Drops files that already have tags.
4. Folder-clusters the rest and packs up to `[media].batch_size` files into the batch.
5. Downloads a thumbnail at `[media].thumbnail_width` per file.
6. Writes `output/tag-batch-images-YYYY-MM-DD-HHMM.html` — a single self-contained file with base64-embedded thumbs, per-folder "Apply to all" controls, and an Export button.

**You** open the HTML in your browser, fill in tags (comma-separated), optionally check "mark for deletion" on a row, and click "Export". The browser downloads `tag-batch-images-YYYY-MM-DD-HHMM.edited.csv`.

**`update_images.py`** then validates and applies:

| Validation | What it catches |
|---|---|
| `PATH_NOT_FOUND` | File moved/renamed/deleted since the batch was generated |
| `HASH_CHANGED` | File content changed since the batch was generated |
| `CONFLICT_TAG_AND_DELETE` | A single row has both `new_tags` and `delete=x` |
| `INVALID_TAG` | A tag fails Dropbox's rules (a-z, 0-9, hyphens, 1-32 chars) after normalization |
| `TOO_MANY_TAGS` | Existing + new tags would exceed Dropbox's 20-per-file cap |
| `EXCEEDS_MAX_ROWS` | The CSV has more rows than `[media].batch_size` |

All validations run to completion; if any fails the whole batch aborts (exit 2). On success, the user types `yes`, then each row is either tagged (deduped against existing) or moved to the recycle bin. Errors on individual rows continue the batch; `AuthError` aborts immediately.

After the run, three artifacts:
- `output/tag-archive.json` — the persistent local archive, keyed by Dropbox path. Survives a future migration away from Dropbox.
- `logs/tag-log-YYYY-MM-DD-HHMM.csv` — per-row audit (timestamp, path, action, tags added/skipped, response).
- The Dropbox files themselves now carry their new tags, searchable in the web UI as `tag:diwali-2019`.

### Tag input format

Tags in the HTML are comma-separated. The script normalizes each one before sending to Dropbox:
- Leading `#` is stripped (so `#seema` and `seema` both work)
- Lowercased
- Internal whitespace runs become single hyphens (so `Diwali 2019` → `diwali-2019`)
- Validated: a-z, 0-9, hyphens only; 1-32 chars

Anything still failing after normalization is rejected at pre-flight validation; the original value is named in the error log so you can fix the CSV and re-run.

### Test before unleashing (tagging)

```bash
# 1. Seed /test-media/ with known fixtures (4 photos + 1 video + 1 PDF)
python seed_test_media.py

# 2. Count — expect "Photos: 4" and "Videos: 1" (PDF excluded)
python count_media.py --config config.test.ini --root /test-media

# 3. Build a photo batch
python get_images.py --config config.test.ini --root /test-media
#    → output/tag-batch-images-<ts>.html (3 untagged photos; photo3.png excluded
#    because seed_test_media.py pre-tagged it).

# 4. Open the HTML in your browser. Expected:
#    - 2 folder sections (eventA, eventB), each with an "Apply to all" control
#    - 3 thumbnails (~256 px wide at test config)
#    - existing tags shown as "(none)" for each row
#
#    Add tags to TWO of the rows. Mark the THIRD row 'x' for deletion
#    (leave its new-tags input blank — a single row can't be both tagged AND deleted).
#    Click "Export edited CSV".

# 5. Apply the edits
python update_images.py --config config.test.ini \
    --csv output/tag-batch-images-<ts>.edited.csv

# 6. Verify in the Dropbox web UI:
#    - The two tagged photos now show their new tags next to the filename
#    - The 'x'-marked file is in "Deleted files"
#    - eventA/photo3.png is unchanged (was excluded; was already-tagged)

# 7. Verify locally:
#    - output/tag-archive-test.json has 2 entries
#    - logs/tag-log-<ts>.csv shows 2 tagged + 1 deleted + 0 errors

# 8. Repeat for videos
python get_videos.py --config config.test.ini --root /test-media
python update_videos.py --config config.test.ini \
    --csv output/tag-batch-videos-<ts>.edited.csv

# 9. Validation drill: open one of the exported CSVs again, mark a row with
#    BOTH new_tags AND delete=x. Re-run update_images.py — should abort
#    with CONFLICT_TAG_AND_DELETE (exit 2), no Dropbox writes.
```

### Real usage

```bash
# Daily count — quick read-only stat
python count_media.py --config config.local.ini

# Build a batch (default 50 photos, untagged-first, folder-clustered)
python get_images.py --config config.local.ini

# (Optional) narrow to a specific subtree
python get_images.py --config config.local.ini --root /Photos/2019

# Open the HTML, tag and/or mark for deletion, click Export.
# Then apply:
python update_images.py --config config.local.ini --csv output/tag-batch-images-<ts>.edited.csv

# Videos: same pattern
python get_videos.py --config config.local.ini
python update_videos.py --config config.local.ini --csv output/tag-batch-videos-<ts>.edited.csv

# Repeat. Each get_* run produces a fresh batch of the next 50 untagged files.
# When all of /Photos/2019 is tagged, the HTML will say "No untagged photos in scope"
# and the file will be empty.
```

### Output files

- `output/tag-batch-images-YYYY-MM-DD-HHMM.html` — review page (gitignored)
- `output/tag-batch-images-YYYY-MM-DD-HHMM.edited.csv` — your saved input (gitignored)
- `output/tag-archive.json` — persistent archive of every tag this tool has applied (gitignored)
- `logs/tag-log-YYYY-MM-DD-HHMM.csv` — per-row audit
- `logs/error-YYYY-MM-DD-HHMM.log` — written on pre-flight failure

### Recovering deleted files

Same as the duplicates flow: `update_*.py` calls `files_delete_v2`, which moves files to "Deleted files" (recycle bin). Restore from the web UI; retention depends on your Dropbox plan.

### Configuration

The `[media]` section of `config.ini` ships with safe defaults. **Copy it into your `config.local.ini`** the first time you upgrade — the install/commit doesn't touch `config.local.ini` because that file is gitignored and holds your personal tuning. A quick one-liner:

```bash
# From dbx-cleanup/
sed -n '/^\[media\]/,$p' config.ini >> config.local.ini
# Then edit config.local.ini to set your personal [media].ignored_folders
```

Tunables:

- `photo_extensions` / `video_extensions` — comma-separated lowercase, no dots.
- `batch_size` — files per HTML page. Default 50; ~1.5 MB at default thumb width.
- `thumbnail_width` — one of 32, 64, 128, 256, 480, 640, 960, 1024, 2048. Default 480.
- `tag_archive_path` — where the persistent JSON archive lives.
- `ignored_folders` — separate from `[scan].ignored_folders`. Folders you skip during a *tag* pass are usually different from folders you skip during a *duplicate* pass.
```

- [ ] **Step 2: Update the root `README.md`**

Add a bullet under the "toolkit" intro, and a short subsection link. Insert after the current `dbx-cleanup/` description bullet:

```markdown
- **Tag photos and videos** with native Dropbox tags for searchability. Build a batch with `get_images.py` / `get_videos.py`, review thumbnails in a self-contained HTML page, apply with `update_images.py` / `update_videos.py`. A local JSON archive at `dbx-cleanup/output/tag-archive.json` is a portable export — if you ever leave Dropbox, your tagging work goes with you. See [Tagging photos and videos](./dbx-cleanup/README.md#tagging-photos-and-videos).
```

Also add `count_media.py`, `get_images.py`, `get_videos.py`, `update_images.py`, `update_videos.py`, `get_media.py`, `update_media.py`, `dbx_media.py`, `seed_test_media.py` to the "Repository layout" code block in the root README.

- [ ] **Step 3: Commit**

```bash
git add dbx-cleanup/README.md README.md
git commit -m "docs: add tagging-photos-and-videos sections to READMEs"
```

---

### Task 16: Manual end-to-end smoke test against real Dropbox

**Files:** none (this task is a checklist for the engineer to execute against their real Dropbox account; uses the `/test-media/` sandbox).

- [ ] **Step 1: Ensure `.env` has a valid `DROPBOX_ACCESS_TOKEN`**

Refer to `dbx-cleanup/README.md` "One-time setup" if needed.

- [ ] **Step 2: Seed the test root**

```bash
cd dbx-cleanup
PYTHONPATH=. python seed_test_media.py
```

Expected output: `Cleared existing /test-media`, six uploads, "Pre-tagging /eventA/photo3.png", and final "Seed complete" instructions.

- [ ] **Step 3: Run count_media**

```bash
PYTHONPATH=. python count_media.py --config config.test.ini --root /test-media
```

Expected: `Photos: 4` and `Videos: 1`.

- [ ] **Step 4: Build a photo batch**

```bash
PYTHONPATH=. python get_images.py --config config.test.ini --root /test-media
```

Expected: `output/tag-batch-images-<ts>.html` written, containing 3 photos (photo1, photo2, photo4 — photo3 excluded as pre-tagged).

- [ ] **Step 5: Open the HTML in a browser**

```bash
open output/tag-batch-images-<ts>.html
```

Manually verify:
- Two folder sections (eventA, eventB)
- Three thumbnails render
- Existing tags shown as "(none)"

Tag two rows (e.g., photo1 → `seema,family`; photo2 → `family`). Mark photo4 with the delete checkbox (leave its new-tags blank). Click "Export edited CSV"; save to `dbx-cleanup/output/` as `tag-batch-images-<ts>.edited.csv`.

- [ ] **Step 6: Apply the edits**

```bash
PYTHONPATH=. python update_images.py --config config.test.ini \
    --csv output/tag-batch-images-<ts>.edited.csv
```

Confirm with `yes` at the prompt.

Expected: `Tagged: 2 (3 tags added), Deleted: 1, Skipped: 0, Errors: 0`. Audit log and archive paths printed.

- [ ] **Step 7: Verify on Dropbox web UI**

- Open `/test-media/eventA/photo1.png` in the Dropbox web UI: should show tags `seema` and `family`.
- Search `tag:seema` in the Dropbox search bar: photo1.png should appear.
- Check "Deleted files": photo4.png from `/test-media/eventB/` should be there.

- [ ] **Step 8: Verify local artifacts**

```bash
cat output/tag-archive-test.json
```

Expected: 2 entries (photo1, photo2), each with content_hash, tags, last_updated. No entry for photo4 (deleted with no prior tag history).

```bash
cat logs/tag-log-<ts>.csv
```

Expected: header + 3 rows (2 tagged, 1 deleted).

- [ ] **Step 9: Run the validation-drill case**

Edit `output/tag-batch-images-<ts>.edited.csv` by hand: pick any row, set both `new_tags` and `delete` to `x`. Re-run:

```bash
PYTHONPATH=. python update_images.py --config config.test.ini \
    --csv output/tag-batch-images-<ts>.edited.csv
```

Expected: exit code 2, `[CONFLICT_TAG_AND_DELETE]` printed, error log written to `logs/error-<ts>.log`, no Dropbox writes.

- [ ] **Step 10: Repeat for videos (abbreviated)**

```bash
PYTHONPATH=. python get_videos.py --config config.test.ini --root /test-media
# Open HTML, tag the one video, Export.
PYTHONPATH=. python update_videos.py --config config.test.ini \
    --csv output/tag-batch-videos-<ts>.edited.csv
```

Verify `video1.mp4` has the tag on Dropbox and that `tag-archive-test.json` now has 3 entries (2 photos + 1 video).

- [ ] **Step 11: Clean up**

```bash
# Delete the test sandbox folder in Dropbox web UI, or:
PYTHONPATH=. python -c "from dbx_client import get_client, load_token; \
    c = get_client(load_token()); c.files_delete_v2('/test-media')"
rm output/tag-archive-test.json output/tag-batch-*.html output/tag-batch-*.edited.csv
rm logs/tag-log-*.csv logs/error-*.log
```

- [ ] **Step 12: Final unit test run**

```bash
PYTHONPATH=. pytest -v
```

Expected: every test (existing + new) passes.

Done. The tagging tool is ready for real-account use against `config.local.ini`.

---

## Self-Review (post-plan)

**Spec coverage:**
- ✅ count_media.py → Task 6
- ✅ get_images.py / get_videos.py → Tasks 7-9
- ✅ update_images.py / update_videos.py → Tasks 10-13
- ✅ Self-contained HTML with base64 thumbs → Task 8
- ✅ Native Dropbox tags via files_tags_add → Task 4
- ✅ JSON archive keyed by path → Task 5
- ✅ Untagged-first folder-clustered selection → Tasks 3, 7, 9
- ✅ Configurable batch_size, thumbnail_width, extension lists → Tasks 1, 2
- ✅ Independent `[media].ignored_folders` → Tasks 1, 2
- ✅ Six pre-flight validations → Tasks 11, 12
- ✅ Tag normalization (`#`-stripping, lowercase, spaces→hyphens) → Task 3
- ✅ Audit log per row → Task 13
- ✅ Archive merge rules (tagged union, deleted preserves tags, no-prior-entry no-op) → Task 5
- ✅ Unit tests for each pure helper → Tasks 2-5, 7-12
- ✅ Integration test → Task 16
- ✅ Documentation updates → Task 15
