# Photo & video tagging for Dropbox

**Status:** design approved, awaiting implementation plan
**Date:** 2026-05-10
**Author:** Subbu (with Claude)

## Problem

A decade-deep personal Dropbox holds tens of thousands of photos and videos. Finding "the Diwali 2019 photos with Seema in them" today requires drilling through folders by hand — Dropbox search only matches filenames and file content, not people or events. Native Dropbox tags solve discoverability (searchable from the web UI as `tag:diwali-2019`) but applying them through the Dropbox UI one file at a time is impractical at this scale.

Goal: a CLI workflow that surfaces untagged media in folder-clustered batches, lets the user review thumbnails and assign comma-separated tags in a single self-contained HTML page, and writes those tags to Dropbox (with a portable local archive for future migration).

## Non-goals

- Face recognition / auto-suggested person tags (deferred — see "Future work")
- Editing existing tags on Dropbox (current scope: add tags to untagged files)
- Search/query UI on top of the archive (the archive is a portable export, not a query interface)
- Bulk operations on already-tagged files (re-tag passes are out of scope; see "Future work")

## Solution overview

Five new scripts plus shared helpers, mirroring the existing `find_duplicates`/`delete_duplicates`/`dbx_folder_sizes` patterns in `dbx-cleanup/`.

```
count_media.py         single script, prints  "Photos: N  Videos: M"

get_images.py    ─┐
get_videos.py    ─┴── thin wrappers around get_media.py (engine)

update_images.py ─┐
update_videos.py ─┴── thin wrappers around update_media.py (engine)
```

The image and video pipelines are deliberately mirrored — same engine, different extension list and config keys. Separate entry-point scripts (rather than a `--kind` flag) keep the operation un-confusable on the command line.

### Pipeline data flow (images; videos identical)

```
get_images.py
  └─ walk Dropbox (filtered by photo extensions, --root, [media].ignored_folders)
  └─ batch existing-tag lookups via files_tags_get_batch (100 paths/call)
  └─ keep only untagged files
  └─ folder-cluster (group by parent path, sort clusters by file count desc)
  └─ pack into batch: take whole clusters while ≤ batch_size; if next would
     overflow but budget remains, take a partial slice (so the page still fills)
  └─ fetch thumbnails at configured width via files_get_thumbnail_v2
  └─ emit output/tag-batch-<ts>.html  (self-contained, base64-embedded thumbs)

[user opens HTML in browser, edits new_tags + delete checkboxes, clicks "Export"]
       └─ browser downloads tag-batch-<ts>.edited.csv

update_images.py --csv tag-batch-<ts>.edited.csv
  └─ pre-flight validation (6 checks, all-or-nothing)
  └─ prompt for literal "yes"
  └─ for each row: apply native tags (deduped vs existing), or delete file
  └─ merge results into output/tag-archive.json
  └─ write logs/tag-log-<ts>.csv
```

## Key decisions

| Decision | Choice | Reasoning |
|---|---|---|
| Review/edit UX | Self-contained HTML, single file with base64 thumbs and an Export button that downloads a CSV | Best UX for "I need to see the photo to tag it". No server, no spreadsheet image-embedding gymnastics. |
| Tag storage on Dropbox | Native Dropbox tags via `files_tags_add` | Searchable in the Dropbox web UI (`tag:foo`). File-properties metadata would not be UI-searchable. |
| Local archive | Single JSON file at `output/tag-archive.json`, keyed by Dropbox path | Portable export for life-after-Dropbox. JSON cleanly handles one-to-many (file → tags) and is consumable by any future system (Immich, PhotoPrism, Google Photos via their import tools). |
| Selection rule | Untagged-first, folder-clustered | Skips files already tagged on Dropbox. Folder clustering maximizes the "apply to all in this folder" shortcut. Empty batch = "done" signal. |
| Photo/video classification | File extension lists in config | Predictable. Dropbox's `media_info` field is unreliable for older/unusual files. |
| Default batch size | 50 | ~1.5 MB HTML at default thumb width. 200+ produces 20+ MB pages. User can raise in `config.local.ini`. |
| Default thumbnail width | 480 px | Dropbox's `files_get_thumbnail_v2` supports exactly {32, 64, 128, 256, 480, 640, 960, 1024, 2048}. 480 is the smallest size you can actually tag from. Configurable. |
| Face recognition | Deferred | Heavy deps (dlib/face_recognition ~500 MB) and a known-faces setup overhead. Design as a clean hook for later. |
| Filename style | `count_media.py`, `get_images.py`, `update_images.py`, `get_videos.py`, `update_videos.py` (snake_case) | Matches the existing `find_duplicates.py` / `delete_duplicates.py` / `dbx_folder_sizes.py` convention. |
| `ignored_folders` scope | New `[media].ignored_folders`, independent of `[scan].ignored_folders` | The folders you want excluded from a tag-review pass are different from the ones you want excluded from a duplicate scan. |

## File-by-file design

### `dbx_client.py` — extend with `MediaConfig` and `load_media_config`

New frozen dataclass parallel to the existing `Config`:

```python
@dataclass(frozen=True)
class MediaConfig:
    photo_extensions: frozenset[str]   # lowercase, no dots: {"jpg", "jpeg", ...}
    video_extensions: frozenset[str]
    batch_size: int                    # > 0
    thumbnail_width: int               # ∈ {32,64,128,256,480,640,960,1024,2048}
    tag_archive_path: Path
    ignored_folders: tuple[str, ...]   # same normalized form as Config.ignored_folders
    csv_output_dir: Path               # reused from [paths]
    log_dir: Path                      # reused from [paths]
```

`load_media_config(path)` reads `[media]` and `[paths]` from the same INI file. Validation at load time (before any Dropbox call):

- `thumbnail_width` ∈ allowed set, else `ValueError("thumbnail_width must be one of: 32, 64, ...")`
- `batch_size > 0`
- Extension lists non-empty, lowercase, no leading dots
- `tag_archive_path` parent directory is writable

### `dbx_media.py` — new shared helper module

Pure functions, no Dropbox client state. Easy to unit-test.

```python
def classify_media(path: str, photo_exts: frozenset[str],
                   video_exts: frozenset[str]) -> Literal["photo", "video", "other"]: ...

def normalize_tag(raw: str) -> str:
    """Strip leading '#', lowercase, spaces→hyphens, validate.
    Raises ValueError(f"invalid tag: {raw!r}") if result doesn't match Dropbox's rules:
    a-z0-9 and hyphens only, 1-32 chars."""

def fold_to_folders(paths: list[str]) -> list[tuple[str, list[str]]]:
    """Group paths by parent folder. Returns list of (folder, [paths_in_folder]),
    sorted by len(paths) desc so the biggest clusters come first."""
```

Client-using helpers (separate file or same — see writing-plans):

```python
def fetch_existing_tags(client, paths: list[str]) -> dict[str, list[str]]:
    """Calls files_tags_get_batch in chunks of 100. Returns {path: [tag, ...]}."""

def fetch_thumbnail(client, path: str, width: int) -> bytes:
    """Calls files_get_thumbnail_v2. Returns JPEG bytes."""

def apply_tags(client, path: str, tags_to_add: list[str]) -> list[str]:
    """Calls files_tags_add per tag. Returns list of tags actually added
    (may be smaller than input if some already existed and the API said so)."""
```

### Tag archive (`output/tag-archive.json`)

Single forever-append JSON, keyed by Dropbox path. Example:

```json
{
  "/Photos/2019/Diwali/IMG_4421.jpg": {
    "content_hash": "abc123...",
    "tags": ["diwali-2019", "seema", "performance"],
    "last_updated": "2026-05-10T14:32:11"
  },
  "/Photos/2019/Diwali/IMG_4422.jpg": {
    "content_hash": "def456...",
    "tags": ["diwali-2019"],
    "last_updated": "2026-05-10T14:32:11",
    "deleted_at": "2026-05-10T14:35:02"
  }
}
```

**Merge rules:**
- Tagged path: `archive[path] = {content_hash, tags: sorted(set(old_tags) ∪ set(new_tags)), last_updated: now}`. Stored in normalized form (matches what's on Dropbox).
- Deleted path **with an existing archive entry** (i.e., previously tagged via this tool): preserve `tags`, add `deleted_at: now`.
- Deleted path **with no existing archive entry**: skip — no entry is created. The archive is a tag repository, not an audit of every delete (that's what `logs/tag-log-<ts>.csv` is for).
- Errored row: do not touch the archive.

### `count_media.py` — terminal-only count

Walks the whole account via `files_list_folder(recursive=True)`. For each `FileMetadata`, classifies by extension (`[media].ignored_folders` and hidden segments are honored). Prints:

```
Photos: 12,347
Videos: 482
```

No CSV, no flags except `--config`.

### `get_media.py` (engine) and the two thin wrappers

The engine takes a `kind: Literal["image", "video"]` parameter and the loaded `MediaConfig`. The wrappers are 5-line files:

```python
# get_images.py
from get_media import run
sys.exit(run(kind="image"))
```

Engine responsibilities:
1. Argument parsing: `--config` (default `config.ini`), `--root` (default `/`)
2. Walk Dropbox under `--root`, applying ignored_folders and extension filter
3. Tag-batch lookup for all candidates (chunks of 100)
4. Drop already-tagged paths
5. Folder-cluster (`fold_to_folders`)
6. Take first `batch_size` files in cluster order
7. Fetch thumbnails (one API call per file, sequential — Dropbox rate-limits parallel thumbnail fetches aggressively)
8. Render HTML using a template (see HTML schema below)
9. Write `output/tag-batch-<ts>.html`, print path
10. Empty result → write a header-only HTML that says "No untagged photos in scope" and exit 0

### HTML structure

Single self-contained file. No external CDNs, no fonts, no images-from-disk.

```html
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Tag batch — 50 photos from 2026-05-10 14:32</title>
  <style>/* ~40 lines, plain CSS, no framework */</style>
</head>
<body>
  <header>
    <h1>Tag batch — 50 photos from 2026-05-10 14:32</h1>
    <button id="export">Export edited CSV</button>
  </header>

  <section class="folder" data-folder="/Photos/2019/Diwali">
    <h2>/Photos/2019/Diwali <small>(28 files)</small></h2>
    <div class="bulk">
      Apply to all in folder:
      <input class="bulk-tags" placeholder="diwali-2019, family">
      <button class="bulk-apply">Apply</button>
    </div>
    <div class="row" data-path="/Photos/2019/Diwali/IMG_4421.jpg"
                     data-hash="abc123..."
                     data-existing="">
      <img src="data:image/jpeg;base64,...">
      <div class="meta">
        <code class="filename">IMG_4421.jpg</code>
        <div>existing: <span class="existing">(none)</span></div>
        <label>new tags:
          <input class="new-tags" placeholder="seema, performance">
        </label>
        <label><input type="checkbox" class="del"> mark for deletion</label>
      </div>
    </div>
    <!-- ...more rows... -->
  </section>
  <!-- ...more folder sections... -->

  <script>
    // ~50 lines, vanilla JS, no dependencies.
    // - "Apply" button: copies bulk-tags value into every new-tags input in this folder.
    // - "Export" button: walks each .row, builds a CSV string with columns
    //   path, content_hash, filename, existing_tags, new_tags, delete,
    //   triggers a download as tag-batch-<ts>.edited.csv.
  </script>
</body>
</html>
```

The exported CSV's column order is fixed and matches what `update_media.py` expects. Blank `new_tags` AND blank `delete` rows are still included (they become no-ops on update).

### `update_media.py` (engine) and the two thin wrappers

Mirror of `delete_duplicates.py` shape. Argument parsing: `--config`, `--csv` (required).

**Pre-flight validation** — 6 checks, all run to completion so the error log lists every problem at once:

| Code | What it checks | Why |
|---|---|---|
| `PATH_NOT_FOUND` | Each row's path still exists in Dropbox (`files_get_metadata`) | File was moved/renamed/deleted between scan and update |
| `HASH_CHANGED` | Each row's `content_hash` matches Dropbox | File was edited; refuse to tag/delete content you may not recognize |
| `CONFLICT_TAG_AND_DELETE` | No row has both `new_tags` non-empty AND `delete=x` | User-explicit safety rule |
| `INVALID_TAG` | Each parsed tag passes `normalize_tag` rules | Fail in pre-flight rather than mid-batch |
| `TOO_MANY_TAGS` | `existing_tags ∪ new_tags` ≤ 20 per file | Dropbox's hard cap |
| `EXCEEDS_MAX_ROWS` | Total rows ≤ `[media].batch_size` | Catches hand-edited CSVs that ballooned past intended batch |

`PATH_NOT_FOUND` and `HASH_CHANGED` both rely on `files_get_metadata` per marked row. Combine into one call per row to halve API traffic.

On any failure: `logs/error-<ts>.log` written, exit code `2`, no Dropbox writes.

**On all-pass:** prompt for literal `yes`, then per row:

- `delete=x` → `files_delete_v2(path)` (moves to recycle bin)
- `new_tags` non-empty → for each tag NOT already in `existing_tags`, call `files_tags_add(path, tag)`
- Both empty → no-op, logged as `skipped`

**Error handling** mirrors `delete_duplicates.py`:
- `AuthError` re-raised immediately
- Per-file `ApiError` logged and the batch continues

**Audit log** (`logs/tag-log-<ts>.csv`):

```
timestamp, path, action, tags_added, tags_skipped_already_present, dropbox_response
2026-05-10T14:35:01, /Photos/2019/Diwali/IMG_4421.jpg, tagged, seema|performance, diwali-2019, ok
2026-05-10T14:35:02, /Photos/2019/Diwali/IMG_4422.jpg, deleted, , , moved to recycle bin
```

Tags are pipe-separated within their column to keep CSV parseable.

**Archive merge** happens after the API loop, only for rows that succeeded:
- Tagged path → union tags, update `last_updated`
- Deleted path with existing entry → preserve tags, add `deleted_at`
- Deleted path with no entry → skip (no entry created)
- Errored rows → archive untouched

**Final summary**:

```
Done. Tagged: 38 (97 tags added), Deleted: 4, Skipped: 0, Errors: 0
Audit log: logs/tag-log-2026-05-10-1435.csv
Archive:   output/tag-archive.json (1,247 paths total)
```

## Config changes

New `[media]` section in `config.ini` (shipped with defaults) and `config.local.ini` (user-customized):

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

`config.test.ini` gets a `[media]` block with `batch_size = 10` and otherwise default values.

`.gitignore` already covers `output/` and `logs/`, so `tag-archive.json`, the HTML batches, and the audit logs are not committed.

## Test plan

### Unit tests (no Dropbox calls)

Run with `PYTHONPATH=. pytest -v` from `dbx-cleanup/`.

**`test_dbx_media.py`** — pure helpers
- `normalize_tag`: `"#Diwali 2019"` → `"diwali-2019"`, `"  SeEMa  "` → `"seema"`
- `normalize_tag` rejection: `"subject!"`, `"a" * 33`, empty string → raises with offending tag named
- `classify_media` by extension, case-insensitive: `/a/b.JPG` → photo, `/a/b.MoV` → video, `/a/b.txt` → other
- `fold_to_folders`: clusters and sorts by cluster size desc

**`test_get_media.py`** — engine logic
- `filter_untagged`: drops entries with non-empty tag lists
- `select_batch` packing: takes whole folder clusters in size-desc order while cumulative count ≤ `batch_size`; if the next cluster would overflow but budget is not exhausted, take a partial slice of that cluster (preserves "mostly-clustered" property while filling the page)
- `select_batch` huge-folder case: a single folder with 200 files and `batch_size=50` returns 50 files from that folder
- `build_html` happy path: produces a string containing each entry's filename, `data:image/jpeg;base64,` prefix, and per-folder "Apply to all" controls
- Empty input → header-only HTML with "No untagged media in scope"

**`test_update_media.py`** — engine logic
- CSV parsing: extracts all six columns, tolerates blank separator rows
- `validate_conflict_tag_and_delete`: row with both populated → raises with row index
- `validate_tag_count`: existing 18 + new 5 = 23 → `TOO_MANY_TAGS`
- `validate_invalid_tag`: any tag still failing normalization rules → `INVALID_TAG`
- `merge_archive` tag path: existing `{tags: [a, b]}` + new `[b, c]` → `{tags: [a, b, c]}`
- `merge_archive` delete with prior entry: preserves tags, adds `deleted_at`
- `merge_archive` delete with no prior entry: archive unchanged (no entry created)

**`test_dbx_client.py`** — extend existing
- `load_media_config` happy path
- `load_media_config` rejects `thumbnail_width = 333`, `batch_size = 0`
- `[media].ignored_folders` and `[scan].ignored_folders` parsed independently

### Integration test (against real Dropbox sandbox)

New `seed_test_media.py` populates `/test-media/`:

```
/test-media/
├── eventA/photo1.jpg         (tiny real JPEG, untagged)
├── eventA/photo2.jpg         (tiny real JPEG, untagged)
├── eventA/photo3.jpg         (tiny real JPEG, tagged "already-tagged" via API after upload)
├── eventB/photo4.jpg         (tiny real JPEG, untagged)
├── eventB/video1.mp4         (small real MP4 fixture, untagged)
└── other/doc.pdf             (should be ignored — not in media extensions)
```

JPEG and MP4 bytes bundled as constants in `seed_test_media.py` (no Pillow dep needed).

**Test steps** (added to README):

```bash
# 1. Seed
python seed_test_media.py

# 2. Count — expect "Photos: 4, Videos: 1" (PDF excluded)
python count_media.py --config config.test.ini --root /test-media

# 3. Get images
python get_images.py --config config.test.ini --root /test-media
#    → output/tag-batch-<ts>.html with 3 untagged photos
#    (photo3 excluded because it has a tag)

# 4. Open the HTML in your browser. Expected:
#    - 2 folder sections (eventA, eventB)
#    - "Apply to all in folder" controls per section
#    - 3 thumbnails (~480 px wide)
#    - existing_tags shown as "(none)" for each
#    Add tags to TWO of the three rows; mark the THIRD row 'x' for deletion
#    (leave its new_tags blank — a single row can't be both tagged AND deleted).
#    Click Export.

# 5. Run update against the exported CSV
python update_images.py --config config.test.ini \
    --csv output/tag-batch-<ts>.edited.csv

# 6. Verify in the Dropbox web UI:
#    - 2 photos have the new tags (visible next to filename, searchable)
#    - The 'x'-marked file is in Deleted Files
#    - photo3.jpg is unchanged (already-tagged, was excluded from the batch)

# 7. Verify locally:
#    - output/tag-archive.json has 2 entries (the two tagged photos).
#      The deleted file had no prior archive entry, so per the merge rules
#      it does NOT get added on delete — the audit log is the record of that.
#    - logs/tag-log-<ts>.csv shows 2 tagged + 1 deleted + 0 errors

# 8. Repeat for videos:
python get_videos.py --config config.test.ini --root /test-media
python update_videos.py --config config.test.ini --csv ...

# 9. Validation drill: open an exported CSV, mark a row with BOTH
#    new_tags AND delete=x. Re-run update_images.py — should abort with
#    CONFLICT_TAG_AND_DELETE and write logs/error-<ts>.log.
#    No Dropbox writes.
```

## Documentation updates

- **`dbx-cleanup/README.md`** — add a "Tagging photos and videos" section after the duplicates section. Structure parallels the duplicates docs: setup, test path ("Test before unleashing for tags"), real usage, output files, recovery for deletes.
- **`/README.md`** (root) — add a bullet under the toolkit list: "Tag photos and videos with native Dropbox tags for searchability, with a self-contained HTML review page and a portable JSON archive." Link to the new section.

## Open questions

None. All design decisions resolved during brainstorm; ready for implementation plan.

## Future work

- **Face recognition pre-fill.** A separate `suggest_faces.py` script that, given a known-faces directory (one folder per person), pre-fills the `new_tags` field in newly-generated HTML batches. Heavy deps (dlib/face_recognition) live in an optional `requirements-faces.txt`. Hook point: a `pre_fill_tags(path, thumbnail_bytes) -> list[str]` function called inside the HTML generation step. v1 always returns `[]`.
- **Re-tag passes.** A `--include-tagged` flag on `get_images.py` to include already-tagged files in batches (with their existing tags shown, ready to be added to or replaced). Would require `update_images.py` to also support tag *removal*.
- **Archive query CLI.** A `query_archive.py` that searches `tag-archive.json` (e.g. `query_archive.py --tag seema --year 2019`). Today the archive is exported-for-future-systems; this would also make it useful in-place.
- **Archive-as-cache optimization.** Skip the `files_tags_get_batch` API call for paths already present in the archive (trust the archive). Adds a "rebuild archive" path for cases where Dropbox tags were edited outside this tool.
