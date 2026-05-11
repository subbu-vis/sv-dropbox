# dbx-cleanup

Phase 1 scripts for finding and deleting duplicate files in Dropbox, plus a read-only folder-size report.

Three scripts:

- `find_duplicates.py` — scans Dropbox, identifies byte-identical files in different locations, writes a CSV ranked by wasted space (≤100 rows).
- `delete_duplicates.py` — ingests the user-edited CSV and moves flagged files to Dropbox's recycle bin, with strict pre-flight validations.
- `dbx_folder_sizes.py` — read-only audit. Walks every file and writes a CSV listing every folder by total size (descending). Useful for "where's my space going?".

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

### 3. Create your personal config

`config.ini` ships with safe defaults and generic example folders. **Don't edit it directly** — instead, copy it to `config.local.ini` and put your real settings there. `config.local.ini` is gitignored, so your folder names and tuning never get pushed.

```bash
cp config.ini config.local.ini
# then edit config.local.ini with your real ignored_folders and any tuning
```

***VERY IMPORTANT***

From here on, **always pass `--config config.local.ini`** to every command. If you forget the flag, the scripts fall back to the generic `config.ini` and will scan folders you wanted to skip.

Things you might tune in `config.local.ini`:

- `min_file_size_bytes` — files smaller than this are ignored (default 100 KB).
- `ignored_folders` — list of folders to skip during the scan, one per line. Match is **case-insensitive path prefix**: an entry skips the folder itself and everything inside it recursively, but does NOT affect siblings or parents.

  ```ini
  ignored_folders =
      /Old Backups
      /Photos/2019/raw
  ```

  In this example, `/Photos/2019/raw` is ignored (and its descendants), but `/Photos/2019/edited`, `/Photos/2019`, and `/Photos/2020/raw` are all still scanned. To add a folder later, edit the file and re-run; no other changes needed.

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

Always pass `--config config.local.ini` so your personal settings (ignored folders, thresholds) take effect.

```bash
# 1. Find candidate duplicates across your whole Dropbox
python find_duplicates.py --config config.local.ini

# 2. Open the CSV under output/ in your spreadsheet (Excel, Numbers, Google Sheets…).
#    Mark 'x' in the `delete` column for files you want removed.
#    Save the CSV.

# 3. Move marked files to Dropbox's recycle bin
python delete_duplicates.py --config config.local.ini --csv output/duplicates-<timestamp>.csv

# 4. Folder-size audit (read-only, optional)
python dbx_folder_sizes.py --config config.local.ini

# 5. Repeat. Each run handles up to 100 candidates.
```

> **Note:** if you omit `--config config.local.ini`, the scripts fall back to the tracked `config.ini`, which has only generic example values for `ignored_folders`. Folders you intended to skip will be scanned anyway. Not destructive — `find_duplicates.py` is read-only — but slower and noisier.

When the delete script finishes, it prints a final line like:

```
Done. Deleted: 42, Errors: 0, Space freed: 318 MB
Audit log: logs/delete-log-2026-04-29-2105.csv
```

`Space freed` is the sum of byte sizes of every successfully-deleted file, rounded up to the nearest MB. Useful for tracking how much you've reclaimed across runs.

## How it works

### `find_duplicates.py` — scan, filter, rank

**What counts as a duplicate.** Two files are duplicates if and only if they share Dropbox's `content_hash` (a deterministic SHA-256 of 4 MB blocks). Filenames don't have to match — `report.pdf` in `/Work` and `renamed.pdf` in `/Archive` with identical bytes will group together. Conversely, two files named the same with different content do NOT group.

**Skip rules** (applied in this order; first match wins):

1. **Empty files** (`size == 0`) — always skipped.
2. **Below threshold** — files smaller than `min_file_size_bytes` (configurable, default 100 KB).
3. **Hidden** — files/folders with any path segment starting with `.` (e.g. `/.dropbox.cache/...`), when `skip_hidden=true`.
4. **Ignored folders** — files under any entry in `ignored_folders` (case-insensitive path-prefix match; affects only the listed subtree, not siblings or the parent).
5. **Shared, not owned by you** — files in shared folders where the last modifier is someone else, when `skip_shared_not_owned=true`. This is a heuristic based on `sharing_info.modified_by`; if a collaborator touched a file you own, it may be excluded; if you touched their file, it may be included.
6. **Incomplete or untyped** — files where `content_hash` is `None` (still uploading) or `server_modified` is `None` (e.g., Dropbox Paper docs surfaced as files).

**Selection logic.** After grouping by `content_hash` and dropping singletons, each group's wasted bytes is `(count − 1) × file_size`. Groups are sorted by wasted bytes desc (tie-breaker: more copies first). Then greedy: take whole groups in ranked order while the cumulative row count stays ≤ `max_csv_rows` (default 100). Groups are never split — you always see all copies of a file together when deciding what to delete.

Greedy is intentionally not optimal: if the top group has 30 rows and the next two groups have 35 + 35 rows, you'd see only the first one (the 70-row pair would fit better but greedy already committed). The summary tells you how many groups were deferred to the next run, so you know to rerun.

**Early exit.** Scanning the whole Dropbox can take a while. Once the running tally hits `early_exit_row_threshold` duplicate rows (default 1,000), the scan stops — you already have plenty to work with. Groups beyond that point will surface in subsequent runs once you've cleaned up.

### `delete_duplicates.py` — validate, confirm, execute

The script does **all** validation before any Dropbox writes. If anything fails, no file is touched.

**Four pre-flight validations** (all run to completion so you see every problem at once):

| Code | What it checks | Why |
|---|---|---|
| `PATH_NOT_FOUND` | Each marked path still exists in Dropbox (`files_get_metadata`). Only `path/not_found` errors are bucketed here; permission/malformed-path errors propagate as a real error. | The file may have been moved or deleted since the scan. |
| `GROUP_FULLY_MARKED` | For each `group_id`, at least one row is NOT marked `x`. | Safety net: prevents deleting all copies of a file even if you mark them all by accident. |
| `EXCEEDS_MAX_ROWS` | Total `x` count ≤ `max_csv_rows` (default 100). | Daily-rate-limit safety; matches the cap `find_duplicates.py` writes into the CSV. |
| `HASH_CHANGED` | Each marked row's current `content_hash` in Dropbox matches what's in the CSV. | Catches the case where you edited a file between scan and delete; refuses to delete content you may have changed. |

If any validation fails: an error log is written to `logs/error-YYYY-MM-DD-HHMM.log` listing every offending row and the reason. Exit code `2`. No deletes performed.

If all pass: you're prompted for a literal `yes`. Anything else aborts (exit `1`).

**Execution.** For each marked row, the script calls `files_delete_v2(path)`, which moves the file to Dropbox's "Deleted files" area (recoverable for 30+ days). Behavior:

- **Per-file errors** are logged to the audit CSV and the script continues with the rest. One bad file doesn't block the batch.
- **`AuthError` mid-batch** (expired token, etc.) is re-raised immediately rather than being logged as a per-row error — otherwise an expired token at row 50 would produce 50 fake "errors" instead of one clear "regenerate your token" message.
- **Audit log** (`logs/delete-log-YYYY-MM-DD-HHMM.csv`) records timestamp, path, size, hash, status (`deleted` or `error`), and the Dropbox response confirming each move to the recycle bin.
- **Final summary** prints `Deleted N, Errors M, Space freed K MB` where `K` is the sum of byte sizes of every successfully-deleted row, rounded up to MB.

### `dbx_folder_sizes.py` — read-only folder-size audit

Strictly read-only — no Dropbox API call ever modifies state. The script walks every file in the account via `files_list_folder(recursive=True)` and attributes each file's size to its named ancestor folders, **capped at 3 levels deep** (the `MAX_FOLDER_DEPTH` constant). A 5 MB file at `/Photos/2019/raw/jan/img.cr2` rolls up to `/Photos`, `/Photos/2019`, and `/Photos/2019/raw` — but `/Photos/2019/raw/jan` does not appear in the output. This `du`-style rollup means top-level totals stay correct (the file is still counted in `/Photos/2019/raw`'s sum), while keeping the report scannable.

**No filtering.** Unlike `find_duplicates.py`, this script ignores no folders or file types. Hidden folders, shared-not-owned files, `ignored_folders` entries, and tiny files are all counted — the goal is a complete picture of your data.

**Tree-ordered output.** Rows are arranged so each parent folder is followed immediately by its subfolders, recursively. At every level, siblings are sorted by size descending. Example:

```
folder,size_mb,file_count
/Photos,200,50         ← biggest top-level
/Photos/2020,120,30    ←   biggest /Photos child
/Photos/2020/raw,100,25  ←     biggest /Photos/2020 child
/Photos/2020/edited,20,5
/Photos/2019,80,20     ←   smaller /Photos child
/Music,100,30          ← second top-level
/Music/Albums,90,25
/Music/Albums/Stones,50,12
/Music/Albums/Beatles,30,10
/Music/Singles,10,5
```

**Output file.** `output/dbx-file-size-YYYY-MM-DD-HHMM.csv` with columns `folder, size_mb, file_count`. Sizes are integer MB rounded up — anything ≥ 1 byte shows at least 1 MB.

**Usage:**
```bash
python dbx_folder_sizes.py --config config.local.ini
# --config defaults to config.ini if omitted
```

The only config setting it reads is `[paths].csv_output_dir`. The scan itself isn't tunable — it walks everything to depth 3.

## Output files

- `output/duplicates-YYYY-MM-DD-HHMM.csv` — candidate duplicates, columns: `group_id, filename, size_bytes, path, content_hash, last_modified, delete`. Rows are grouped, with a blank row between groups.
- `output/dbx-file-size-YYYY-MM-DD-HHMM.csv` — folder-size audit (read-only), columns: `folder, size_mb, file_count`. Sorted descending by size.
- `logs/delete-log-YYYY-MM-DD-HHMM.csv` — audit log of every delete attempt: timestamp, path, status (`deleted` or `error`), and the Dropbox response confirming the file is in the recycle bin.
- `logs/error-YYYY-MM-DD-HHMM.log` — written when pre-flight validation fails. Lists every offending row and the reason. No deletions occur.

## Recovering deleted files

`delete_duplicates.py` calls `files_delete_v2`, which moves files to Dropbox's "Deleted files" area (recycle bin). Restore via:

1. Web UI → **Deleted files** in the left sidebar
2. Find the file → **Restore**

Retention: 30 days on free / Plus, 180 days on Professional / Business.

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
| `INVALID_TAG` | A tag fails Dropbox's rules (a-z, 0-9, underscores, 1-32 chars) after normalization |
| `TOO_MANY_TAGS` | Existing + new tags would exceed Dropbox's 20-per-file cap |
| `EXCEEDS_MAX_ROWS` | The CSV has more rows than `[media].batch_size` |

All validations run to completion; if any fails the whole batch aborts (exit 2). On success, the user types `yes`, then each row is either tagged (deduped against existing) or moved to the recycle bin. Errors on individual rows continue the batch; `AuthError` aborts immediately.

After the run, three artifacts:
- `output/tag-archive.json` — the persistent local archive, keyed by Dropbox path. Survives a future migration away from Dropbox.
- `logs/tag-log-YYYY-MM-DD-HHMM.csv` — per-row audit (timestamp, path, action, tags added/skipped, response).
- The Dropbox files themselves now carry their new tags, searchable in the web UI as `tag:diwali_2019`.

### Tag input format

Tags in the HTML are comma-separated. The script normalizes each one before sending to Dropbox:
- Leading `#` is stripped (so `#seema` and `seema` both work)
- Lowercased
- Internal whitespace AND hyphens become single underscores (so `Diwali 2019` and `diwali-2019` both → `diwali_2019`). Dropbox's tag validator rejects hyphens — only `a-z`, `0-9`, and `_` are permitted.
- Validated: a-z, 0-9, underscores only; 1-32 chars

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

## Tests

Unit tests run without hitting Dropbox:

```bash
source .venv/bin/activate
PYTHONPATH=. pytest -v
```

The full integration test against a real Dropbox account is the **Test before unleashing** section above.
