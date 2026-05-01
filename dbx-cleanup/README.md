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

### 3. Review settings

Open `config.ini`. Things you might tune:

- `min_file_size_bytes` — files smaller than this are ignored (default 100 KB).
- `ignored_folders` — list of folders to skip during the scan, one per line. Match is **case-insensitive path prefix**: an entry skips the folder itself and everything inside it recursively, but does NOT affect siblings or parents.

  ```ini
  ignored_folders =
      /Old Backups
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
python dbx_folder_sizes.py
# Optional: --config <path> to use a different config (default: config.ini)
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

## Tests

Unit tests run without hitting Dropbox:

```bash
source .venv/bin/activate
PYTHONPATH=. pytest -v
```

The full integration test against a real Dropbox account is the **Test before unleashing** section above.
