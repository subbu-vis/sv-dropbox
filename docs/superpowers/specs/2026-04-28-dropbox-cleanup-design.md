# Dropbox Cleanup — Phase 1: Duplicate Removal

**Date:** 2026-04-28
**Status:** Design approved, awaiting implementation plan
**Owner:** Subbu

## Goal

Build two Python scripts that help safely remove duplicate files from a personal Dropbox account:

1. `find_duplicates.py` — scan Dropbox, identify byte-identical duplicate files in different locations, output a CSV ranked by wasted space.
2. `delete_duplicates.py` — ingest the user-edited CSV and move flagged files to Dropbox's recycle bin, with safety checks.

The user reviews the CSV between runs and marks an `x` next to files to delete. Each run is capped at 100 candidate rows to bound risk.

## Out of scope (future phases)

- Cleanup of files other than duplicates (large files, old files, etc.)
- Automated suggestions for which copy to keep
- Web UI / non-CLI interface
- Full OAuth refresh-token flow (Phase 1 uses a long-lived access token)

## Project layout

New folder: `<repo>/dbx-cleanup/`

```
dbx-cleanup/
├── README.md                  # setup + usage instructions
├── requirements.txt           # python deps
├── .env.example               # template, committed
├── .env                       # actual token, gitignored
├── config.ini                 # default tunable settings, committed
├── config.test.ini            # test-mode settings, committed
├── find_duplicates.py         # script 1
├── delete_duplicates.py       # script 2
├── seed_test_data.py          # uploads known test files to /test-duplicates/
├── dbx_client.py              # shared: auth, SDK init, retry helpers
├── output/                    # CSV outputs (gitignored)
└── logs/                      # audit + error logs (gitignored)
```

## Configuration

`config.ini` (committed):

```ini
[scan]
min_file_size_bytes = 102400        ; 100 KB
skip_shared_not_owned = true
skip_hidden = true
early_exit_row_threshold = 1000
max_csv_rows = 100

[paths]
csv_output_dir = ./output
log_dir = ./logs
```

`config.test.ini` (committed): same shape, but `min_file_size_bytes = 1024`, `early_exit_row_threshold = 50`.

`.env` (gitignored):

```
DROPBOX_ACCESS_TOKEN=sl.XXXXXXXXX
```

## Authentication

Single long-lived Dropbox access token generated via the Dropbox App Console. Stored in `.env`. Required scopes: `files.metadata.read`, `files.content.read`, `files.content.write`.

If the token is missing or invalid at runtime, scripts print clear instructions pointing to README and exit non-zero.

## CLI surface

Both scripts accept:

- `--config <path>` — defaults to `config.ini`
- `--root <dropbox-path>` (find only) — defaults to `/`
- `--csv <path>` (delete only) — required, points to user-edited CSV

## CSV format

Columns: `group_id, filename, size_bytes, path, content_hash, last_modified, delete`

- Rows sorted by `group_id` ascending, then `path` ascending.
- One blank row between groups for visual separation in spreadsheets.
- `delete` column is empty initially; user fills in `x` for files to delete.
- `content_hash` is Dropbox's SHA-256 of 4 MB blocks (deterministic, server-computed).

## `find_duplicates.py` — flow

1. Load config + `.env`. Validate token; on missing/invalid, print setup instructions and exit non-zero.
2. Print `Connecting to Dropbox as <account email>...` (sanity via `users_get_current_account`).
3. Walk Dropbox from `--root` using `files_list_folder(recursive=True)`, paging.
4. For each file entry:
   - Skip if `size < min_file_size_bytes`.
   - Skip if any path segment starts with `.` (when `skip_hidden=true`).
   - Skip if the file lives in a shared folder not owned by the user (when `skip_shared_not_owned=true`).
   - Skip empty files (size 0).
   - Otherwise add `(content_hash, path, name, size, server_modified)` to an in-memory dict keyed by `content_hash`.
5. Print progress every 1,000 files: `Scanned 5,000 files, found 23 duplicate groups (47 rows)...`
6. After each progress batch, count total duplicate rows. If `>= early_exit_row_threshold`, stop scanning early.
7. After scan ends:
   - Drop singletons (groups with only 1 file).
   - Compute wasted space per group: `(count - 1) * size`.
   - Sort groups by wasted space descending.
   - Greedily take groups in order until adding the next group's rows would exceed `max_csv_rows`. Never split a group.
   - Assign `group_id` 1..N in output order.
8. Write `output/duplicates-YYYY-MM-DD-HHMM.csv` with the columns above and blank rows between groups.
9. Print summary: groups, rows, total wasted bytes, output path, hint to mark `x` and run delete script.

## `delete_duplicates.py` — flow

1. Load config + `.env`. Validate token.
2. Read CSV from `--csv` arg.
3. **Pre-flight scan (no deletions):**
   - Parse all rows, group by `group_id`.
   - **Validation A** — every row marked `x` references a path that still exists in Dropbox (call `files_get_metadata` per `x` row).
   - **Validation B** — for each group, `x_count < total_rows` (cannot delete all copies).
   - **Validation C** — total `x` count `<= max_csv_rows`.
   - **Validation D** — for each `x` row, current `content_hash` from `files_get_metadata` matches the CSV's `content_hash` (catches edits between scan and delete).
   - All validations run to completion before any abort decision (so the error log lists every problem, not just the first one). Validations A and D share the same `files_get_metadata` call per row.
   - If any validation found a problem: write `logs/error-YYYY-MM-DD-HHMM.log` listing every offending row and reason, print summary to stdout, exit non-zero. **Nothing has been deleted.**
4. **Confirmation prompt:** `About to move N files to Dropbox recycle bin. Type 'yes' to proceed: ` — must be the literal string `yes`.
5. **Execute:** for each `x` row, call `files_delete_v2(path)`. Continue on per-file errors (do not abort the whole run).
6. Write `logs/delete-log-YYYY-MM-DD-HHMM.csv` with columns `timestamp, path, size_bytes, content_hash, status, dropbox_response`. `status` is `deleted` or `error`. `dropbox_response` records the API response confirming the file went to the recycle bin (the API returns metadata of the deleted entry).
7. Print summary: deleted count, error count, log file path.

## Error handling & rate limits

- Wrap all Dropbox API calls in a retry helper (`dbx_client.py`):
  - On `RateLimitError`: sleep the SDK-provided `backoff` seconds, retry up to 3 times.
  - On network errors: exponential backoff 1s, 2s, 4s; up to 3 retries.
  - On auth errors (401, invalid token): print pointer to README, exit non-zero immediately.
- Top-level exception handler in each script writes traceback to `logs/error-*.log` and exits non-zero.
- During delete execution, per-file errors are logged and the script continues; final summary shows successes vs. failures.

## Test plan

Manual smoke test against a real Dropbox `/test-duplicates/` folder.

`seed_test_data.py` uploads:

- **Group A:** 3 copies of a 2 KB file at `/test-duplicates/A/file.txt`, `/test-duplicates/B/file.txt`, `/test-duplicates/C/file.txt`
- **Group B:** 2 copies of a 5 KB file at `/test-duplicates/B/photo.jpg`, `/test-duplicates/D/photo.jpg`
- **Group C:** 4 copies of a 1.5 KB file at `/test-duplicates/E/doc.pdf`, `/test-duplicates/F/doc.pdf`, `/test-duplicates/G/doc.pdf`, `/test-duplicates/H/doc.pdf`
- **Noise:** 2 unique non-duplicate files at varying sizes
- **Below threshold:** 1 file under 1 KB to verify the size filter

Test sequence (in README):

1. `python seed_test_data.py`
2. `python find_duplicates.py --config config.test.ini --root /test-duplicates/`
3. Verify CSV contains exactly 3 groups, 9 total rows, sorted by wasted space descending. Wasted space per group is `(count - 1) * size`:
   - Group B: 2 copies × 5 KB → 5 KB wasted
   - Group C: 4 copies × 1.5 KB → 4.5 KB wasted
   - Group A: 3 copies × 2 KB → 4 KB wasted

   Expected order in CSV: Group B, Group C, Group A.
4. Mark a row in Group B with `x`, save.
5. `python delete_duplicates.py --config config.test.ini --csv output/<file>.csv`
6. Verify in Dropbox web UI → Deleted files that the file is in the recycle bin.
7. Mark ALL rows of Group A with `x`, re-run delete. Confirm script aborts with Validation B error and writes an error log naming Group A.

## README.md

Covers:

1. Setup (Python venv, `pip install`, Dropbox app creation walk-through, token generation, `.env` setup)
2. Configuration (when to adjust `min_file_size_bytes` etc.)
3. Test sequence (above)
4. Real usage (find → review CSV → delete → repeat)
5. File output reference (output/, logs/)
6. Recovering deleted files (Dropbox web UI → Deleted files; 30 days free / longer paid)

## Open questions

None at design close. Implementation plan to follow.
