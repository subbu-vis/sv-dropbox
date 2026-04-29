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

Open `config.ini`. Things you might tune:

- `min_file_size_bytes` — files smaller than this are ignored (default 100 KB).
- `ignored_folders` — list of folders to skip during the scan, one per line. Match is **case-insensitive path prefix**: an entry skips the folder itself and everything inside it recursively, but does NOT affect siblings or parents.

  ```ini
  ignored_folders =
      /Cetachi Comics
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
