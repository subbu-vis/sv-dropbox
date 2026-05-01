# sv-dropbox

A small toolkit for cleaning up a personal Dropbox account at scale. Today the repo contains one tool:

- **[`dbx-cleanup/`](./dbx-cleanup/)** — Python scripts that find byte-identical duplicate files anywhere in your Dropbox, let you review them in a CSV, and then delete the ones you mark. Also includes a read-only folder-size audit.

If you've got a Dropbox that has grown into the hundreds of GB over a decade and you want to claw back space without trusting a black-box "clean my drive" button, this is for you.

## Why not just use Dropbox's built-in deduplication?

Dropbox's web UI has a "Find duplicates" feature (and various third-party "Dropbox cleaner" apps wrap the same idea). They work, but they're limited in ways that matter on a real account:

| | Dropbox native UI / typical 3rd-party cleaners | `dbx-cleanup` |
|---|---|---|
| **Detection** | Often filename + size based, sometimes scoped to one folder at a time. | True content-hash match using Dropbox's own `content_hash` (SHA-256 of 4 MB blocks). Renames, moves, and re-uploads all collapse into one group. |
| **Scope** | One folder, one view, one click at a time. | Whole-account scan in a single pass. |
| **Review surface** | A modal in the browser. Hard to sort, filter, or share with a spreadsheet-savvy partner. | A plain CSV. Open in Excel / Numbers / Google Sheets, sort by wasted space, mark `x` on the rows you want gone, save. |
| **Safety rails** | "Are you sure?" dialogs. | Pre-flight validations that abort the entire batch if anything looks off — missing path, all copies of a file marked, hash changed since scan, batch too large. No partial-delete surprises. |
| **Auditability** | None — once it's deleted, the only record is Dropbox's own "Deleted files" view. | Every delete is logged to a CSV (`logs/delete-log-*.csv`) with timestamp, path, size, hash, and the API response. Keep it forever. |
| **Repeatability** | Click-driven; no record of what rules you applied. | Config-driven (`config.ini`). Re-run with the same settings, share the config, version-control it. |
| **Skipping** | All-or-nothing on a folder. | Skip lists by path prefix, hidden-file rules, shared-not-owned filter, minimum file size — all configurable. |
| **Cost** | Free / bundled. | Free, runs locally, no third-party service ever sees your file list. |

The headline reason most people end up here: **content-hash matching**. If you have `vacation.mov` in `/Photos/2018/` and the same file re-uploaded as `IMG_4421.mov` in `/Camera Uploads/2018-08/`, native tools that key on filename will miss it. `dbx-cleanup` will see them as one duplicate group and let you pick which to keep.

## ⚠️ This tool DELETES files. Read this section.

`delete_duplicates.py` is not a simulation. When you mark `x` in the CSV and run the script, the named files are removed from your Dropbox. There is no dry-run flag — the review *is* the dry run.

The tool is designed to be hard to misuse, but you can still do real damage. Concretely:

- **You decide what gets deleted.** The script never picks for you. Whatever you mark `x`, it deletes — including, in principle, files that aren't actually duplicates if you've edited the CSV by hand.
- **The script enforces a "keep at least one copy" rule per duplicate group.** If you accidentally mark every row in a group, pre-flight validation aborts the whole batch with a `GROUP_FULLY_MARKED` error. Nothing is touched. But — if you put a file in the CSV as a singleton (or edit groups), this safety net depends on the `group_id` column being intact.
- **It will not delete files whose content has changed since the scan.** Each row's `content_hash` is re-checked against Dropbox right before the batch runs (`HASH_CHANGED` validation). If you opened, edited, and saved a file between scan and delete, that row is rejected and the whole batch aborts.
- **It will not delete files that no longer exist at the recorded path.** If you moved a file in the meantime, that row is rejected (`PATH_NOT_FOUND`) and the batch aborts.
- **It caps batches at 100 files by default.** Configurable, but exists to limit blast radius and stay well under Dropbox's daily delete-rate limits.
- **You will be prompted for a literal `yes` before any delete happens.** Anything else aborts.

None of these are substitutes for **looking at the CSV**. The duplicate detector is exact (content-hash equality is mathematically reliable), but exact duplicates can still be intentional — e.g., a file you keep in two places on purpose. Skim the CSV before marking.

### Recommended first run

The repo includes a full end-to-end test path that uses a sandbox folder in your real Dropbox. Do this before pointing the scripts at your actual files. See the **Test before unleashing** section in [`dbx-cleanup/README.md`](./dbx-cleanup/README.md#test-before-unleashing).

## Recovering deleted files

`delete_duplicates.py` calls Dropbox's `files_delete_v2` API, which **does not hard-delete**. It moves files to your "Deleted files" area — Dropbox's recycle bin. To recover:

1. Open Dropbox in a browser.
2. Click **Deleted files** in the left sidebar.
3. Find the file (it'll be at its original path) and click **Restore**.

You can also restore an entire folder at once, or use Dropbox's **Rewind** feature to roll your whole account back to a point in time.

### How long do you have?

Retention depends on your Dropbox plan. Always verify against [Dropbox's current docs](https://help.dropbox.com/delete-restore/recover-deleted-files) — these numbers can change:

| Plan | Deleted-file retention |
|---|---|
| Basic (free) | 30 days |
| Plus / Family | 30 days |
| Professional | 180 days |
| Standard / Advanced / Business | 180 days |
| Enterprise | configurable, often 365 days |

After the retention window, files are permanently purged and cannot be recovered through normal means.

**Practical advice:** the audit log under `dbx-cleanup/logs/delete-log-*.csv` lists every path the script removed. Keep it. If you realize a week later that you deleted something you wanted, that log is your shopping list — open the Deleted Files view in Dropbox and restore by path.

### What the tool does NOT do

- It does **not** call any "permanent delete" API. There's no way to bypass the recycle bin from this code.
- It does **not** modify file contents. Every operation is "move to Deleted files."
- It does **not** touch shared files you don't own (when `skip_shared_not_owned=true`, which is the default).
- It does **not** silently retry on auth errors. If your token expires mid-batch, the script stops immediately with a clear message — no half-finished delete sprees.

## Setup and usage

All operational documentation — how to install, generate a Dropbox token, configure the scan, run the find/delete cycle, and run tests — lives in [`dbx-cleanup/README.md`](./dbx-cleanup/README.md).

> **Recommended pattern for everyone:** copy `dbx-cleanup/config.ini` to `dbx-cleanup/config.local.ini`, put your real `ignored_folders` and tuning there (it's gitignored), and pass `--config config.local.ini` to every script invocation. The tracked `config.ini` is a generic public template — scanning your real Dropbox with it works, but it won't honor any personal skip rules. Full details in [`dbx-cleanup/README.md`](./dbx-cleanup/README.md#3-create-your-personal-config).

## Repository layout

```
sv-dropbox/
├── README.md                 ← this file
├── .gitignore
└── dbx-cleanup/              ← the tool
    ├── README.md             ← detailed setup + usage
    ├── find_duplicates.py
    ├── delete_duplicates.py
    ├── dbx_folder_sizes.py
    ├── dbx_client.py
    ├── seed_test_data.py
    ├── config.ini
    ├── config.test.ini
    ├── .env.example
    ├── requirements.txt
    └── tests/
```

## Security & privacy

- Your Dropbox access token lives in `dbx-cleanup/.env` and is **never** committed (`.env` is gitignored).
- The scripts run entirely on your machine. Nothing is sent anywhere except to `api.dropboxapi.com`.
- The token is a personal access token tied to a Dropbox app you create yourself — you can revoke it any time from the [Dropbox App Console](https://www.dropbox.com/developers/apps).
- Output CSVs (which contain your file paths) are written to `dbx-cleanup/output/` and `dbx-cleanup/logs/`, both of which are gitignored.

## License

MIT. See [`LICENSE`](./LICENSE) — or if not yet present, treat as "use at your own risk, no warranty, you delete your own files."
