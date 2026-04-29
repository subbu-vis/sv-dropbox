"""Move user-flagged duplicate files in Dropbox to the recycle bin."""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import dropbox
from dropbox.exceptions import ApiError, AuthError, DropboxException

from dbx_client import Config, MissingTokenError, get_client, load_config, load_token, with_retry


@dataclass(frozen=True)
class CsvRow:
    group_id: int
    filename: str
    size_bytes: int
    path: str
    content_hash: str
    last_modified: str
    marked_delete: bool


REQUIRED_COLUMNS = {"group_id", "filename", "size_bytes", "path",
                    "content_hash", "last_modified"}


def parse_csv(csv_path: Path) -> list[CsvRow]:
    """Parse a duplicates CSV (header + rows + blank separators between groups).

    Raises ValueError with row context for missing columns or non-int values
    in `group_id`/`size_bytes`. `delete` column is optional; absent or
    whitespace-only values mean "do not delete"."""
    rows: list[CsvRow] = []
    # utf-8-sig transparently strips a BOM if Excel-on-Windows added one.
    with csv_path.open(encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - fieldnames
        if missing:
            raise ValueError(f"{csv_path}: CSV is missing required columns: "
                             f"{sorted(missing)}")
        # DictReader yields header at line 1, first data row at line 2.
        for lineno, raw in enumerate(reader, start=2):
            if not raw.get("group_id"):
                continue  # blank separator row
            try:
                rows.append(CsvRow(
                    group_id=int(raw["group_id"]),
                    filename=raw["filename"],
                    size_bytes=int(raw["size_bytes"]),
                    path=raw["path"],
                    content_hash=raw["content_hash"],
                    last_modified=raw["last_modified"],
                    marked_delete=raw.get("delete", "").strip().lower() == "x",
                ))
            except (ValueError, TypeError) as exc:
                raise ValueError(f"{csv_path} line {lineno}: {exc}") from exc
    return rows


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
            meta = with_retry(lambda r=row: client.files_get_metadata(r.path))
        except ApiError as exc:
            # Only treat path/not_found as "missing"; let other API errors
            # (permission, malformed path, etc.) propagate so the user gets
            # a real traceback rather than a misleading PATH_NOT_FOUND.
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
                resp = with_retry(lambda r=row: client.files_delete_v2(r.path))
                # files_delete_v2 returns a DeleteResult with .metadata of the deleted entry,
                # confirming Dropbox accepted and moved the file to deleted-files (recycle bin).
                deleted_path = getattr(resp.metadata, "path_display", row.path)
                writer.writerow([ts, row.path, row.size_bytes, row.content_hash,
                                 "deleted", f"moved to recycle bin: {deleted_path}"])
                success += 1
                print(f"  deleted: {row.path}")
            except AuthError:
                # Token expired/invalid mid-batch. Don't log 100 "errors" — fail
                # fast so main() can surface a clear message and the user can
                # regenerate their token before retrying.
                raise
            except DropboxException as exc:
                # ApiError, RateLimitError (after retries exhausted), etc.
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

    try:
        config = load_config(Path(args.config))
        token = load_token()
        client = get_client(token)
    except FileNotFoundError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 1
    except MissingTokenError as exc:
        print(f"Token error: {exc}", file=sys.stderr)
        return 1
    except AuthError as exc:
        print(f"Dropbox auth failed: {exc}. See README for token regeneration.",
              file=sys.stderr)
        return 1

    try:
        rows = parse_csv(Path(args.csv))
    except (FileNotFoundError, ValueError) as exc:
        print(f"CSV error: {exc}", file=sys.stderr)
        return 1

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
    try:
        summary = execute_deletes(client, rows_to_delete, audit_path)
    except AuthError as exc:
        print(f"\nDropbox auth failed mid-batch: {exc}. Audit log written to "
              f"{audit_path} for any deletes that succeeded before the failure. "
              "Regenerate your token and re-run with the same CSV.",
              file=sys.stderr)
        return 1

    print(f"\nDone. Deleted: {summary.success_count}, Errors: {summary.error_count}")
    print(f"Audit log: {summary.log_path}")
    return 0 if summary.error_count == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
