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
