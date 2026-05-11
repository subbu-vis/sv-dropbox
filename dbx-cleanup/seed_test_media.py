"""One-shot uploader: populate /test-media/ with known fixtures so the
tagging scripts can be exercised end-to-end against real Dropbox.

Idempotent: clears /test-media/ before populating.

The "photos" are tiny 1×1 PNGs (69 bytes each, well-known canonical encoding).
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

# 1×1 white PNG (69 bytes). Canonical/well-known minimal PNG; verifiable
# by base64-decoding and feeding to any PNG decoder.
TINY_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVQI12P4//8/AAX+Av58GHV5AAAAAElFTkSuQmCC"
)

# Minimal MP4 (30 bytes): ftyp box declaring isom brand. Dropbox accepts
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
    print("\nPre-tagging /eventA/photo3.png with 'already_tagged'...")
    with_retry(lambda: client.files_tags_add(f"{ROOT}/eventA/photo3.png", "already_tagged"))

    print("\nSeed complete. Now run:")
    print(f"  python count_media.py --config config.test.ini --root {ROOT}")
    print(f"  python get_images.py --config config.test.ini --root {ROOT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
