from dataclasses import dataclass

import pytest

from find_duplicates import FileEntry, should_skip_file


@dataclass(frozen=True)
class FakeMeta:
    """Subset of dropbox.files.FileMetadata fields we care about."""
    name: str
    path_display: str
    size: int
    content_hash: str
    server_modified: str
    sharing_info: object | None = None


@pytest.mark.parametrize(
    "size, path, expected",
    [
        (50_000, "/Photos/img.jpg", True),     # below 100 KB threshold
        (200_000, "/Photos/img.jpg", False),   # above threshold
        (200_000, "/Photos/.hidden", True),    # hidden file
        (200_000, "/.dropbox.cache/x", True),  # hidden folder segment
        (0, "/Photos/empty.txt", True),        # empty file
    ],
)
def test_should_skip_file_size_and_hidden(size: int, path: str, expected: bool) -> None:
    meta = FakeMeta(name=path.rsplit("/", 1)[-1], path_display=path, size=size,
                    content_hash="abc", server_modified="2024-01-01T00:00:00Z")
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=True,
                            skip_shared_not_owned=True, owner_account_id="self") is expected


@dataclass(frozen=True)
class FakeShareInfo:
    modified_by: str | None


def test_should_skip_shared_not_owned() -> None:
    meta = FakeMeta(
        name="x.pdf", path_display="/Shared/x.pdf", size=200_000,
        content_hash="h", server_modified="2024-01-01T00:00:00Z",
        sharing_info=FakeShareInfo(modified_by="other-account"),
    )
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=True,
                            skip_shared_not_owned=True, owner_account_id="self") is True


def test_should_keep_shared_owned_by_self() -> None:
    meta = FakeMeta(
        name="x.pdf", path_display="/Shared/x.pdf", size=200_000,
        content_hash="h", server_modified="2024-01-01T00:00:00Z",
        sharing_info=FakeShareInfo(modified_by="self"),
    )
    assert should_skip_file(meta, min_file_size_bytes=100_000, skip_hidden=True,
                            skip_shared_not_owned=True, owner_account_id="self") is False
