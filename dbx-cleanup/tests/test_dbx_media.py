from __future__ import annotations

import pytest

from dbx_media import classify_media, normalize_tag, fold_to_folders


def test_classify_media_photo_lowercase() -> None:
    assert classify_media("/a/b.jpg", frozenset({"jpg", "png"}), frozenset({"mp4"})) == "photo"


def test_classify_media_photo_uppercase() -> None:
    assert classify_media("/a/B.JPG", frozenset({"jpg"}), frozenset({"mp4"})) == "photo"


def test_classify_media_video() -> None:
    assert classify_media("/a/b.MoV", frozenset({"jpg"}), frozenset({"mov"})) == "video"


def test_classify_media_other() -> None:
    assert classify_media("/a/b.txt", frozenset({"jpg"}), frozenset({"mp4"})) == "other"


def test_classify_media_no_extension() -> None:
    assert classify_media("/a/README", frozenset({"jpg"}), frozenset({"mp4"})) == "other"


def test_classify_media_dotfile_no_extension() -> None:
    """`.gitignore` has no extension, just a hidden name."""
    assert classify_media("/a/.gitignore", frozenset({"jpg"}), frozenset({"mp4"})) == "other"


def test_normalize_tag_strips_hash_and_lowercases() -> None:
    assert normalize_tag("#Diwali") == "diwali"


def test_normalize_tag_spaces_to_hyphens() -> None:
    assert normalize_tag("Diwali 2019") == "diwali-2019"


def test_normalize_tag_strips_whitespace() -> None:
    assert normalize_tag("  seema  ") == "seema"


def test_normalize_tag_collapses_multiple_spaces() -> None:
    assert normalize_tag("a   b   c") == "a-b-c"


def test_normalize_tag_keeps_existing_hyphens() -> None:
    assert normalize_tag("diwali-2019") == "diwali-2019"


def test_normalize_tag_rejects_invalid_chars() -> None:
    with pytest.raises(ValueError, match="invalid tag"):
        normalize_tag("subject!")


def test_normalize_tag_rejects_too_long() -> None:
    with pytest.raises(ValueError, match="invalid tag"):
        normalize_tag("a" * 33)


def test_normalize_tag_accepts_32_chars() -> None:
    """32 is the max length per Dropbox rules — exactly 32 is fine."""
    assert normalize_tag("a" * 32) == "a" * 32


def test_normalize_tag_rejects_empty() -> None:
    with pytest.raises(ValueError, match="invalid tag"):
        normalize_tag("")


def test_normalize_tag_rejects_whitespace_only() -> None:
    with pytest.raises(ValueError, match="invalid tag"):
        normalize_tag("   ")


def test_fold_to_folders_basic() -> None:
    paths = ["/x/a.jpg", "/x/b.jpg", "/y/c.jpg"]
    result = fold_to_folders(paths)
    assert result == [("/x", ["/x/a.jpg", "/x/b.jpg"]),
                      ("/y", ["/y/c.jpg"])]


def test_fold_to_folders_sorted_by_cluster_size_desc() -> None:
    """Bigger clusters come first."""
    paths = ["/small/a.jpg", "/big/1.jpg", "/big/2.jpg", "/big/3.jpg"]
    result = fold_to_folders(paths)
    folders = [f for f, _ in result]
    assert folders == ["/big", "/small"]


def test_fold_to_folders_preserves_input_order_within_cluster() -> None:
    paths = ["/x/z.jpg", "/x/a.jpg", "/x/m.jpg"]
    result = fold_to_folders(paths)
    assert result == [("/x", ["/x/z.jpg", "/x/a.jpg", "/x/m.jpg"])]


def test_fold_to_folders_empty() -> None:
    assert fold_to_folders([]) == []


def test_fold_to_folders_root_level() -> None:
    """File at root has parent ''."""
    assert fold_to_folders(["/a.jpg"]) == [("", ["/a.jpg"])]


# --- Tests for section 2: Dropbox helpers -------

from unittest.mock import MagicMock

from dbx_media import fetch_existing_tags, fetch_thumbnail, apply_tags


def _path_to_tags(path: str, tag_texts: list[str]) -> MagicMock:
    """Build a fake PathToTags result. tags is a list of objects with .tag_text."""
    pt = MagicMock()
    pt.path = path
    pt.tags = [MagicMock(tag_text=t) for t in tag_texts]
    return pt


def test_fetch_existing_tags_single_batch() -> None:
    """≤100 paths: one API call, results merged into dict."""
    client = MagicMock()
    client.files_tags_get_batch.return_value = MagicMock(paths_to_tags=[
        _path_to_tags("/a.jpg", []),
        _path_to_tags("/b.jpg", ["existing"]),
    ])
    result = fetch_existing_tags(client, ["/a.jpg", "/b.jpg"])
    assert result == {"/a.jpg": [], "/b.jpg": ["existing"]}
    assert client.files_tags_get_batch.call_count == 1
    client.files_tags_get_batch.assert_called_with(["/a.jpg", "/b.jpg"])


def test_fetch_existing_tags_chunks_above_100() -> None:
    """>100 paths: split into chunks of 100."""
    paths = [f"/p{i}.jpg" for i in range(250)]
    client = MagicMock()
    client.files_tags_get_batch.side_effect = [
        MagicMock(paths_to_tags=[_path_to_tags(p, []) for p in paths[0:100]]),
        MagicMock(paths_to_tags=[_path_to_tags(p, []) for p in paths[100:200]]),
        MagicMock(paths_to_tags=[_path_to_tags(p, []) for p in paths[200:250]]),
    ]
    result = fetch_existing_tags(client, paths)
    assert len(result) == 250
    assert client.files_tags_get_batch.call_count == 3
    # Verify each chunk size
    call_lengths = [len(call.args[0]) for call in client.files_tags_get_batch.call_args_list]
    assert call_lengths == [100, 100, 50]


def test_fetch_existing_tags_empty_input() -> None:
    """Zero paths: no API call, empty dict."""
    client = MagicMock()
    result = fetch_existing_tags(client, [])
    assert result == {}
    assert client.files_tags_get_batch.call_count == 0


def test_fetch_thumbnail_returns_bytes() -> None:
    client = MagicMock()
    response = MagicMock()
    response.content = b"\xff\xd8\xff\xe0fake-jpeg-bytes"
    client.files_get_thumbnail_v2.return_value = (MagicMock(), response)
    result = fetch_thumbnail(client, "/photo.jpg", 480)
    assert result == b"\xff\xd8\xff\xe0fake-jpeg-bytes"


def test_fetch_thumbnail_rejects_unsupported_width() -> None:
    client = MagicMock()
    with pytest.raises(ValueError, match="thumbnail width 333 not supported"):
        fetch_thumbnail(client, "/photo.jpg", 333)


def test_apply_tags_adds_each() -> None:
    client = MagicMock()
    apply_tags(client, "/photo.jpg", ["a", "b", "c"])
    assert client.files_tags_add.call_count == 3
    client.files_tags_add.assert_any_call("/photo.jpg", "a")
    client.files_tags_add.assert_any_call("/photo.jpg", "b")
    client.files_tags_add.assert_any_call("/photo.jpg", "c")


def test_apply_tags_empty_list_is_noop() -> None:
    client = MagicMock()
    apply_tags(client, "/photo.jpg", [])
    assert client.files_tags_add.call_count == 0
