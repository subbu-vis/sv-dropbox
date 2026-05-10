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
