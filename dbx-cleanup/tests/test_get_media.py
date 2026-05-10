from __future__ import annotations

import pytest

from get_media import filter_untagged, select_batch


def test_filter_untagged_keeps_only_empty() -> None:
    candidates = ["/a.jpg", "/b.jpg", "/c.jpg"]
    tags = {"/a.jpg": [], "/b.jpg": ["existing"], "/c.jpg": []}
    assert filter_untagged(candidates, tags) == ["/a.jpg", "/c.jpg"]


def test_filter_untagged_treats_missing_as_untagged() -> None:
    """If a path didn't come back in the tags dict (shouldn't happen, but be defensive),
    treat as untagged."""
    candidates = ["/a.jpg", "/b.jpg"]
    tags = {"/a.jpg": []}
    assert filter_untagged(candidates, tags) == ["/a.jpg", "/b.jpg"]


def test_filter_untagged_empty_input() -> None:
    assert filter_untagged([], {}) == []


def test_select_batch_whole_clusters_fit() -> None:
    """Both clusters fit within budget — take both whole."""
    folded = [("/x", ["/x/a", "/x/b"]), ("/y", ["/y/c"])]
    assert select_batch(folded, batch_size=5) == ["/x/a", "/x/b", "/y/c"]


def test_select_batch_exact_fit() -> None:
    folded = [("/x", ["/x/a", "/x/b", "/x/c"])]
    assert select_batch(folded, batch_size=3) == ["/x/a", "/x/b", "/x/c"]


def test_select_batch_partial_cluster_fills_budget() -> None:
    """First cluster takes 3 of the 5-budget; second cluster has 4 files,
    only 2 fit — take partial slice."""
    folded = [("/x", ["/x/1", "/x/2", "/x/3"]),
              ("/y", ["/y/1", "/y/2", "/y/3", "/y/4"])]
    assert select_batch(folded, batch_size=5) == ["/x/1", "/x/2", "/x/3", "/y/1", "/y/2"]


def test_select_batch_huge_folder_partial_slice() -> None:
    """A single cluster bigger than batch_size — take a slice of it."""
    folded = [("/x", [f"/x/{i}" for i in range(200)])]
    result = select_batch(folded, batch_size=50)
    assert len(result) == 50
    assert result == [f"/x/{i}" for i in range(50)]


def test_select_batch_empty_input() -> None:
    assert select_batch([], batch_size=50) == []


def test_select_batch_zero_budget_returns_empty() -> None:
    folded = [("/x", ["/x/a"])]
    assert select_batch(folded, batch_size=0) == []


import base64

from get_media import build_html, BatchEntry


def test_build_html_has_title_and_export_button() -> None:
    html = build_html(entries=[], kind="image", timestamp="2026-05-10 14:32")
    assert "<title>" in html
    assert "Tag batch" in html
    assert "2026-05-10 14:32" in html
    assert 'id="export"' in html


def test_build_html_empty_entries_shows_friendly_message() -> None:
    html = build_html(entries=[], kind="image", timestamp="2026-05-10 14:32")
    assert "No untagged" in html


def test_build_html_groups_by_folder() -> None:
    entries = [
        BatchEntry(path="/x/a.jpg", filename="a.jpg", content_hash="h1",
                   existing_tags=[], thumbnail_bytes=b"\xff\xd8fake"),
        BatchEntry(path="/x/b.jpg", filename="b.jpg", content_hash="h2",
                   existing_tags=["existing"], thumbnail_bytes=b"\xff\xd8fake"),
        BatchEntry(path="/y/c.jpg", filename="c.jpg", content_hash="h3",
                   existing_tags=[], thumbnail_bytes=b"\xff\xd8fake"),
    ]
    html = build_html(entries=entries, kind="image", timestamp="2026-05-10 14:32")

    # Two folder sections
    assert html.count('<section class="folder"') == 2
    assert 'data-folder="/x"' in html
    assert 'data-folder="/y"' in html

    # Bulk-apply controls per folder
    assert html.count('class="bulk-tags"') == 2
    assert html.count('class="bulk-apply"') == 2

    # One row per entry
    assert html.count('class="row"') == 3
    assert 'data-path="/x/a.jpg"' in html
    assert 'data-hash="h1"' in html
    assert 'data-existing=""' in html
    assert 'data-existing="existing"' in html

    # Filenames shown
    assert "a.jpg" in html
    assert "b.jpg" in html
    assert "c.jpg" in html


def test_build_html_embeds_thumbnails_as_base64() -> None:
    fake_jpeg = b"\xff\xd8\xff\xe0FAKE"
    entries = [BatchEntry(
        path="/x/a.jpg", filename="a.jpg", content_hash="h", existing_tags=[],
        thumbnail_bytes=fake_jpeg,
    )]
    html = build_html(entries=entries, kind="image", timestamp="2026-05-10 14:32")
    expected_b64 = base64.b64encode(fake_jpeg).decode("ascii")
    assert f"data:image/jpeg;base64,{expected_b64}" in html


def test_build_html_includes_export_js() -> None:
    """The JS in the page should call URL.createObjectURL on a Blob; smoke check
    that the export logic is wired."""
    html = build_html(entries=[], kind="image", timestamp="2026-05-10 14:32")
    assert "createObjectURL" in html
    assert "Blob" in html


def test_build_html_escapes_html_special_chars() -> None:
    """If a path contains < or & or quotes, output must escape them."""
    entries = [BatchEntry(
        path="/x/<weird&name>.jpg", filename="<weird&name>.jpg",
        content_hash="h", existing_tags=[], thumbnail_bytes=b"x",
    )]
    html = build_html(entries=entries, kind="image", timestamp="2026-05-10 14:32")
    # The raw string should NOT appear unescaped in the HTML body.
    assert "<weird&name>" not in html or "&lt;weird&amp;name&gt;" in html
