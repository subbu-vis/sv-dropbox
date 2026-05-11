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


from unittest.mock import MagicMock
from pathlib import Path

from dropbox.files import FileMetadata


def _fake_file_meta(name: str, path: str, content_hash: str) -> MagicMock:
    """Build a MagicMock(spec=FileMetadata) so isinstance() returns True
    in the production code under test."""
    m = MagicMock(spec=FileMetadata)
    m.name = name
    m.path_display = path
    m.content_hash = content_hash
    m.size = 1000
    return m


def _path_to_tags_mock(path: str, tag_texts: list[str]) -> MagicMock:
    """Tag is a stone union: .is_user_generated_tag() / .get_user_generated_tag()."""
    def _fake(text: str) -> MagicMock:
        inner = MagicMock(); inner.tag_text = text
        t = MagicMock()
        t.is_user_generated_tag.return_value = True
        t.get_user_generated_tag.return_value = inner
        return t
    pt = MagicMock()
    pt.path = path
    pt.tags = [_fake(t) for t in tag_texts]
    return pt


def test_run_end_to_end_with_mocks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Full orchestration: walk → tag-lookup → filter → fold → pack → thumb → html."""
    from get_media import run

    # Config file in tmp_path so load_media_config works.
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\nmin_file_size_bytes=1\nskip_shared_not_owned=true\nskip_hidden=true\n"
        "early_exit_row_threshold=1\nmax_csv_rows=1\nignored_folders=\n\n"
        "[paths]\n"
        f"csv_output_dir={tmp_path}/output\n"
        f"log_dir={tmp_path}/logs\n\n"
        "[media]\nphoto_extensions=jpg\nvideo_extensions=mp4\nbatch_size=5\n"
        "thumbnail_width=480\n"
        f"tag_archive_path={tmp_path}/archive.json\n"
        "ignored_folders=\n"
    )

    client = MagicMock()
    # files_list_folder result: three jpgs and one non-FileMetadata entry.
    page = MagicMock()
    page.entries = [
        _fake_file_meta("a.jpg", "/x/a.jpg", "h1"),
        _fake_file_meta("b.jpg", "/x/b.jpg", "h2"),
        _fake_file_meta("c.jpg", "/y/c.jpg", "h3"),
        MagicMock(spec=object),  # isinstance() FileMetadata fails -> skipped
    ]
    page.has_more = False
    client.files_list_folder.return_value = page

    # files_tags_get: a.jpg untagged, b.jpg already tagged, c.jpg untagged.
    client.files_tags_get.return_value = MagicMock(paths_to_tags=[
        _path_to_tags_mock("/x/a.jpg", []),
        _path_to_tags_mock("/x/b.jpg", ["existing"]),
        _path_to_tags_mock("/y/c.jpg", []),
    ])

    # files_get_thumbnail_v2: return fake bytes for any path.
    thumb_response = MagicMock()
    thumb_response.content = b"\xff\xd8FAKE"
    client.files_get_thumbnail_v2.return_value = (MagicMock(), thumb_response)

    # Mock auth + client builder at the get_media import points.
    monkeypatch.setattr("get_media.get_client", lambda token: client)
    monkeypatch.setattr("get_media.load_token", lambda: "fake-token")

    # CLI args.
    monkeypatch.setattr("sys.argv", ["get_images.py", "--config", str(cfg_path), "--root", "/"])

    rc = run(kind="image")
    assert rc == 0

    # An HTML file should be produced under csv_output_dir.
    html_files = list((tmp_path / "output").glob("tag-batch-*.html"))
    assert len(html_files) == 1
    html_text = html_files[0].read_text()
    # b.jpg was already tagged -> excluded; a.jpg and c.jpg included.
    assert "/x/a.jpg" in html_text
    assert "/x/b.jpg" not in html_text
    assert "/y/c.jpg" in html_text
