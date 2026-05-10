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
