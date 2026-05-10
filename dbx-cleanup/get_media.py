"""Engine for get_images.py and get_videos.py.

Walks Dropbox, looks up existing tags, filters to untagged, folder-clusters,
packs into a batch sized by config, downloads thumbnails, and writes a
self-contained HTML review page.
"""

from __future__ import annotations

from typing import Iterable


def filter_untagged(
    candidates: list[str],
    existing_tags: dict[str, list[str]],
) -> list[str]:
    """Return paths whose tag list is empty (or absent from the dict).
    Order is preserved from `candidates`."""
    return [p for p in candidates if not existing_tags.get(p)]


def select_batch(
    folded: list[tuple[str, list[str]]],
    batch_size: int,
) -> list[str]:
    """Pack folder clusters into a batch.

    Walk clusters in order (caller has already sorted by cluster size desc).
    For each cluster:
      - if the whole cluster fits in remaining budget, take it whole;
      - else if any budget remains, take a partial slice of it and stop;
      - else stop.

    This preserves "biggest clusters first" while filling the page when the
    last cluster doesn't quite fit whole."""
    if batch_size <= 0:
        return []
    out: list[str] = []
    remaining = batch_size
    for _folder, paths in folded:
        if remaining <= 0:
            break
        if len(paths) <= remaining:
            out.extend(paths)
            remaining -= len(paths)
        else:
            out.extend(paths[:remaining])
            remaining = 0
            break
    return out
