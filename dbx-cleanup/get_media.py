"""Engine for get_images.py and get_videos.py.

Walks Dropbox, looks up existing tags, filters to untagged, folder-clusters,
packs into a batch sized by config, downloads thumbnails, and writes a
self-contained HTML review page.
"""

from __future__ import annotations

import base64
from collections import Counter
import html as html_lib
from dataclasses import dataclass
from typing import Iterable, Literal


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


@dataclass
class BatchEntry:
    path: str
    filename: str
    content_hash: str
    existing_tags: list[str]
    thumbnail_bytes: bytes


_HTML_TEMPLATE_HEAD = """\
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif;
            margin: 1.5rem; max-width: 900px; }}
    header {{ position: sticky; top: 0; background: white; padding: 0.5rem 0;
              border-bottom: 1px solid #ddd; z-index: 10; }}
    h1 {{ margin: 0 0 0.5rem; font-size: 1.2rem; }}
    button {{ font-size: 1rem; padding: 0.4rem 0.8rem; cursor: pointer; }}
    .folder {{ margin: 2rem 0; }}
    .folder h2 {{ font-size: 1rem; color: #333; }}
    .bulk {{ margin: 0.5rem 0 1rem; padding: 0.5rem; background: #f5f5f5;
             border-radius: 4px; }}
    .bulk input {{ width: 60%; padding: 0.3rem; }}
    .row {{ display: flex; gap: 1rem; margin: 1rem 0; padding: 0.5rem;
             border: 1px solid #eee; border-radius: 4px; }}
    .row img {{ max-width: 480px; height: auto; }}
    .meta {{ flex: 1; }}
    .meta code {{ font-weight: bold; }}
    .meta label {{ display: block; margin: 0.4rem 0; }}
    .meta input[type=text] {{ width: 100%; padding: 0.3rem; }}
    .existing {{ color: #666; }}
    .empty {{ color: #888; font-style: italic; padding: 2rem; text-align: center; }}
    .tag-cloud {{ position: fixed; top: 5rem; right: 1rem; width: 180px;
                  max-height: calc(100vh - 7rem); overflow-y: auto;
                  padding: 0.6rem 0.8rem; background: #fafafa;
                  border: 1px solid #ddd; border-radius: 4px; font-size: 0.85rem; }}
    .tag-cloud h3 {{ font-size: 0.9rem; margin: 0 0 0.5rem; color: #444; }}
    .tag-cloud ul {{ list-style: none; padding: 0; margin: 0; }}
    .tag-cloud li {{ padding: 0.15rem 0; display: flex;
                     justify-content: space-between; gap: 0.5rem; }}
    .tag-cloud .count {{ color: #888; }}
  </style>
</head>
<body>
  <header>
    <h1>{title}</h1>
    <button id="export">Export edited CSV</button>
  </header>
"""

_HTML_TEMPLATE_TAIL = """\
  <script>
    // Per-folder "Apply to all" copies the bulk-tags input into every new-tags
    // input within the same .folder section.
    document.querySelectorAll('.bulk-apply').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var folder = btn.closest('.folder');
        var value = folder.querySelector('.bulk-tags').value;
        folder.querySelectorAll('.new-tags').forEach(function (input) {
          input.value = value;
        });
      });
    });

    // Export: build a CSV string from each .row and trigger a download.
    document.getElementById('export').addEventListener('click', function () {
      var csv = 'path,content_hash,filename,existing_tags,new_tags,delete\\n';
      document.querySelectorAll('.row').forEach(function (row) {
        var path = row.dataset.path;
        var hash = row.dataset.hash;
        var filename = row.querySelector('.filename').textContent;
        var existing = row.dataset.existing;
        var newTags = row.querySelector('.new-tags').value;
        var del = row.querySelector('.del').checked ? 'x' : '';
        // CSV quoting: wrap in double quotes if comma, quote, or newline present.
        function q(s) {
          if (s === '' || s == null) return '';
          if (/[,"\\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
          return s;
        }
        csv += [q(path), q(hash), q(filename), q(existing), q(newTags), q(del)].join(',') + '\\n';
      });
      var blob = new Blob([csv], { type: 'text/csv' });
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url;
      // Replace .html with .edited.csv in the current page filename
      var name = location.pathname.split('/').pop().replace(/\\.html$/, '.edited.csv');
      a.download = name || 'tag-batch.edited.csv';
      a.click();
      URL.revokeObjectURL(url);
    });
  </script>
</body>
</html>
"""


def _esc(s: str) -> str:
    """Escape for HTML text and attributes. quote=True covers " in attributes."""
    return html_lib.escape(s, quote=True)


def build_html(
    entries: list[BatchEntry],
    kind: Literal["image", "video"],
    timestamp: str,
    top_tags: list[tuple[str, int]] | None = None,
) -> str:
    """Render entries to a self-contained HTML page. Empty entries → friendly
    'nothing to tag' message instead of a folder list. `top_tags` is an optional
    list of (tag, count) pairs already sorted desc; rendered as a sticky
    right-side panel. Omitted/empty → no panel."""
    kind_label = "photos" if kind == "image" else "videos"
    title = f"Tag batch — {len(entries)} {kind_label} from {timestamp}"

    parts: list[str] = [_HTML_TEMPLATE_HEAD.format(title=_esc(title))]

    if top_tags:
        parts.append('  <aside class="tag-cloud">\n')
        parts.append('    <h3>Top tags so far</h3>\n')
        parts.append('    <ul>\n')
        for tag, count in top_tags:
            parts.append(f'      <li><span>{_esc(tag)}</span>'
                         f'<span class="count">{count}</span></li>\n')
        parts.append('    </ul>\n')
        parts.append('  </aside>\n')

    if not entries:
        parts.append('  <p class="empty">No untagged ' + kind_label + ' in scope.</p>\n')
        parts.append(_HTML_TEMPLATE_TAIL)
        return "".join(parts)

    # Group by parent folder, preserving input order across folders.
    folder_groups: list[tuple[str, list[BatchEntry]]] = []
    current_folder: str | None = None
    current_list: list[BatchEntry] = []
    for e in entries:
        parent = e.path.rsplit("/", 1)[0]
        if parent != current_folder:
            if current_folder is not None:
                folder_groups.append((current_folder, current_list))
            current_folder = parent
            current_list = [e]
        else:
            current_list.append(e)
    if current_folder is not None:
        folder_groups.append((current_folder, current_list))

    for folder, group in folder_groups:
        parts.append(f'  <section class="folder" data-folder="{_esc(folder)}">\n')
        parts.append(f'    <h2>{_esc(folder)} <small>({len(group)} files)</small></h2>\n')
        parts.append('    <div class="bulk">\n')
        parts.append('      Apply to all in folder:\n')
        parts.append('      <input class="bulk-tags" placeholder="e.g. event_2019, family">\n')
        parts.append('      <button class="bulk-apply">Apply</button>\n')
        parts.append('    </div>\n')
        for e in group:
            existing_str = ",".join(e.existing_tags)
            existing_display = ", ".join(e.existing_tags) if e.existing_tags else "(none)"
            b64 = base64.b64encode(e.thumbnail_bytes).decode("ascii")
            parts.append(
                f'    <div class="row" data-path="{_esc(e.path)}" '
                f'data-hash="{_esc(e.content_hash)}" '
                f'data-existing="{_esc(existing_str)}">\n'
            )
            parts.append(f'      <img src="data:image/jpeg;base64,{b64}">\n')
            parts.append('      <div class="meta">\n')
            parts.append(f'        <code class="filename">{_esc(e.filename)}</code>\n')
            parts.append(f'        <div>existing: <span class="existing">{_esc(existing_display)}</span></div>\n')
            parts.append('        <label>new tags: <input type="text" class="new-tags" '
                         'placeholder="comma-separated, e.g. seema, performance"></label>\n')
            parts.append('        <label><input type="checkbox" class="del"> mark for deletion</label>\n')
            parts.append('      </div>\n')
            parts.append('    </div>\n')
        parts.append('  </section>\n')

    parts.append(_HTML_TEMPLATE_TAIL)
    return "".join(parts)


import argparse
import sys
from datetime import datetime
from pathlib import Path

import dropbox  # noqa: F401  (kept for type clarity at the run() boundary)
from dropbox.exceptions import AuthError
from dropbox.files import FileMetadata, ListFolderResult

from dbx_client import MissingTokenError, get_client, load_media_config, load_token, with_retry
from dbx_media import classify_media, fetch_existing_tags, fetch_thumbnail, fold_to_folders


def _walk_and_collect_untagged(
    client,
    root: str,
    photo_exts: frozenset[str],
    video_exts: frozenset[str],
    kind: Literal["image", "video"],
    ignored_folders: tuple[str, ...],
    target: int,
) -> tuple[list[str], dict[str, str], Counter[str]]:
    """Walk Dropbox under `root` page by page. For each page, classify entries
    as photos/videos, fetch their existing tags, and accumulate untagged paths.

    Stops as soon as we have at least `target` untagged paths OR the walk is
    exhausted. Avoids the slow "walk every file in the account before filtering"
    pattern.

    Returns (untagged_paths, hash_by_path, tag_counts). The first two are
    ordered by walk traversal; tag_counts aggregates every tag occurrence seen
    during the walk (one increment per (file, tag) pair). Sample is partial
    when the walk exits early on the untagged-target."""
    want = "photo" if kind == "image" else "video"
    kind_label = "photos" if kind == "image" else "videos"
    list_path = "" if root == "/" else root.rstrip("/")

    untagged: list[str] = []
    hash_by_path: dict[str, str] = {}
    tag_counts: Counter[str] = Counter()
    files_walked = 0
    candidates_checked = 0

    result: ListFolderResult = with_retry(
        lambda: client.files_list_folder(list_path, recursive=True)
    )
    while True:
        # 1. Filter this page to media-classified candidates.
        page_candidates: list[tuple[str, str]] = []
        for entry in result.entries:
            if not isinstance(entry, FileMetadata):
                continue
            if entry.content_hash is None:
                continue
            files_walked += 1
            if any(seg.startswith(".") for seg in entry.path_display.split("/")):
                continue
            path_lower = entry.path_display.lower()
            if any(path_lower == f or path_lower.startswith(f + "/") for f in ignored_folders):
                continue
            if classify_media(entry.path_display, photo_exts, video_exts) == want:
                page_candidates.append((entry.path_display, entry.content_hash))

        # 2. Tag-lookup this page's candidates and accumulate untagged.
        if page_candidates:
            tags_by_path = fetch_existing_tags(client, [p for p, _ in page_candidates])
            for p, h in page_candidates:
                candidates_checked += 1
                file_tags = tags_by_path.get(p) or []
                if not file_tags:
                    untagged.append(p)
                    hash_by_path[p] = h
                else:
                    tag_counts.update(file_tags)

        print(f"  walked {files_walked} files, checked {candidates_checked} {kind_label}, "
              f"{len(untagged)} untagged so far...")

        # 3. Early exit if we hit the target.
        if len(untagged) >= target:
            print(f"  reached batch target ({target}); stopping walk early.")
            break
        if not result.has_more:
            print(f"  walk complete (scanned every {kind_label[:-1]} in scope).")
            break
        cursor = result.cursor
        result = with_retry(lambda c=cursor: client.files_list_folder_continue(c))

    return untagged, hash_by_path, tag_counts


def _write_empty_html(mc, kind: Literal["image", "video"]) -> int:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    file_ts = datetime.now().strftime("%Y-%m-%d-%H%M")
    suffix = "images" if kind == "image" else "videos"
    html = build_html(entries=[], kind=kind, timestamp=timestamp)
    out_path = mc.csv_output_dir / f"tag-batch-{suffix}-{file_ts}.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html)
    print(f"No untagged {suffix} in scope. Wrote empty page to {out_path}")
    return 0


def run(kind: Literal["image", "video"]) -> int:
    parser = argparse.ArgumentParser(
        description=f"Build a tag-review batch for {'photos' if kind == 'image' else 'videos'}."
    )
    parser.add_argument("--config", default="config.ini")
    parser.add_argument("--root", default="/", help="Dropbox path to scan (default: /)")
    args = parser.parse_args()

    root = args.root.strip()
    if not root.startswith("/"):
        root = "/" + root

    try:
        mc = load_media_config(Path(args.config))
        token = load_token()
        client = get_client(token)
    except FileNotFoundError as exc:
        print(f"Config error: {exc}", file=sys.stderr); return 1
    except MissingTokenError as exc:
        print(f"Token error: {exc}", file=sys.stderr); return 1
    except AuthError as exc:
        print(f"Dropbox auth failed: {exc}.", file=sys.stderr); return 1

    print(f"Walking Dropbox under {root} (streaming — stops once {mc.batch_size} "
          f"untagged found)...")
    untagged_paths, hash_by_path, tag_counts = _walk_and_collect_untagged(
        client, root, mc.photo_extensions, mc.video_extensions, kind,
        mc.ignored_folders, target=mc.batch_size,
    )

    if not untagged_paths:
        return _write_empty_html(mc, kind)

    folded = fold_to_folders(untagged_paths)
    selected_paths = select_batch(folded, mc.batch_size)
    print(f"Selected {len(selected_paths)} for this batch (batch_size={mc.batch_size}).")

    print("Fetching thumbnails...")
    entries: list[BatchEntry] = []
    for i, p in enumerate(selected_paths, start=1):
        try:
            thumb = fetch_thumbnail(client, p, mc.thumbnail_width)
        except AuthError:
            raise
        except Exception as exc:
            print(f"  WARN: thumbnail failed for {p}: {exc}", file=sys.stderr)
            continue
        entries.append(BatchEntry(
            path=p,
            filename=p.rsplit("/", 1)[-1],
            content_hash=hash_by_path[p],
            # Selected paths are untagged by construction (filter happens during
            # the walk). The HTML will show "(none)" for the existing-tags column.
            existing_tags=[],
            thumbnail_bytes=thumb,
        ))
        if i % 10 == 0:
            print(f"    {i}/{len(selected_paths)}")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    file_ts = datetime.now().strftime("%Y-%m-%d-%H%M")
    html = build_html(
        entries=entries, kind=kind, timestamp=timestamp,
        top_tags=tag_counts.most_common(20),
    )

    suffix = "images" if kind == "image" else "videos"
    out_path = mc.csv_output_dir / f"tag-batch-{suffix}-{file_ts}.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html)
    print(f"\nWrote {len(entries)} {suffix} to {out_path}")
    print(f"Open in browser, edit tags, click 'Export', then run:")
    print(f"  python update_{suffix}.py --config {args.config} --csv {out_path.with_suffix('.edited.csv')}")
    return 0
