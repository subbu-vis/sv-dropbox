"""Engine for get_images.py and get_videos.py.

Walks Dropbox, looks up existing tags, filters to untagged, folder-clusters,
packs into a batch sized by config, downloads thumbnails, and writes a
self-contained HTML review page.
"""

from __future__ import annotations

import base64
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
) -> str:
    """Render entries to a self-contained HTML page. Empty entries → friendly
    'nothing to tag' message instead of a folder list."""
    kind_label = "photos" if kind == "image" else "videos"
    title = f"Tag batch — {len(entries)} {kind_label} from {timestamp}"

    parts: list[str] = [_HTML_TEMPLATE_HEAD.format(title=_esc(title))]

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
        parts.append('      <input class="bulk-tags" placeholder="e.g. event-2019, family">\n')
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
