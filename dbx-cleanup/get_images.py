"""Build a tag-review batch for untagged photos. See get_media.run() for details."""

import sys
from get_media import run

if __name__ == "__main__":
    sys.exit(run(kind="image"))
