"""Apply edited photo tags to Dropbox. See update_media.run() for details."""

import sys
from update_media import run

if __name__ == "__main__":
    sys.exit(run(kind="image"))
