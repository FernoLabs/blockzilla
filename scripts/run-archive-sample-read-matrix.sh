#!/usr/bin/env bash
# Keep the existing command name. The runner uses Python's standard library.
set -euo pipefail
exec python3 "$(dirname "$0")/archive_sample_matrix.py" "$@"
