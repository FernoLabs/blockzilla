#!/usr/bin/env bash
set -euo pipefail

# Usage: ./fetch_epochs.sh <start> <end> [base_url] [outfile]
# Example: ./fetch_epochs.sh 0 900
# This generates URLs from start..end (inclusive) like:
# https://files.old-faithful.net/42/epoch-42.car

START="${1:-}"
END="${2:-}"
BASE_URL="${3:-https://files.old-faithful.net}"
OUTFILE="${4:-urls.txt}"

if [[ -z "${START}" || -z "${END}" ]]; then
  echo "Usage: $0 <start> <end> [base_url] [outfile]" >&2
  exit 1
fi

# Ensure aria2c is available
if ! command -v aria2c >/dev/null 2>&1; then
  echo "Error: aria2c not found in PATH." >&2
  exit 1
fi

# Build URL list
: > "${OUTFILE}"
for i in $(seq "${START}" "${END}"); do
  echo "${BASE_URL}/${i}/epoch-${i}.car" >> "${OUTFILE}"
done

echo "Wrote $(wc -l < "${OUTFILE}") URLs to ${OUTFILE}"

# Launch aria2c
aria2c -c -i "${OUTFILE}" -x 16 -s 16 -j 8 --file-allocation=none