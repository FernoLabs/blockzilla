#!/bin/sh
set -eu

: "${SHRED_JOURNAL_ID:?SHRED_JOURNAL_ID must be set}"
case "$SHRED_JOURNAL_ID" in
  *[!0-9a-f]*)
    echo "SHRED_JOURNAL_ID must be exactly 32 lowercase hexadecimal characters" >&2
    exit 64
    ;;
esac
if [ "${#SHRED_JOURNAL_ID}" -ne 32 ]; then
  echo "SHRED_JOURNAL_ID must be exactly 32 lowercase hexadecimal characters" >&2
  exit 64
fi

umask 077
config=/tmp/raw-shred-pull-source.json
sed "s/__SHRED_JOURNAL_ID__/${SHRED_JOURNAL_ID}/g" \
  /etc/hivezilla/raw-shred-pull.json.template > "$config"
exec /usr/local/bin/hivezilla serve-shred-spool-pull-source --config "$config"
