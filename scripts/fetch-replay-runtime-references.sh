#!/usr/bin/env bash
set -euo pipefail

# Fetch the exact source revisions used by the Replay Runtime V0 research.
# Existing directories are left untouched so this script never overwrites a
# developer's reference tree.

reference_root="${1:-/tmp/blockzilla-runtime-references}"
mkdir -p "$reference_root"

fetch_revision() {
  local name="$1"
  local url="$2"
  local revision="$3"
  local destination="$reference_root/$name"

  if [[ -e "$destination" ]]; then
    printf 'refusing to overwrite existing path: %s\n' "$destination" >&2
    return 1
  fi

  git init --quiet "$destination"
  git -C "$destination" remote add origin "$url"
  git -C "$destination" fetch --quiet --depth 1 origin "$revision"
  git -C "$destination" checkout --quiet --detach FETCH_HEAD
  printf '%-22s %s\n' "$name" "$(git -C "$destination" rev-parse HEAD)"
}

fetch_revision \
  agave \
  https://github.com/anza-xyz/agave.git \
  e1566c2ec46ab4ba8f6f12ebb5399bfff62c4dc3

fetch_revision \
  litesvm \
  https://github.com/LiteSVM/litesvm.git \
  8dea7cde73923cf2a60b1c934e40442df9cf20c2

fetch_revision \
  firedancer \
  https://github.com/firedancer-io/firedancer.git \
  decca0535765f25e1dbe94258db1408d1213c17f

fetch_revision \
  mithril \
  https://github.com/Overclock-Validator/mithril.git \
  2325aa5802c176bd97768c46188370ee47706d2b

fetch_revision \
  quasar-svm \
  https://github.com/blueshift-gg/quasar-svm.git \
  b5a9363de13e0f1e5e4559f4251c77563c3c9986

fetch_revision \
  solana-v1.0.7 \
  https://github.com/solana-labs/solana.git \
  57abc370fa39e42e8fb84145a30395ddcf891692

fetch_revision \
  solana-v1.0.8 \
  https://github.com/solana-labs/solana.git \
  2a617f2d07f714918891f2b479d1cb1c324f0365

fetch_revision \
  solana-v1.1.14 \
  https://github.com/solana-labs/solana.git \
  fd5222ad21673494fa1a1850ec131ecda5362ba2

fetch_revision \
  solana-sbpf-v0.21.0 \
  https://github.com/anza-xyz/sbpf.git \
  f95941e1f8ffed43d8722543f350b09e389f332f

fetch_revision \
  solana-sbpf-v0.22.0 \
  https://github.com/anza-xyz/sbpf.git \
  db4f0681951171ee97988989695ceef67fe3dbb3

fetch_revision \
  yellowstone-faithful \
  https://github.com/rpcpool/yellowstone-faithful.git \
  ec48e6c12e7c1cb9e8b03fb1d045057d7bba7ba9
