#!/usr/bin/env bash
set -euo pipefail

# =========================
# Configurable paths
# =========================
BASE_DIR="/Users/augustin/Developement/ferno/blockzilla/asset/blockzilla"       # Base directory for all epochs
GENESIS="$BASE_DIR/genesis.tar.bz2"  # Path to genesis tarball
# =========================

# Epoch number (first argument, default 0)
EPOCH=${1:-0}
EPOCH_DIR="$BASE_DIR/epoch-$EPOCH"   # Dedicated folder for this epoch
CAR_DIR="$EPOCH_DIR/cars"
INDEX_DIR="$EPOCH_DIR/indexes"
SLOTS_DIR="$EPOCH_DIR/slots"
CONFIG_DIR="$EPOCH_DIR/config"

BASE_URL="https://files.old-faithful.net/$EPOCH"

mkdir -p "$CAR_DIR" "$INDEX_DIR" "$SLOTS_DIR" "$CONFIG_DIR"

echo "Setting up epoch $EPOCH in $EPOCH_DIR"

# -------------------------
# Download slots list
# -------------------------
echo "Downloading slots list..."
curl -fSL "$BASE_URL/$EPOCH.slots.txt" -o "$SLOTS_DIR/$EPOCH.slots.txt"

# -------------------------
# Handle CID file
# -------------------------
LOCAL_CID_FILE="./epoch-$EPOCH.cid"
CID_FILE="$CAR_DIR/epoch-$EPOCH.cid"

if [[ -f "$LOCAL_CID_FILE" ]]; then
    echo "Using existing CID file from $LOCAL_CID_FILE"
    cp "$LOCAL_CID_FILE" "$CID_FILE"
else
    echo "Downloading CID file..."
    curl -fSL "$BASE_URL/epoch-$EPOCH.cid" -o "$CID_FILE"
fi

CID=$(cat "$CID_FILE")
echo "CID for epoch $EPOCH: $CID"

# -------------------------
# Download CAR file
# -------------------------
CAR_FILE="$CAR_DIR/epoch-$EPOCH.car"
if [[ ! -f "$CAR_FILE" ]]; then
    echo "Downloading CAR file..."
    curl -fSL "$BASE_URL/epoch-$EPOCH.car" -o "$CAR_FILE"
fi

# -------------------------
# Download indexes
# -------------------------
echo "Downloading prebuilt indexes..."
curl -fSL "$BASE_URL/epoch-$EPOCH-$CID-mainnet-cid-to-offset-and-size.index" -o "$INDEX_DIR/epoch-$EPOCH-$CID-mainnet-cid-to-offset-and-size.index"
curl -fSL "$BASE_URL/epoch-$EPOCH-$CID-mainnet-sig-exists.index" -o "$INDEX_DIR/epoch-$EPOCH-$CID-mainnet-sig-exists.index"
curl -fSL "$BASE_URL/epoch-$EPOCH-$CID-mainnet-sig-to-cid.index" -o "$INDEX_DIR/epoch-$EPOCH-$CID-mainnet-sig-to-cid.index"
curl -fSL "$BASE_URL/epoch-$EPOCH-$CID-mainnet-slot-to-blocktime.index" -o "$INDEX_DIR/epoch-$EPOCH-$CID-mainnet-slot-to-blocktime.index"
curl -fSL "$BASE_URL/epoch-$EPOCH-$CID-mainnet-slot-to-cid.index" -o "$INDEX_DIR/epoch-$EPOCH-$CID-mainnet-slot-to-cid.index"

# -------------------------
# Generate Faithful RPC YAML
# -------------------------
CONFIG_FILE="$CONFIG_DIR/epoch-$EPOCH.yaml"

cat > "$CONFIG_FILE" <<EOF
epoch: $EPOCH
version: 1
data:
  car:
    uri: $CAR_DIR/epoch-$EPOCH.car
  filecoin:
    enable: false
genesis:
  uri: $GENESIS
indexes:
  cid_to_offset_and_size:
    uri: '$INDEX_DIR/epoch-$EPOCH-$CID-mainnet-cid-to-offset-and-size.index'
  slot_to_cid:
    uri: '$INDEX_DIR/epoch-$EPOCH-$CID-mainnet-slot-to-cid.index'
  sig_to_cid:
    uri: '$INDEX_DIR/epoch-$EPOCH-$CID-mainnet-sig-to-cid.index'
  sig_exists:
    uri: '$INDEX_DIR/epoch-$EPOCH-$CID-mainnet-sig-exists.index'
  slot_to_blocktime:
    uri: '$INDEX_DIR/epoch-$EPOCH-$CID-mainnet-slot-to-blocktime.index'
EOF

echo "Generated Faithful RPC config: $CONFIG_FILE"
echo "All files for epoch $EPOCH are organized in $EPOCH_DIR"
