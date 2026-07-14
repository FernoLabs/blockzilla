#!/bin/sh
set -eu

# Provision a hard-capped, preallocated ext4 cache on the Dokploy host. This is
# deliberately a bounded disk-first tier: sealed generations remain local until
# pressure requires an exactly verified Backblaze spill.

umask 077
export LC_ALL=C

CACHE_BYTES=${BLOCKZILLA_CACHE_BYTES:-3221225472}
ROOT_RESERVE_BYTES=${BLOCKZILLA_ROOT_RESERVE_BYTES:-4294967296}
CACHE_IMAGE=${BLOCKZILLA_CACHE_IMAGE:-/var/lib/blockzilla/raw-cache.ext4}
MOUNT_PATH=${BLOCKZILLA_CACHE_MOUNT_PATH:-/mnt/blockzilla-raw}
VOLUME_NAME=${BLOCKZILLA_RAW_VOLUME_NAME:-blockzilla-live-raw-data-external}
CACHE_LABEL=bz-raw-cache-v1
MARKER_NAME=.blockzilla-raw-volume
MARKER_VALUE=blockzilla-raw-cache-v1
DATA_DIR_NAME=grpc-cache
RUNTIME_UID=${BLOCKZILLA_RUNTIME_UID:-10001}
RUNTIME_GID=${BLOCKZILLA_RUNTIME_GID:-10001}
FSTAB=${BLOCKZILLA_FSTAB_PATH:-/etc/fstab}

die() {
  printf '%s\n' "provision-3gb-cache: $*" >&2
  exit 1
}

require_uint() {
  case "$2" in
    ''|*[!0-9]*) die "$1 must be an unsigned integer" ;;
  esac
}

require_uint BLOCKZILLA_CACHE_BYTES "$CACHE_BYTES"
require_uint BLOCKZILLA_ROOT_RESERVE_BYTES "$ROOT_RESERVE_BYTES"
require_uint BLOCKZILLA_RUNTIME_UID "$RUNTIME_UID"
require_uint BLOCKZILLA_RUNTIME_GID "$RUNTIME_GID"

[ "$(id -u)" -eq 0 ] || die "must run as root"
[ "$CACHE_BYTES" -eq 3221225472 ] || \
  die "this deployment requires an exact 3 GiB cache (3221225472 bytes)"

for command_name in \
  blkid blockdev df docker fallocate findmnt losetup mkfs.ext4 mount readlink \
  stat sync
do
  command -v "$command_name" >/dev/null 2>&1 || die "missing command: $command_name"
done

case "$CACHE_IMAGE" in
  /*) ;;
  *) die "BLOCKZILLA_CACHE_IMAGE must be absolute" ;;
esac
case "$MOUNT_PATH" in
  /*) ;;
  *) die "BLOCKZILLA_CACHE_MOUNT_PATH must be absolute" ;;
esac
[ "$CACHE_IMAGE" != "$MOUNT_PATH" ] || die "cache image and mount path must differ"

image_parent=$(dirname "$CACHE_IMAGE")
mkdir -p "$image_parent" "$MOUNT_PATH"
[ ! -L "$image_parent" ] || die "cache image parent must not be a symlink"
[ ! -L "$MOUNT_PATH" ] || die "cache mount path must not be a symlink"
[ "$(readlink -f "$image_parent")" = "$image_parent" ] || \
  die "cache image parent contains a symlink"
[ "$(readlink -f "$MOUNT_PATH")" = "$MOUNT_PATH" ] || \
  die "cache mount path contains a symlink"

root_device=$(stat -c %d /)

if [ ! -e "$CACHE_IMAGE" ]; then
  root_available_kib=$(df -Pk "$image_parent" | awk 'NR == 2 { print $4 }')
  case "$root_available_kib" in
    ''|*[!0-9]*) die "could not read root free space" ;;
  esac
  root_available_bytes=$((root_available_kib * 1024))
  required_bytes=$((CACHE_BYTES + ROOT_RESERVE_BYTES))
  [ "$required_bytes" -gt "$CACHE_BYTES" ] || die "required byte calculation overflow"
  [ "$root_available_bytes" -ge "$required_bytes" ] || \
    die "need at least $required_bytes root bytes free before reserving cache; have $root_available_bytes"

  image_tmp=$CACHE_IMAGE.new.$$
  trap 'rm -f "$image_tmp"' EXIT INT TERM HUP
  [ ! -e "$image_tmp" ] || die "temporary cache image already exists: $image_tmp"
  fallocate -l "$CACHE_BYTES" "$image_tmp"
  [ "$(stat -c %s "$image_tmp")" -eq "$CACHE_BYTES" ] || die "cache image has wrong size"
  allocated_bytes=$(( $(stat -c %b "$image_tmp") * 512 ))
  [ "$allocated_bytes" -ge "$CACHE_BYTES" ] || die "cache image is sparse"
  # mke2fs discards preallocated regular-file extents by default. Disabling
  # discard preserves the hard reservation on the host root filesystem.
  mkfs.ext4 -F -m 0 -E nodiscard -L "$CACHE_LABEL" "$image_tmp" >/dev/null
  [ "$(stat -c %s "$image_tmp")" -eq "$CACHE_BYTES" ] || \
    die "formatted cache image has wrong size"
  allocated_bytes=$(( $(stat -c %b "$image_tmp") * 512 ))
  [ "$allocated_bytes" -ge "$CACHE_BYTES" ] || \
    die "formatted cache image is sparse"
  [ "$(blkid -s TYPE -o value "$image_tmp")" = ext4 ] || \
    die "formatted cache image is not ext4"
  [ "$(blkid -s LABEL -o value "$image_tmp")" = "$CACHE_LABEL" ] || \
    die "formatted cache image has an unexpected filesystem label"
  sync -f "$image_tmp"
  mv "$image_tmp" "$CACHE_IMAGE"
  trap - EXIT INT TERM HUP
  sync -f "$image_parent"
fi

[ -f "$CACHE_IMAGE" ] && [ ! -L "$CACHE_IMAGE" ] || \
  die "cache image must be a regular non-symlink file"
[ "$(stat -c %s "$CACHE_IMAGE")" -eq "$CACHE_BYTES" ] || die "cache image has wrong size"
allocated_bytes=$(( $(stat -c %b "$CACHE_IMAGE") * 512 ))
[ "$allocated_bytes" -ge "$CACHE_BYTES" ] || die "cache image is sparse"
[ "$(blkid -s TYPE -o value "$CACHE_IMAGE")" = ext4 ] || die "cache image is not ext4"
[ "$(blkid -s LABEL -o value "$CACHE_IMAGE")" = "$CACHE_LABEL" ] || \
  die "cache image has an unexpected filesystem label"

fstab_line="$CACHE_IMAGE $MOUNT_PATH ext4 loop,nosuid,nodev,noexec,noatime 0 2"
if awk -v source="$CACHE_IMAGE" -v target="$MOUNT_PATH" '
    $1 == source || $2 == target { found = 1 }
    END { exit found ? 0 : 1 }
  ' "$FSTAB"
then
  existing_line=$(awk -v source="$CACHE_IMAGE" -v target="$MOUNT_PATH" \
    '$1 == source || $2 == target { print; exit }' "$FSTAB")
  [ "$existing_line" = "$fstab_line" ] || die "conflicting fstab entry: $existing_line"
else
  printf '\n%s\n' "$fstab_line" >> "$FSTAB"
  sync -f "$FSTAB"
fi
if ! findmnt -rn -M "$MOUNT_PATH" >/dev/null 2>&1; then
  mount "$MOUNT_PATH"
fi

[ "$(findmnt -nr -o TARGET -M "$MOUNT_PATH")" = "$MOUNT_PATH" ] || \
  die "cache is not an exact mountpoint"
[ "$(findmnt -nr -o FSTYPE -M "$MOUNT_PATH")" = ext4 ] || \
  die "mounted cache is not ext4"
loop_device=$(findmnt -nr -o SOURCE -M "$MOUNT_PATH")
case "$loop_device" in
  /dev/loop*) ;;
  *) die "mounted cache is not backed by a loop device: $loop_device" ;;
esac
loop_backing=$(losetup --noheadings --output BACK-FILE "$loop_device" | sed -n '1{s/^[[:space:]]*//;s/[[:space:]]*$//;p;}')
[ "$(readlink -f "$loop_backing")" = "$CACHE_IMAGE" ] || \
  die "loop device uses unexpected backing file: $loop_backing"
[ "$(blockdev --getsize64 "$loop_device")" -eq "$CACHE_BYTES" ] || \
  die "loop device has unexpected capacity"
cache_device=$(stat -c %d "$MOUNT_PATH")
[ "$cache_device" != "$root_device" ] || die "cache mount is on the host root filesystem"

chown root:root "$MOUNT_PATH"
chmod 0755 "$MOUNT_PATH"
marker_path=$MOUNT_PATH/$MARKER_NAME
if [ -e "$marker_path" ]; then
  [ -f "$marker_path" ] && [ ! -L "$marker_path" ] || die "invalid existing volume marker"
  IFS= read -r marker_existing < "$marker_path" || die "cannot read existing volume marker"
  [ "$marker_existing" = "$MARKER_VALUE" ] || die "unexpected existing volume marker"
else
  marker_tmp=$MOUNT_PATH/$MARKER_NAME.new.$$
  printf '%s\n' "$MARKER_VALUE" > "$marker_tmp"
  chown root:root "$marker_tmp"
  chmod 0444 "$marker_tmp"
  mv "$marker_tmp" "$marker_path"
  sync -f "$MOUNT_PATH"
fi

data_path=$MOUNT_PATH/$DATA_DIR_NAME
if [ -e "$data_path" ]; then
  [ -d "$data_path" ] && [ ! -L "$data_path" ] || die "invalid cache data directory"
else
  mkdir "$data_path"
fi
chown "$RUNTIME_UID:$RUNTIME_GID" "$data_path"
chmod 0700 "$data_path"
sync -f "$MOUNT_PATH"

if docker volume inspect "$VOLUME_NAME" >/dev/null 2>&1; then
  existing_driver=$(docker volume inspect -f '{{.Driver}}' "$VOLUME_NAME")
  existing_type=$(docker volume inspect -f '{{with .Options}}{{index . "type"}}{{end}}' "$VOLUME_NAME")
  existing_options=$(docker volume inspect -f '{{with .Options}}{{index . "o"}}{{end}}' "$VOLUME_NAME")
  existing_device=$(docker volume inspect -f '{{with .Options}}{{index . "device"}}{{end}}' "$VOLUME_NAME")
  [ "$existing_driver" = local ] && [ "$existing_type" = none ] \
    && [ "$existing_options" = bind ] && [ "$existing_device" = "$MOUNT_PATH" ] || \
    die "Docker volume $VOLUME_NAME has unexpected options"
else
  docker volume create --driver local \
    --opt type=none --opt o=bind --opt device="$MOUNT_PATH" \
    "$VOLUME_NAME" >/dev/null
fi

printf '%s\n' "cache_ready bytes=$CACHE_BYTES mount=$MOUNT_PATH volume=$VOLUME_NAME"
