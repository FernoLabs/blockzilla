# NAS raw gRPC safety capture

This deployment records the exact Yellowstone `SubscribeUpdate` protobuf for every confirmed
Solana block, including transactions and entries. It is an append-only safety capture: the
Compose project contains no uploader, acknowledgement consumer, retention worker, or deletion
command.

The durable host path is `/home/ach/data/blockzilla-emergency-grpc`. The `continuous` spool rolls
only its immutable 1 GiB WAL segment files; it does not roll or retire whole generations. Any sealed
generation left by an earlier recorder remains untouched. The recorder stops before the filesystem
falls below 1 TiB free instead of deleting data. Docker restarts it after a stream failure or 180
seconds without a durable block.

## Prepare and start

Create all required directories before the first start. The image runs as the fixed unprivileged
UID/GID `10001:10001`.

```sh
sudo install -d -o 10001 -g 10001 -m 0700 \
  /home/ach/data/blockzilla-emergency-grpc \
  /home/ach/data/blockzilla-emergency-grpc/continuous \
  /home/ach/data/blockzilla-emergency-grpc/continuous/.monitoring

docker compose \
  --env-file /home/ach/.config/blockzilla-emergency-capture/live-producer.env \
  -f /home/ach/dev/blockzilla-emergency-capture/docker-compose.yml \
  up -d
```

The environment file must be mode `0600` and contain only the Triton endpoint and x-token. Do not
put it in Git or print it in diagnostics.

## Verify without mutating data

```sh
docker inspect \
  --format='state={{.State.Status}} restarts={{.RestartCount}}' \
  blockzilla-emergency-capture-raw-capture-1

du -sh /home/ach/data/blockzilla-emergency-grpc
tail -n 1 /home/ach/data/blockzilla-emergency-grpc/continuous/raw-blocks.jsonl
find /home/ach/data/blockzilla-emergency-grpc/cache/sealed \
  -mindepth 1 -maxdepth 1 -type d -print | sort
```

Check total directory bytes rather than one WAL segment: 1 GiB WAL segments roll, so the currently
active segment can become smaller while the retained total continues growing. The
healthcheck becomes unhealthy if the durable journal has not advanced for four minutes; it never
mutates captured data.

## Deletion invariant

Do not add `rm`, lifecycle expiration, size-based pruning, acknowledgement-driven garbage
collection, or object-storage migration to this project. NAS data stays authoritative until a
separate pull/replay tool has been implemented and its end-to-end durability has been proven.
