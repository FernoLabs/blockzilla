"""Fetch the small source records before a retained CAR import starts."""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.request


def save(path, value):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def retry_delay(error):
    value = error.headers.get("Retry-After") if isinstance(error, urllib.error.HTTPError) else None
    if value:
        try:
            return max(60, int(value))
        except ValueError:
            try:
                date = parsedate_to_datetime(value)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=timezone.utc)
                return max(60, int((date - datetime.now(timezone.utc)).total_seconds()))
            except (ValueError, TypeError, OverflowError):
                pass
    return 300


def prepare_source(control, epoch, expected_bytes, attempts=24):
    control = Path(control)
    names = {
        f"{epoch}.recap.yaml": 100_000,
        f"epoch-{epoch}.cid": 1024,
        f"{epoch}.slots.txt": 10_000_000,
    }
    base = f"https://files.old-faithful.net/{epoch}/"
    for name, limit in names.items():
        path = control / name
        if path.exists():
            if not path.is_file() or path.is_symlink() or not 0 < path.stat().st_size <= limit:
                raise RuntimeError(f"Invalid retained source record: {path}")
            continue
        for attempt in range(1, attempts + 1):
            try:
                request = urllib.request.Request(base + name, headers={"User-Agent": "aria2/1.36.0"})
                with urllib.request.urlopen(request, timeout=30) as response:
                    if response.status != 200 or response.geturl() != base + name:
                        raise RuntimeError(f"Unexpected source response for {name}")
                    data = response.read(limit + 1)
                if not 0 < len(data) <= limit:
                    raise RuntimeError(f"Invalid source record length: {name}")
                temporary = path.with_suffix(path.suffix + ".part")
                with temporary.open("wb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.link(temporary, path)
                temporary.unlink()
                break
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                if isinstance(error, urllib.error.HTTPError) and error.code not in (404, 408, 429, 500, 502, 503, 504):
                    raise
                if attempt == attempts:
                    raise RuntimeError(f"Source retry limit reached for {name}: {error}") from error
                delay = retry_delay(error)
                state = {
                    "state": "WAITING_SOURCE_METADATA",
                    "epoch": epoch,
                    "file": name,
                    "attempt": attempt,
                    "retry_after_seconds": delay,
                    "error": str(error),
                    "updated_at": datetime.now().astimezone().isoformat(),
                }
                save(control / "status.json", state)
                print(json.dumps(state), flush=True)
                # The worker remains separate from the chat during source retries.
                until = time.monotonic() + delay
                while True:
                    remaining = until - time.monotonic()
                    if remaining <= 0:
                        break
                    time.sleep(min(30, remaining))

    recap = (control / f"{epoch}.recap.yaml").read_text()
    values = dict(re.findall(r"^([a-z_0-9]+):\s*([^\n]+)$", recap, re.MULTILINE))
    numeric = ["epoch", "num_transactions", "num_slots", "first_slot", "last_slot", "car_file_size_bytes", "num_total_missing_tx_meta"]
    metadata = {key: int(values[key]) for key in numeric}
    metadata["epoch_cid"] = values["epoch_cid"].strip()
    cid = (control / f"epoch-{epoch}.cid").read_text().strip()
    if metadata["epoch"] != epoch or metadata["car_file_size_bytes"] != expected_bytes:
        raise RuntimeError("Published epoch or CAR length changed")
    if metadata["epoch_cid"] != cid or not re.fullmatch(r"b[a-z2-7]{20,}", cid):
        raise RuntimeError("Published epoch CID records disagree")
    if metadata["num_total_missing_tx_meta"] != 0 or metadata["num_transactions"] <= 0:
        raise RuntimeError("Incomplete published transaction metadata")
    slots = [int(line) for line in (control / f"{epoch}.slots.txt").read_text().splitlines()]
    first, end = epoch * 432000, (epoch + 1) * 432000
    if not slots or any(a >= b for a, b in zip(slots, slots[1:])):
        raise RuntimeError("Published slot list is empty or not strictly ordered")
    if any(slot != first - 1 and not first <= slot < end for slot in slots):
        raise RuntimeError("Published slot list crosses an unexpected epoch boundary")
    covered = [slot for slot in slots if first <= slot < end]
    if not covered or len(covered) != metadata["num_slots"]:
        raise RuntimeError("Published block count and slot list disagree")
    if (covered[0], covered[-1]) != (metadata["first_slot"], metadata["last_slot"]):
        raise RuntimeError("Published slot bounds and slot list disagree")
    metadata["source_records"] = {
        name: {"url": base + name, "bytes": (control / name).stat().st_size,
               "sha256": hashlib.sha256((control / name).read_bytes()).hexdigest()}
        for name in names
    }
    metadata["checked_at"] = datetime.now().astimezone().isoformat()
    save(control / "source.json", metadata)
    print(json.dumps({"state": "SOURCE_METADATA_READY", "epoch": epoch,
                      "blocks": metadata["num_slots"], "transactions": metadata["num_transactions"]}), flush=True)
    return metadata
