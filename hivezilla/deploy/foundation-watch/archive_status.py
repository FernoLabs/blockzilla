"""Bounded, read-only archive telemetry for the private Foundation monitor."""
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import stat

JOB = Path("jobs/car-backfill-20261012")
UNIT = "hivezilla-foundation-car-backfill-20261012.service"
LIMIT = 65536
PHASES = {"SOURCE_METADATA": 0, "DOWNLOADING": 0, "REGISTRIES": 1,
          "REGISTRY_INDEX": 2, "COMPACTING": 3, "VALIDATING": 4, "UPLOADING": 4}


def integer(value, maximum=2**53 - 1):
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError("invalid_archive_number")
    return value


def read_json(path):
    if not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError("invalid_archive_file")
    with path.open("rb") as f:
        raw = f.read(LIMIT + 1)
    if len(raw) > LIMIT:
        raise ValueError("archive_file_too_large")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("invalid_archive_object")
    return value


def tail(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("invalid_archive_log")
    with path.open("rb") as f:
        f.seek(max(0, info.st_size - LIMIT))
        text = f.read(LIMIT).decode("utf-8", errors="replace")
    return re.sub(r"\x1b\[[0-9;]*m", "", text).splitlines(), info.st_mtime_ns // 1000000


def duration(value):
    if not re.fullmatch(r"(?:\s*\d+(?:\.\d+)?[dhms])+\s*", value):
        return None
    units = {"d": 86400, "h": 3600, "m": 60, "s": 1}
    seconds = sum(float(n) * units[u] for n, u in re.findall(r"(\d+(?:\.\d+)?)([dhms])", value))
    return math.ceil(seconds) if 0 <= seconds <= 365 * 86400 else None


def rate(number, unit):
    binary = "i" in unit
    power = "BKMGT".index(unit[0]) if unit[0] != "B" else 0
    value = float(number) * (1024 if binary else 1000) ** power
    return value if math.isfinite(value) and 0 <= value <= 1e12 else None


def timestamp(line):
    match = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z)", line)
    if match:
        return round(datetime.fromisoformat(match[1].replace("Z", "+00:00")).timestamp() * 1000)
    match = re.match(r"(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d)", line)
    if match:
        return round(datetime.strptime(match[1], "%Y/%m/%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)
    return None


def progress(job, epoch, state, stage_start, now):
    path = job / ("controller.log" if state == "UPLOADING" else f"epoch-{epoch}.log")
    try:
        lines, modified = tail(path)
    except (OSError, ValueError):
        return None
    marker = {"REGISTRIES": "[Split Compact Registry]", "COMPACTING": "[Archive V2 Hot Write]"}.get(state)
    for line in reversed(lines):
        percent = speed = eta = stamp = None
        if marker and marker in line:
            match = re.search(r"progress=([0-9.]+)% ETA=([^|]+)\|", line)
            if not match:
                continue
            percent, eta, stamp = float(match[1]), duration(match[2].strip()), timestamp(line)
        elif state == "DOWNLOADING":
            if "Checksum:" in line:
                check = re.search(r"\[Checksum:[^\]]*?\(([0-9.]+)%\)\]", line)
                if not check or not -30000 <= now - modified <= 120000 or modified < stage_start - 2000:
                    return None
                percent = float(check[1])
                if not 0 <= percent <= 100:
                    return None
                return {"state": "CHECKING_CAR_HASH", "progress_percent": percent,
                        "bytes_per_second": None, "eta_seconds": None,
                        "progress_updated_unix_ms": modified}
            match = re.search(r"\(([0-9.]+)%\).*?DL:([0-9.]+)([KMGT]?i?B)\s+ETA:([^\]]+)\]", line)
            if not match:
                continue
            percent, speed, eta, stamp = float(match[1]), rate(match[2], match[3]), duration(match[4]), modified
            # aria2 checks the complete CAR hash after transfer. A transfer ETA
            # of zero cannot prove that this download stage has completed.
            if percent >= 99.95:
                eta = speed = None
        elif state == "UPLOADING":
            # A final rclone transfer line can use "ETA -" while object and
            # manifest validation still follow. Keep that newer progress line;
            # falling back to an earlier numeric ETA would renew old data.
            match = re.search(r",\s*([0-9.]+)%,\s*([0-9.]+)\s+([KMGT]?i?B)/s,\s*ETA\s+([0-9a-z. ]+|-)\s*$", line)
            if not match:
                continue
            percent, speed, eta, stamp = float(match[1]), rate(match[2], match[3]), duration(match[4].strip()), timestamp(line)
        else:
            continue
        if (stamp is None or stamp < stage_start - 2000 or not -30000 <= now - stamp <= 120000
                or not 0 <= percent <= 100):
            return None
        # Registry final writes and upload validation can follow a zero scan ETA.
        if eta == 0:
            eta = None
        return {"progress_percent": percent, "bytes_per_second": speed,
                "eta_seconds": eta, "progress_updated_unix_ms": stamp}
    return None


def collect(root, now, service_reader):
    job = root / JOB
    if not (job / "PLAN.json").exists():
        return None
    rows = read_json(job / "PLAN.json")["epochs"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= 32:
        raise ValueError("invalid_archive_plan")
    plan = {integer(row["epoch"], 100000): integer(row["bytes"]) for row in rows}
    if len(plan) != len(rows) or any(size == 0 for size in plan.values()):
        raise ValueError("invalid_archive_plan")
    status = read_json(job / "STATUS.json")
    state, epoch = status["state"], integer(status["epoch"], 100000)
    if not isinstance(state, str) or not re.fullmatch(r"[A-Z_]{1,32}", state) or epoch not in plan:
        raise ValueError("invalid_archive_state")
    start = integer(status["updated_unix_ms"])
    result = {"state": state, "epoch": epoch, "updated_unix_ms": now, "stage_updated_unix_ms": start,
              "progress_updated_unix_ms": None, "progress_percent": None, "bytes_per_second": None,
              "eta_seconds": None, "eta_scope": None, "paused": None,
              "epoch_eta_min_seconds": None, "epoch_eta_max_seconds": None,
              "queue_eta_min_seconds": None, "queue_eta_max_seconds": None,
              "last_completed_epoch": None, "last_compact_bytes": None,
              "completed_epochs": [], "upload_queue_epochs": []}
    completed, histories = {}, []
    for e, raw_bytes in sorted(plan.items()):
        receipt = job / f"upload-state/epoch-{e}/complete.json"
        if not receipt.exists():
            continue
        row = read_json(receipt)
        if (row.get("schema") != "blockzilla-stored-compact-backup-v1" or row.get("epoch") != e
                or row.get("upload_profile") != "query-with-signatures"
                or row.get("remote_file_sizes_verified") is not True):
            raise ValueError("archive_receipt_identity_invalid")
        selected_bytes = integer(row["bytes"])
        ended = integer(row["completed_unix_seconds"])
        completed[e] = selected_bytes
        control = job / f"epoch-{e}"
        try:
            paths = [control / name for name in ("source.json", "download.json", "REGISTRIES.json", "REGISTRY_INDEX.json", "COMPACTING.json")]
            if any(not stat.S_ISREG(p.lstat().st_mode) for p in paths):
                continue
            points = [p.stat().st_mtime for p in paths] + [ended]
            times = [b - a for a, b in zip(points, points[1:])]
            if all(0 <= value <= 86400 for value in times) and sum(times) >= 60:
                histories.append([value / raw_bytes for value in times])
        except OSError:
            pass
    result["completed_epochs"] = sorted(completed)
    result["upload_queue_epochs"] = sorted(set(plan) - set(completed))
    if completed:
        last = max(completed)
        result.update(last_completed_epoch=last, last_compact_bytes=completed[last])
    try:
        priority = read_json(root / "reports/live-priority-20261012/STATUS.json")
        if -30000 <= now - integer(priority["updated_unix_ms"]) <= 120000:
            result["paused"] = priority["state"] != "BACKGROUND_RUNNING"
    except (OSError, ValueError, KeyError):
        pass
    try:
        active, _ = service_reader(UNIT)
    except Exception:
        active = "unknown"
    if active != "active" and state not in ("COMPLETE", "EPOCH_COMPLETE"):
        result["state"] = "STOPPED_" + state
    if active != "active" or result["paused"] is not False or state not in PHASES:
        return result
    measured = progress(job, epoch, state, start, now)
    if measured:
        result.update(measured)
        result["eta_scope"] = "download" if state == "DOWNLOADING" else "upload" if state == "UPLOADING" else "stage"
    # Status timestamps are stage starts. They do not prove work is advancing.
    # Do not show history estimates after progress stops for over two minutes.
    if not histories or (measured is None and now - start > 120000):
        return result
    histories = histories[-2:]
    low = [min(row[i] for row in histories) * .85 for i in range(5)]
    high = [max(row[i] for row in histories) * 1.2 for i in range(5)]
    phase, size = PHASES[state], plan[epoch]
    elapsed = max(0, (now - start) / 1000)
    current_low, current_high = max(0, low[phase] * size - elapsed), max(0, high[phase] * size - elapsed)
    if measured and measured["eta_seconds"] is not None:
        current_low = measured["eta_seconds"] * .9
        current_high = measured["eta_seconds"] * 1.2 + 30
    epoch_low = current_low + sum(low[phase + 1:]) * size
    epoch_high = current_high + sum(high[phase + 1:]) * size
    later = sum(plan[e] for e in result["upload_queue_epochs"] if e > epoch)
    values = [math.ceil(epoch_low), math.ceil(epoch_high), math.ceil(epoch_low + sum(low) * later), math.ceil(epoch_high + sum(high) * later)]
    if all(0 <= value <= 365 * 86400 for value in values):
        for key, value in zip(("epoch_eta_min_seconds", "epoch_eta_max_seconds", "queue_eta_min_seconds", "queue_eta_max_seconds"), values):
            result[key] = value
    return result
