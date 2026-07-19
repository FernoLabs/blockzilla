#!/usr/bin/env python3
"""Send one Telegram incident when the durable Blockzilla ACK stops advancing."""

from __future__ import annotations

import json
import os
import re
import signal
import stat
import sys
import time
import urllib.parse
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


MAX_ACK_STATUS_BYTES = 64 * 1024
MAX_STATE_BYTES = 4096
MAX_TOKEN_BYTES = 256
MAX_TELEGRAM_RESPONSE_BYTES = 64 * 1024
TOKEN_RE = re.compile(r"^[0-9]{6,15}:[A-Za-z0-9_-]{30,}$")
CHAT_RE = re.compile(r"^(?:-?[0-9]{1,20}|@[A-Za-z0-9_]{5,32})$")


@dataclass(frozen=True)
class Config:
    ack_status_file: Path
    state_file: Path
    token_file: Path
    chat_id: str
    message_thread_id: int | None
    stale_after_secs: int
    startup_grace_secs: int
    interval_secs: int


@dataclass(frozen=True)
class AckSnapshot:
    through_sequence: int
    updated_unix_secs: int


class StateStore:
    def __init__(self, path: Path):
        self.path = path

    def load(self) -> str:
        try:
            payload = read_bounded_regular(self.path, MAX_STATE_BYTES)
        except FileNotFoundError:
            return "inactive"
        value = json.loads(payload)
        if not isinstance(value, dict) or value.get("schema_version") != 1:
            raise ValueError("unsupported alert state")
        phase = value.get("phase")
        if phase == "opening":
            return "active"
        if phase == "recovery":
            return "inactive"
        if phase not in {"inactive", "active"}:
            raise ValueError("invalid alert phase")
        return phase

    def save(self, phase: str, now: int) -> None:
        if phase not in {"inactive", "opening", "active", "recovery"}:
            raise ValueError("invalid alert phase")
        parent = self.path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent_metadata = parent.lstat()
        if not stat.S_ISDIR(parent_metadata.st_mode) or stat.S_ISLNK(parent_metadata.st_mode):
            raise ValueError("alert state parent is not a real directory")
        payload = json.dumps(
            {"schema_version": 1, "phase": phase, "updated_unix_secs": now},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        temporary = parent / f".{self.path.name}.{os.getpid()}.{time.monotonic_ns()}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(temporary, flags, 0o600)
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


DELIVERED = "delivered"
REJECTED = "rejected"
AMBIGUOUS = "ambiguous"


class TelegramSender:
    def __init__(self, config: Config):
        self.config = config

    def __call__(self, message: str) -> str:
        try:
            token = read_token(self.config.token_file)
            fields: dict[str, str] = {
                "chat_id": self.config.chat_id,
                "text": message,
                "disable_web_page_preview": "true",
            }
            if self.config.message_thread_id is not None:
                fields["message_thread_id"] = str(self.config.message_thread_id)
            request = urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=urllib.parse.urlencode(fields).encode("ascii"),
                method="POST",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            with urllib.request.urlopen(request, timeout=15) as response:
                body = response.read(MAX_TELEGRAM_RESPONSE_BYTES + 1)
            if len(body) > MAX_TELEGRAM_RESPONSE_BYTES:
                raise ValueError("Telegram response is too large")
            result = json.loads(body)
            if isinstance(result, dict) and result.get("ok") is True:
                return DELIVERED
            print("pull_ack_monitor telegram_delivery_rejected", file=sys.stderr)
            return REJECTED
        except urllib.error.HTTPError as error:
            # A completed non-2xx response is a definite rejection, so a later
            # retry cannot duplicate an accepted Telegram message.
            print(
                f"pull_ack_monitor telegram_delivery_rejected http_status={error.code}",
                file=sys.stderr,
            )
            return REJECTED
        except Exception as error:  # The error text can contain the token-bearing URL.
            # A timeout, truncated 2xx response, or local interruption may occur
            # after Telegram accepted the request. Keep the durable transitional
            # phase so restart/retry is at-most-once rather than spammy.
            print(f"pull_ack_monitor telegram_delivery_ambiguous type={type(error).__name__}", file=sys.stderr)
            return AMBIGUOUS


def read_bounded_regular(path: Path, maximum: int) -> bytes:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ValueError(f"{path} is not a regular file")
    if metadata.st_size <= 0 or metadata.st_size > maximum:
        raise ValueError(f"{path} has an invalid size")
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise ValueError(f"{path} changed while opening")
        data = b""
        while len(data) <= maximum:
            chunk = os.read(descriptor, min(8192, maximum + 1 - len(data)))
            if not chunk:
                break
            data += chunk
        if len(data) > maximum:
            raise ValueError(f"{path} exceeds its size limit")
        return data
    finally:
        os.close(descriptor)


def read_token(path: Path) -> str:
    raw = read_bounded_regular(path, MAX_TOKEN_BYTES)
    if b"\r" in raw:
        raise ValueError("Telegram token contains a carriage return")
    token = raw.decode("ascii").removesuffix("\n")
    if "\n" in token or not TOKEN_RE.fullmatch(token):
        raise ValueError("Telegram token has an invalid shape")
    return token


def read_ack_snapshot(path: Path, now: int) -> AckSnapshot | None:
    try:
        value = json.loads(read_bounded_regular(path, MAX_ACK_STATUS_BYTES))
        if not isinstance(value, dict) or value.get("schema_version") != 1:
            return None
        sequence = value.get("through_sequence")
        updated = value.get("updated_unix_secs")
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence < 0
            or not isinstance(updated, int)
            or isinstance(updated, bool)
            or updated <= 0
            or updated > now + 300
        ):
            return None
        return AckSnapshot(sequence, updated)
    except (FileNotFoundError, OSError, UnicodeError, ValueError, json.JSONDecodeError):
        return None


def human_duration(seconds: int) -> str:
    seconds = max(0, seconds)
    if seconds < 60:
        return f"{seconds} seconds"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} minute" + ("" if minutes == 1 else "s")
    hours = minutes // 60
    return f"{hours} hour" + ("" if hours == 1 else "s")


def warning_message(age: int | None) -> str:
    last = "unavailable" if age is None else f"{human_duration(age)} ago"
    return "\n".join(
        (
            "Blockzilla backup - WARNING",
            "Durable receiver confirmations stopped",
            f"Last confirmation: {last}.",
            "source host is keeping every unconfirmed gRPC block.",
            "Action: check live capture first, then the NAS receiver.",
        )
    )


def recovery_message() -> str:
    return "\n".join(
        (
            "Blockzilla backup - RECOVERED",
            "Durable receiver confirmations resumed",
            "source host received a new signed storage ACK.",
            "This confirms backup storage, not indexing.",
            "Action: none.",
        )
    )


def run_once(
    config: Config,
    store: StateStore,
    sender: Callable[[str], str],
    *,
    now: int,
    startup_elapsed: int,
) -> None:
    phase = store.load()
    snapshot = read_ack_snapshot(config.ack_status_file, now)
    age = None if snapshot is None else max(0, now - snapshot.updated_unix_secs)
    stale = snapshot is None or age > config.stale_after_secs

    if stale:
        if startup_elapsed < config.startup_grace_secs or phase == "active":
            return
        store.save("opening", now)
        delivery = sender(warning_message(age))
        if delivery == DELIVERED:
            store.save("active", now)
            print("pull_ack_monitor warning_delivered", file=sys.stderr)
        elif delivery == REJECTED:
            store.save("inactive", now)
        return

    if phase == "active":
        store.save("recovery", now)
        delivery = sender(recovery_message())
        if delivery == DELIVERED:
            store.save("inactive", now)
            print("pull_ack_monitor recovery_delivered", file=sys.stderr)
        elif delivery == REJECTED:
            store.save("active", now)
    elif not config.state_file.exists():
        store.save("inactive", now)


def positive_int(name: str, default: str) -> int:
    raw = os.environ.get(name, default)
    if not raw.isascii() or not raw.isdigit() or int(raw) <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return int(raw)


def load_config() -> Config:
    chat_id = os.environ.get("BLOCKZILLA_TELEGRAM_CHAT_ID", "")
    if not CHAT_RE.fullmatch(chat_id):
        raise ValueError("BLOCKZILLA_TELEGRAM_CHAT_ID has an invalid shape")
    raw_thread = os.environ.get("BLOCKZILLA_TELEGRAM_MESSAGE_THREAD_ID", "")
    thread_id = None
    if raw_thread:
        if not raw_thread.isascii() or not raw_thread.isdigit() or int(raw_thread) <= 0:
            raise ValueError("BLOCKZILLA_TELEGRAM_MESSAGE_THREAD_ID must be positive")
        thread_id = int(raw_thread)
    config = Config(
        ack_status_file=Path(os.environ.get("BLOCKZILLA_PULL_ACK_STATUS_FILE", "/control/pull-ack-status.json")),
        state_file=Path(os.environ.get("BLOCKZILLA_PULL_ACK_ALERT_STATE_FILE", "/alert-state/pull-ack-alert.json")),
        token_file=Path(os.environ.get("BLOCKZILLA_TELEGRAM_BOT_TOKEN_FILE", "/run/secrets/telegram_bot_token")),
        chat_id=chat_id,
        message_thread_id=thread_id,
        stale_after_secs=positive_int("BLOCKZILLA_PULL_ACK_STALE_AFTER_SECS", "300"),
        startup_grace_secs=positive_int("BLOCKZILLA_PULL_ACK_STARTUP_GRACE_SECS", "300"),
        interval_secs=positive_int("BLOCKZILLA_PULL_ACK_MONITOR_INTERVAL_SECS", "30"),
    )
    for path in (config.ack_status_file, config.state_file, config.token_file):
        if not path.is_absolute() or path == Path("/"):
            raise ValueError("monitor paths must be absolute and non-root")
    read_token(config.token_file)
    config.state_file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    return config


def main() -> int:
    try:
        config = load_config()
    except Exception as error:
        print(f"pull_ack_monitor invalid_configuration type={type(error).__name__}", file=sys.stderr)
        return 2
    store = StateStore(config.state_file)
    sender = TelegramSender(config)
    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    started = time.monotonic()
    print(
        "pull_ack_monitor started "
        f"stale_after_secs={config.stale_after_secs} interval_secs={config.interval_secs}",
        file=sys.stderr,
    )
    while not stopping:
        now = int(time.time())
        try:
            run_once(
                config,
                store,
                sender,
                now=now,
                startup_elapsed=int(time.monotonic() - started),
            )
        except Exception as error:
            print(f"pull_ack_monitor check_failed type={type(error).__name__}", file=sys.stderr)
        deadline = time.monotonic() + config.interval_secs
        while not stopping and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))
    print("pull_ack_monitor stopped", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
