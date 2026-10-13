#!/usr/bin/env python3
"""Run one worker for each new native capture session. Keep old output intact."""
import argparse
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import time
from pipeline_health import inspect_capture, latest_session

stopping = False
child = None


def stop(_signal, _frame):
    global stopping
    stopping = True
    if child is not None and child.poll() is None:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def run(args):
    global child
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    finished = None
    while not stopping:
        try:
            session = latest_session(args.capture_root)
        except (FileNotFoundError, ValueError):
            time.sleep(1)
            continue
        if session == finished or not (session / "events.sqlite").exists():
            time.sleep(1)
            continue
        try:
            _, problems = inspect_capture(session)
        except (sqlite3.OperationalError, FileNotFoundError):
            # SESSION.json precedes the first committed SQLite schema.
            time.sleep(1)
            continue
        if problems:
            print("Capture is not healthy: " + "; ".join(problems), flush=True)
            return 1
        output = args.output_root / session.name
        print(f"Start compact worker for capture session {session.name}", flush=True)
        command = [str(args.engine), "build-processed" if args.processed else "build", str(session), str(output),
                   "--blockzilla", str(args.blockzilla), "--follow"]
        if not args.processed:
            command += ["--batch-blocks", str(args.batch_blocks)]
        child = subprocess.Popen(command, start_new_session=True)
        result = child.wait()
        if stopping:
            return 0
        if result:
            # A signal can interrupt a safe pending batch. A source error must stop.
            if result < 0:
                print(f"Worker stopped by signal {-result}; pending source is preserved", flush=True)
                return 75
            return result
        finished = session
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--capture-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--engine", type=Path, required=True)
    parser.add_argument("--blockzilla", type=Path, required=True)
    parser.add_argument("--processed", action="store_true")
    parser.add_argument("--batch-blocks", type=int, default=32)
    raise SystemExit(run(parser.parse_args()))
