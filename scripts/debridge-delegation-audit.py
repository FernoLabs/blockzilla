#!/usr/bin/env python3
"""Read saved deBridge delegate RPC results, or explicitly scan the current RPC.

Offline: --input-response FILE --input-response FILE --out-dir NEW_DIRECTORY
Live: --rpc-url URL --scan-current-token-accounts --out-dir NEW_DIRECTORY
Candidates: --rpc-url URL --candidate-accounts FILE --out-dir NEW_DIRECTORY
Saved JSON can be {query: RPC_REQUEST, response: RPC_JSON, seconds: NUMBER}.
Use classic=FILE or token2022=FILE for plain responses with no account rows.
Offline mode makes no network calls. Candidate mode reads batches of 100 known
addresses. Global live mode needs --scan-current-token-accounts and makes two
serial read-only calls. These can cause full token account scans.
"""
import argparse
import base64
import csv
import hashlib
import json
from pathlib import Path
import sys
import time
import urllib.request

INTENTS_PROGRAM = "DeintXmrdZFgSKzEWcJWhK793HuqnfDydAPvHmayY5Xz"
STATE_DELEGATE = "73dUbDAWUDZRfXM3DcuRgR4q74nr97MyuBt95738JnMz"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
PROGRAMS = {"classic": TOKEN_PROGRAM, "token2022": TOKEN_2022_PROGRAM}
MAX_U64 = 2**64 - 1
BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
CSV_FIELDS = ("owner", "account", "mint", "program", "delegate", "balance_raw",
              "delegated_amount_raw", "spendable_upper_bound_raw", "state", "decimals", "query_slot")


class AuditError(Exception):
    pass


def b58encode(data):
    number, encoded = int.from_bytes(data, "big"), ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = BASE58[remainder] + encoded
    return "1" * (len(data) - len(data.lstrip(b"\0"))) + encoded


def candidate_entry(key, account, delegate, slot):
    """Read only the base token layout; account type separates Token-2022 mints."""
    if account is None:
        return "closed_or_missing", None
    program = account.get("owner")
    if program not in PROGRAMS.values():
        return "other_owner_program", None
    data = account.get("data")
    if not isinstance(data, list) or len(data) != 2 or data[1] != "base64":
        raise AuditError("Candidate account is not base64 data")
    raw = base64.b64decode(data[0], validate=True)
    if account.get("executable") is not False or len(raw) not in (165, 166):
        raise AuditError("Candidate token account has invalid base data")
    if program == TOKEN_PROGRAM and len(raw) != 165:
        raise AuditError("Classic token account must have 165 bytes")
    if program == TOKEN_2022_PROGRAM and len(raw) == 166 and raw[165] != 2:
        raise AuditError("Token-2022 candidate is not an Account")
    space = account.get("space")
    if space is not None and (not isinstance(space, int) or isinstance(space, bool)
                              or space < len(raw) or (len(raw) == 165 and space != 165)):
        raise AuditError("Candidate full size conflicts with the returned account slice")
    option = int.from_bytes(raw[72:76], "little")
    if option == 0:
        return "delegate_absent", None
    if option != 1:
        raise AuditError("Candidate delegate option is invalid")
    actual_delegate = b58encode(raw[76:108])
    if actual_delegate != delegate:
        return "other_delegate", None
    state = {1: "initialized", 2: "frozen"}.get(raw[108])
    if state is None:
        raise AuditError("Delegated candidate is not initialized")
    balance = int.from_bytes(raw[64:72], "little")
    allowance = int.from_bytes(raw[121:129], "little")
    row = {"owner": b58encode(raw[32:64]), "account": key, "mint": b58encode(raw[:32]),
           "program": program, "delegate": actual_delegate, "balance_raw": str(balance),
           "delegated_amount_raw": str(allowance), "spendable_upper_bound_raw": str(min(balance, allowance)),
           "state": state, "decimals": None, "query_slot": slot}
    return "selected_delegate", row


def mint_decimals(account, expected_program):
    if account is None:
        raise AuditError("Mint account is missing; decimals are unavailable")
    if account.get("owner") != expected_program or account.get("executable") is not False:
        raise AuditError("Mint account owner or executable flag is invalid")
    data = account.get("data")
    if not isinstance(data, list) or len(data) != 2 or data[1] != "base64":
        raise AuditError("Mint data is not a base64 pair")
    raw = base64.b64decode(data[0], validate=True)
    if len(raw) != 82 or raw[45] != 1:
        raise AuditError("Mint base data is incomplete or uninitialized")
    if int.from_bytes(raw[:4], "little") not in (0, 1) or int.from_bytes(raw[46:50], "little") not in (0, 1):
        raise AuditError("Mint authority options are invalid")
    if expected_program == TOKEN_PROGRAM and account.get("space", 82) != 82:
        raise AuditError("Classic mint must have 82 bytes")
    return raw[44]


def format_units(raw_amount, decimals):
    if decimals is None:
        return None
    text = str(raw_amount)
    if not decimals:
        return text
    text = text.rjust(decimals + 1, "0")
    return (text[:-decimals] + "." + text[-decimals:]).rstrip("0").rstrip(".")


def read_candidate_keys(path, raw=None):
    text = (path.read_bytes() if raw is None else raw).decode("utf-8")
    rows = []
    if path.suffix in (".csv", ".tsv"):
        rows = list(csv.DictReader(text.splitlines(), delimiter="\t" if path.suffix == ".tsv" else ","))
    elif text.lstrip().startswith("["):
        rows = json.loads(text)
    else:
        rows = [json.loads(line) if line.lstrip().startswith("{") else line.strip() for line in text.splitlines() if line.strip()]
    keys = set()
    for row in rows:
        key = row if isinstance(row, str) else next((row.get(field) for field in ("token_account", "account", "source_token_account", "source") if row.get(field)), None)
        if not isinstance(key, str) or len(key) > 44 or len(key) < 32 or any(c not in BASE58 for c in key):
            raise AuditError("Candidate input has an invalid account address")
        keys.add(key)
    return sorted(keys)


def run_candidates(args):
    output = args.out_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise AuditError("Use a new empty output directory")
    candidate_input = args.candidate_accounts.read_bytes()
    keys = read_candidate_keys(args.candidate_accounts, candidate_input)
    input_snapshot = "candidate-input" + (args.candidate_accounts.suffix or ".txt")
    (output / input_snapshot).write_bytes(candidate_input)
    input_sha256 = hashlib.sha256(candidate_input).hexdigest()
    started, rows, statuses, receipts, errors = time.time(), [], [], [], []
    minimum_slot = None
    for start in range(0, len(keys), 100):
        chunk = keys[start:start + 100]
        config = {"encoding": "base64", "commitment": "finalized", "dataSlice": {"offset": 0, "length": 166}}
        if minimum_slot is not None:
            config["minContextSlot"] = minimum_slot
        query = {"jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts", "params": [chunk, config]}
        receipt = {"batch": start // 100, "query": query, "state": "STARTED"}
        receipts.append(receipt)
        batch_started = time.time()
        try:
            request = urllib.request.Request(args.rpc_url, data=json.dumps(query).encode(), headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=args.timeout_seconds) as response:
                raw = response.read()
            path = output / ("candidates-" + str(start // 100) + ".response.json")
            path.write_bytes(raw)
            receipt.update(response_file=path.name, response_sha256=hashlib.sha256(raw).hexdigest(), response_bytes=len(raw))
            response = json.loads(raw)
            if "error" in response:
                raise AuditError("RPC error: " + json.dumps(response["error"]))
            result = response.get("result", {})
            slot, values = result.get("context", {}).get("slot"), result.get("value")
            if not isinstance(slot, int) or isinstance(slot, bool) or slot < 0 or not isinstance(values, list) or len(values) != len(chunk):
                raise AuditError("Invalid candidate batch result")
            if minimum_slot is not None and slot < minimum_slot:
                raise AuditError("Candidate context is below minContextSlot")
            batch_rows, batch_statuses = [], []
            for key, account in zip(chunk, values):
                status, row = candidate_entry(key, account, args.delegate, slot)
                batch_statuses.append({"account": key, "status": status, "query_slot": slot})
                if row:
                    batch_rows.append(row)
            rows.extend(batch_rows)
            statuses.extend(batch_statuses)
            minimum_slot = slot
            receipt.update(state="COMPLETE", slot=slot)
        except Exception as exc:
            receipt.update(state="FAILED", error=str(exc))
            errors.append({"batch": start // 100, "error": str(exc)})
            break
        finally:
            receipt["seconds"] = time.time() - batch_started
            save_json(output / "rpc-receipts.json", receipts)
    complete = not errors and len(statuses) == len(keys)
    if complete:
        mint_programs = {}
        for row in rows:
            if row["mint"] in mint_programs and mint_programs[row["mint"]] != row["program"]:
                errors.append({"mint": row["mint"], "error": "Mint has conflicting token programs"})
            mint_programs[row["mint"]] = row["program"]
        mints = sorted(mint_programs)
        for start in range(0, len(mints), 100):
            if errors:
                break
            chunk = mints[start:start + 100]
            config = {"encoding": "base64", "commitment": "finalized", "dataSlice": {"offset": 0, "length": 82},
                      "minContextSlot": minimum_slot}
            query = {"jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts", "params": [chunk, config]}
            receipt = {"batch": "mints-" + str(start // 100), "query": query, "state": "STARTED"}
            receipts.append(receipt)
            batch_started = time.time()
            try:
                request = urllib.request.Request(args.rpc_url, data=json.dumps(query).encode(), headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(request, timeout=args.timeout_seconds) as response:
                    raw = response.read()
                path = output / ("mints-" + str(start // 100) + ".response.json")
                path.write_bytes(raw)
                receipt.update(response_file=path.name, response_sha256=hashlib.sha256(raw).hexdigest(), response_bytes=len(raw))
                response = json.loads(raw)
                if "error" in response:
                    raise AuditError("RPC error: " + json.dumps(response["error"]))
                result = response.get("result", {})
                slot, values = result.get("context", {}).get("slot"), result.get("value")
                if not isinstance(slot, int) or isinstance(slot, bool) or slot < minimum_slot or not isinstance(values, list) or len(values) != len(chunk):
                    raise AuditError("Invalid mint batch result")
                decimals = {key: mint_decimals(account, mint_programs[key]) for key, account in zip(chunk, values)}
                for row in rows:
                    if row["mint"] in decimals:
                        row["decimals"] = decimals[row["mint"]]
                minimum_slot = slot
                receipt.update(state="COMPLETE", slot=slot)
            except Exception as exc:
                receipt.update(state="FAILED", error=str(exc))
                errors.append({"batch": receipt["batch"], "error": str(exc)})
            finally:
                receipt["seconds"] = time.time() - batch_started
                save_json(output / "rpc-receipts.json", receipts)
        complete = not errors
    csv_name = "delegations.csv" if complete else "delegations.partial.csv"
    with (output / csv_name).open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda row: (row["owner"], row["mint"], row["account"])))
    with (output / "candidate-status.csv").open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=("account", "status", "query_slot"))
        writer.writeheader()
        writer.writerows(statuses)
    summary = {"schema": "debridge-historical-candidate-current-state-v1", "state": "COMPLETE" if complete else "FAILED_PARTIAL",
               "complete": complete, "delegate": args.delegate, "totals_scope": "provided_historical_candidates",
               "all_wallet_coverage_proven": False, "atomic_snapshot": False, "candidate_accounts": len(keys),
               "checked_candidate_accounts": len(statuses), "totals": count_rows(rows) if complete else None,
               "input_file_sha256": input_sha256, "input_snapshot": input_snapshot,
               "candidate_status_counts": {status: sum(r["status"] == status for r in statuses) for status in sorted({r["status"] for r in statuses})},
               "query_slots": [{"batch": r["batch"], "slot": r.get("slot"), "state": r["state"]} for r in receipts],
               "csv": csv_name, "errors": errors, "elapsed_seconds": time.time() - started,
               "coverage": "Only the supplied historical approval candidates. Historical scan completeness must be checked separately.",
               "decimals_missing_mints": sorted({row["mint"] for row in rows if row["decimals"] is None}),
               "limits": ["Current account and mint queries have separate finalized slots.", "Decimals come from direct mint base data.",
                          "Formatted token units use base mint decimals; extension-dependent UI conversions are not applied.",
                          "Spendable values are upper bounds; frozen state and Token-2022 extensions can restrict transfers.",
                          "Missing current accounts are closed or missing; these states are not distinguished."]}
    save_json(output / "summary.json", summary)
    print(json.dumps({"state": summary["state"], "totals": summary["totals"], "summary": str(output / "summary.json")}))
    return 0 if complete else 1


def raw_amount(value):
    if not isinstance(value, str) or not value.isascii() or not value.isdigit():
        raise AuditError("Raw amount is not unsigned decimal text")
    number = int(value)
    if number > MAX_U64:
        raise AuditError("Raw amount exceeds UInt64")
    return number


def decode_token_account(entry, program, delegate, slot):
    account = entry.get("account", {})
    if account.get("owner") != program or account.get("executable") is not False:
        raise AuditError("Invalid token account owner or executable flag")
    data = account.get("data", {})
    parsed = data.get("parsed", {}) if isinstance(data, dict) else {}
    if parsed.get("type") != "account":
        raise AuditError("Result is not a parsed token account")
    space = data.get("space", account.get("space"))
    if not isinstance(space, int) or space < 165 or (program == TOKEN_PROGRAM and space != 165):
        raise AuditError("Invalid token account size")
    info = parsed.get("info", {})
    if info.get("delegate") != delegate:
        return None
    balance = raw_amount(info.get("tokenAmount", {}).get("amount"))
    allowance = raw_amount(info.get("delegatedAmount", {}).get("amount"))
    decimals = info.get("tokenAmount", {}).get("decimals")
    if not isinstance(decimals, int) or isinstance(decimals, bool) or not 0 <= decimals <= 255:
        raise AuditError("Invalid token decimals")
    if info.get("delegatedAmount", {}).get("decimals") != decimals:
        raise AuditError("Balance and allowance decimals differ")
    if info.get("state") not in ("initialized", "frozen"):
        raise AuditError("Delegated token account is not initialized")
    for field in ("mint", "owner"):
        if not isinstance(info.get(field), str):
            raise AuditError("Missing token account key: " + field)
    if not isinstance(entry.get("pubkey"), str):
        raise AuditError("Missing token account address")
    return {"owner": info["owner"], "account": entry["pubkey"], "mint": info["mint"],
            "program": program, "delegate": delegate, "balance_raw": str(balance),
            "delegated_amount_raw": str(allowance), "spendable_upper_bound_raw": str(min(balance, allowance)),
            "state": info["state"], "decimals": decimals, "query_slot": slot}


def count_rows(rows):
    positive = [r for r in rows if int(r["delegated_amount_raw"]) > 0]
    funded = [r for r in positive if int(r["balance_raw"]) > 0]
    result = {"token_accounts": len(rows), "distinct_owners": len({r["owner"] for r in rows}),
              "positive_allowance_accounts": len(positive),
              "distinct_owners_positive_allowance": len({r["owner"] for r in positive}),
              "positive_balance_accounts": sum(int(r["balance_raw"]) > 0 for r in rows),
              "positive_balance_and_allowance_accounts": len(funded),
              "distinct_owners_positive_balance_and_allowance": len({r["owner"] for r in funded}),
              "exact_u64_max_allowance_accounts": sum(int(r["delegated_amount_raw"]) == MAX_U64 for r in rows),
              "allowance_ge_2pow63_accounts": sum(int(r["delegated_amount_raw"]) >= 2**63 for r in rows),
              "frozen_accounts": sum(r["state"] == "frozen" for r in rows)}
    groups = {}
    for row in rows:
        groups.setdefault((row["program"], row["mint"]), []).append(row)
    result["per_mint"] = []
    for (program, mint), group in sorted(groups.items()):
        if len({r["decimals"] for r in group}) != 1:
            raise AuditError("Inconsistent mint decimals")
        item = {"program": program, "mint": mint, "decimals": group[0]["decimals"],
                "token_accounts": len(group), "distinct_owners": len({r["owner"] for r in group})}
        for field in ("balance_raw", "delegated_amount_raw", "spendable_upper_bound_raw"):
            item[field] = str(sum(int(r[field]) for r in group))
            item[field.replace("_raw", "_units")] = format_units(item[field], item["decimals"])
        result["per_mint"].append(item)
    return result


def save_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def read_saved(spec):
    label, separator, path = spec.partition("=")
    program = PROGRAMS.get(label) if separator else None
    if program is None:
        path = spec
    source = Path(path)
    raw = source.read_bytes()
    envelope = json.loads(raw)
    receipt = {"source_file": str(source.resolve()), "source_bytes": len(raw),
               "source_file_sha256": hashlib.sha256(raw).hexdigest()}
    for field in ("started_unix", "finished_unix", "seconds"):
        if field in envelope:
            receipt[field] = envelope[field]
    query, response = envelope.get("query"), envelope.get("response", envelope)
    if query is not None:
        params = query.get("params", [])
        if query.get("method") != "getTokenAccountsByDelegate" or len(params) != 3:
            raise AuditError("Saved query is not getTokenAccountsByDelegate")
        requested = params[1].get("programId")
        if program is not None and program != requested:
            raise AuditError("Input label and query program differ")
        program = requested
        receipt["query"] = query
    if program is None:
        values = response.get("result", {}).get("value", [])
        if values:
            program = values[0].get("account", {}).get("owner")
    if program not in PROGRAMS.values():
        raise AuditError("Cannot identify token program; use classic=FILE or token2022=FILE")
    receipt["response_canonical_json_sha256"] = hashlib.sha256(
        json.dumps(response, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return program, query, response, receipt


def live_response(args, label, program, output):
    query = {"jsonrpc": "2.0", "id": 1, "method": "getTokenAccountsByDelegate",
             "params": [args.delegate, {"programId": program}, {"encoding": "jsonParsed", "commitment": "finalized"}]}
    path = output / (label + ".response.json")
    if path.exists():
        raise AuditError("Refuse to replace an existing response file")
    print("Read " + label + " delegation accounts", file=sys.stderr, flush=True)
    started = time.time()
    request = urllib.request.Request(args.rpc_url, data=json.dumps(query).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=args.timeout_seconds) as response:
        raw = response.read()
    finished = time.time()
    path.write_bytes(raw)
    receipt = {"query": query, "source_file": str(path), "response_bytes": len(raw),
               "response_sha256": hashlib.sha256(raw).hexdigest(), "started_unix": started,
               "finished_unix": finished, "seconds": finished - started}
    return query, json.loads(raw), receipt


def parse_response(program, query, response, receipt, delegate):
    receipt["program"] = program
    if query is not None:
        if query["params"][0] != delegate or query["params"][2].get("commitment") != "finalized":
            raise AuditError("Saved query has a different delegate or commitment")
    else:
        receipt["commitment_verified"] = False
    if "error" in response:
        raise AuditError("RPC error: " + json.dumps(response["error"]))
    result = response.get("result", {})
    slot, entries = result.get("context", {}).get("slot"), result.get("value")
    if not isinstance(slot, int) or isinstance(slot, bool) or not isinstance(entries, list):
        raise AuditError("RPC response has no context slot and account list")
    receipt["slot"] = slot
    rows, seen = [], set()
    for entry in entries:
        row = decode_token_account(entry, program, delegate, slot)
        if row is None:
            raise AuditError("Delegate query returned an unrelated or revoked account")
        if row["account"] in seen:
            raise AuditError("Delegate query repeated a token account")
        seen.add(row["account"])
        rows.append(row)
    receipt["state"] = "COMPLETE"
    return rows


def run(args):
    output = args.out_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise AuditError("Use a new empty output directory")
    started, receipts, errors, rows, completed = time.time(), [], [], [], set()
    work = []
    for spec in args.input_response:
        try:
            work.append(read_saved(spec))
        except Exception as exc:
            errors.append({"input": spec, "error": str(exc)})
    for program, query, response, receipt in work:
        receipts.append(receipt)
        try:
            if program in completed:
                raise AuditError("Multiple responses supplied for one token program")
            rows.extend(parse_response(program, query, response, receipt, args.delegate))
            completed.add(program)
        except Exception as exc:
            receipt.update(state="FAILED", error=str(exc))
            errors.append({"program": program, "error": str(exc)})
    if args.scan_current_token_accounts:
        for label, program in PROGRAMS.items():
            if program in completed:
                continue
            receipt = {"program": program, "state": "STARTED"}
            receipts.append(receipt)
            try:
                query, response, details = live_response(args, label, program, output)
                receipt.update(details)
                rows.extend(parse_response(program, query, response, receipt, args.delegate))
                completed.add(program)
            except Exception as exc:
                receipt.update(state="FAILED", error=str(exc))
                errors.append({"program": program, "error": str(exc)})
    for program in sorted(set(PROGRAMS.values()) - completed):
        errors.append({"program": program, "error": "No complete valid response"})
    complete = not errors
    csv_name = "delegations.csv" if complete else "delegations.partial.csv"
    with (output / csv_name).open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda row: (row["owner"], row["mint"], row["account"])))
    summary = {"schema": "debridge-current-delegation-audit-v1", "state": "COMPLETE" if complete else "FAILED_PARTIAL",
               "complete": complete, "delegate": args.delegate, "intents_program": INTENTS_PROGRAM,
               "atomic_snapshot": False, "totals": count_rows(rows) if complete else None,
               "per_program": {program: count_rows([r for r in rows if r["program"] == program]) for program in sorted(completed)},
               "query_slots": [{"program": r.get("program"), "slot": r.get("slot"), "state": r.get("state")} for r in receipts],
               "csv": csv_name, "errors": errors, "elapsed_seconds": time.time() - started,
               "coverage": "The exact selected deBridge intents State delegate. Coverage of all possible deBridge delegates or unallocated PDAs is not proved.",
               "limits": ["Separate finalized RPC slots; not an atomic snapshot.", "Owner addresses are distinct token owners, not people.",
                          "Spendable values are upper bounds; frozen state and Token-2022 extensions can restrict transfers.",
                          "Mint-level permanent delegates are outside this query.", "Raw amounts from different mints are not added together.",
                          "Allowance thresholds are literal values; they do not classify an allowance as unlimited.",
                          "Current state does not give the approval transaction or date."]}
    save_json(output / "rpc-receipts.json", receipts)
    save_json(output / "summary.json", summary)
    print(json.dumps({"state": summary["state"], "totals": summary["totals"], "summary": str(output / "summary.json")}))
    return 0 if complete else 1


def self_test():
    import io
    import tempfile
    import unittest
    from unittest import mock

    class Tests(unittest.TestCase):
        def entry(self, key, owner, balance, allowance, delegate=STATE_DELEGATE):
            return {"pubkey": key, "account": {"owner": TOKEN_PROGRAM, "executable": False,
                    "data": {"space": 165, "parsed": {"type": "account", "info": {
                        "mint": "mint", "owner": owner, "delegate": delegate,
                        "tokenAmount": {"amount": str(balance), "decimals": 6},
                        "delegatedAmount": {"amount": str(allowance), "decimals": 6}, "state": "initialized"}}}}}

        def test_counts_and_exact_large_amounts(self):
            entries = [self.entry("a", "one", 500, MAX_U64 - 100), self.entry("b", "one", 0, MAX_U64), self.entry("c", "two", 20, 0)]
            result = count_rows([decode_token_account(e, TOKEN_PROGRAM, STATE_DELEGATE, 123) for e in entries])
            self.assertEqual((result["token_accounts"], result["distinct_owners"]), (3, 2))
            self.assertEqual(result["distinct_owners_positive_allowance"], 1)
            self.assertEqual(result["distinct_owners_positive_balance_and_allowance"], 1)
            self.assertEqual(result["exact_u64_max_allowance_accounts"], 1)
            self.assertEqual(result["allowance_ge_2pow63_accounts"], 2)
            self.assertEqual(result["per_mint"][0]["delegated_amount_raw"], str(2 * MAX_U64 - 100))
            self.assertEqual(result["per_mint"][0]["spendable_upper_bound_raw"], "500")

        def test_revoked(self):
            self.assertIsNone(decode_token_account(self.entry("a", "one", 20, 100, None), TOKEN_PROGRAM, STATE_DELEGATE, 123))

        def test_targeted_candidate_base_layout_and_removed_delegate(self):
            delegate_number = 0
            for char in STATE_DELEGATE:
                delegate_number = delegate_number * 58 + BASE58.index(char)
            raw = bytearray(165)
            raw[:32], raw[32:64] = bytes([9]) * 32, bytes([7]) * 32
            raw[64:72], raw[72:76] = (42).to_bytes(8, "little"), (1).to_bytes(4, "little")
            raw[76:108], raw[108] = delegate_number.to_bytes(32, "big"), 1
            raw[121:129] = (MAX_U64 - 50).to_bytes(8, "little")
            account = {"owner": TOKEN_PROGRAM, "executable": False,
                       "data": [base64.b64encode(raw).decode(), "base64"]}
            status, row = candidate_entry("candidate", account, STATE_DELEGATE, 123)
            self.assertEqual(status, "selected_delegate")
            self.assertEqual(row["balance_raw"], "42")
            self.assertEqual(row["delegated_amount_raw"], str(MAX_U64 - 50))
            self.assertIsNone(row["decimals"])
            raw[72:76] = bytes(4)
            account["data"][0] = base64.b64encode(raw).decode()
            self.assertEqual(candidate_entry("candidate", account, STATE_DELEGATE, 123), ("delegate_absent", None))
            self.assertEqual(candidate_entry("candidate", None, STATE_DELEGATE, 123), ("closed_or_missing", None))
            account["owner"] = TOKEN_2022_PROGRAM
            raw.extend(b"\1")
            account["data"][0] = base64.b64encode(raw).decode()
            with self.assertRaises(AuditError):
                candidate_entry("candidate", account, STATE_DELEGATE, 123)
            raw[165] = 2
            account["data"][0] = base64.b64encode(raw).decode()
            self.assertEqual(candidate_entry("candidate", account, STATE_DELEGATE, 123), ("delegate_absent", None))
            account["space"] = 165
            with self.assertRaises(AuditError):
                candidate_entry("candidate", account, STATE_DELEGATE, 123)

        def test_frozen_amount_is_upper_bound_and_zero_allowance_has_no_exposure(self):
            entry = self.entry("a", "one", 30, 20)
            entry["account"]["data"]["parsed"]["info"]["state"] = "frozen"
            row = decode_token_account(entry, TOKEN_PROGRAM, STATE_DELEGATE, 123)
            self.assertEqual(row["spendable_upper_bound_raw"], "20")
            result = count_rows([row])
            self.assertEqual(result["frozen_accounts"], 1)
            self.assertEqual(result["distinct_owners_positive_allowance"], 1)
            zero = decode_token_account(self.entry("b", "two", 50, 0), TOKEN_PROGRAM, STATE_DELEGATE, 123)
            self.assertEqual(zero["spendable_upper_bound_raw"], "0")
            self.assertEqual(count_rows([zero])["distinct_owners_positive_allowance"], 0)

        def test_mint_decimals_and_exact_formatting(self):
            raw = bytearray(82)
            raw[44], raw[45] = 6, 1
            account = {"owner": TOKEN_PROGRAM, "executable": False,
                       "data": [base64.b64encode(raw).decode(), "base64"]}
            self.assertEqual(mint_decimals(account, TOKEN_PROGRAM), 6)
            self.assertEqual(format_units(str(MAX_U64), 6), "18446744073709.551615")
            self.assertEqual(format_units("1", 6), "0.000001")
            self.assertEqual(format_units("1000000", 6), "1")
            self.assertIsNone(format_units("1", None))
            with self.assertRaises(AuditError):
                mint_decimals(None, TOKEN_PROGRAM)

        def candidate_rpc_fixture(self):
            delegate_number = 0
            for char in STATE_DELEGATE:
                delegate_number = delegate_number * 58 + BASE58.index(char)
            raw = bytearray(165)
            raw[:32], raw[32:64] = bytes([9]) * 32, bytes([7]) * 32
            raw[64:72], raw[72:76] = (1500000).to_bytes(8, "little"), (1).to_bytes(4, "little")
            raw[76:108], raw[108] = delegate_number.to_bytes(32, "big"), 1
            raw[121:129] = MAX_U64.to_bytes(8, "little")
            token = {"owner": TOKEN_PROGRAM, "executable": False, "data": [base64.b64encode(raw).decode(), "base64"]}
            mint = bytearray(82)
            mint[44], mint[45] = 6, 1
            mint_account = {"owner": TOKEN_PROGRAM, "executable": False,
                            "data": [base64.b64encode(mint).decode(), "base64"]}
            return ({"result": {"context": {"slot": 123}, "value": [token]}},
                    {"result": {"context": {"slot": 124}, "value": [mint_account]}})

        def test_candidate_success_pins_input_and_uses_only_direct_reads(self):
            responses = list(self.candidate_rpc_fixture())
            requests = []
            with tempfile.TemporaryDirectory() as folder:
                source = Path(folder) / "candidates.txt"
                original = (b58encode(bytes([1]) * 32) + "\n").encode()
                source.write_bytes(original)
                args = argparse.Namespace(delegate=STATE_DELEGATE, out_dir=Path(folder) / "out",
                                          candidate_accounts=source, rpc_url="http://fixture.invalid", timeout_seconds=1)

                def reply(request, timeout):
                    requests.append(json.loads(request.data))
                    source.write_text("changed while reading\n")
                    return io.BytesIO(json.dumps(responses.pop(0)).encode())

                with mock.patch.object(urllib.request, "urlopen", side_effect=reply):
                    self.assertEqual(run_candidates(args), 0)
                summary = json.loads((args.out_dir / "summary.json").read_text())
                self.assertEqual(summary["input_file_sha256"], hashlib.sha256(original).hexdigest())
                self.assertEqual((args.out_dir / summary["input_snapshot"]).read_bytes(), original)
                self.assertEqual(summary["totals"]["per_mint"][0]["balance_units"], "1.5")
                self.assertEqual(summary["totals"]["per_mint"][0]["delegated_amount_units"], "18446744073709.551615")
                self.assertEqual([request["method"] for request in requests], ["getMultipleAccounts"] * 2)
                self.assertEqual(requests[1]["params"][1]["minContextSlot"], 123)

        def test_failed_candidate_mint_read_has_partial_csv_and_no_total(self):
            responses = [self.candidate_rpc_fixture()[0], {"error": {"code": -1, "message": "test failure"}}]
            with tempfile.TemporaryDirectory() as folder:
                source = Path(folder) / "candidates.txt"
                source.write_text(b58encode(bytes([1]) * 32) + "\n")
                args = argparse.Namespace(delegate=STATE_DELEGATE, out_dir=Path(folder) / "out",
                                          candidate_accounts=source, rpc_url="http://fixture.invalid", timeout_seconds=1)
                with mock.patch.object(urllib.request, "urlopen", side_effect=lambda request, timeout: io.BytesIO(json.dumps(responses.pop(0)).encode())):
                    self.assertEqual(run_candidates(args), 1)
                summary = json.loads((args.out_dir / "summary.json").read_text())
                self.assertIsNone(summary["totals"])
                self.assertEqual(summary["state"], "FAILED_PARTIAL")
                self.assertEqual(summary["checked_candidate_accounts"], 1)
                self.assertEqual(len(summary["decimals_missing_mints"]), 1)
                with (args.out_dir / summary["csv"]).open() as file:
                    rows = list(csv.DictReader(file))
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["delegated_amount_raw"], str(MAX_U64))
                self.assertEqual(rows[0]["decimals"], "")

        def test_partial_never_reports_zero_total(self):
            with tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "response.json"
                save_json(path, {"query": {"method": "getTokenAccountsByDelegate", "params": [STATE_DELEGATE, {"programId": TOKEN_PROGRAM}, {"commitment": "finalized"}]},
                                 "response": {"result": {"context": {"slot": 123}, "value": []}}})
                args = argparse.Namespace(delegate=STATE_DELEGATE, out_dir=Path(folder) / "out", input_response=[str(path)], scan_current_token_accounts=False)
                self.assertEqual(run(args), 1)
                result = json.loads((args.out_dir / "summary.json").read_text())
                self.assertIsNone(result["totals"])
                self.assertEqual(result["per_program"][TOKEN_PROGRAM]["token_accounts"], 0)

    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    return 0 if result.wasSuccessful() else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rpc-url")
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--delegate", default=STATE_DELEGATE)
    parser.add_argument("--input-response", action="append", default=[])
    parser.add_argument("--candidate-accounts", type=Path, help="Read only supplied historical candidate accounts in batches of 100")
    parser.add_argument("--scan-current-token-accounts", action="store_true")
    parser.add_argument("--timeout-seconds", type=float, default=300)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.out_dir is None:
        parser.error("--out-dir is required")
    if not args.input_response and not args.scan_current_token_accounts and args.candidate_accounts is None:
        parser.error("Provide saved responses or explicitly enable live scanning")
    if args.scan_current_token_accounts and not args.rpc_url:
        parser.error("Live scanning requires --rpc-url")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    if args.candidate_accounts is not None and (not args.rpc_url or args.input_response or args.scan_current_token_accounts):
        parser.error("Candidate mode needs --rpc-url and cannot be combined with other modes")
    try:
        return run_candidates(args) if args.candidate_accounts is not None else run(args)
    except Exception as exc:
        print("Audit failed: " + str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
