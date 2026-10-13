#!/usr/bin/env python3
"""Compare SDK output with local RPC. No cryptographic verification or service changes."""
import argparse
import json
from pathlib import Path
import urllib.request

def rpc(slot):
    body = json.dumps({"jsonrpc":"2.0","id":1,"method":"getBlock","params":[slot,{
        "commitment":"finalized","encoding":"json","transactionDetails":"full","rewards":False,
        "maxSupportedTransactionVersion":1}]}).encode()
    with urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:8899",body,{"Content-Type":"application/json"}),timeout=30) as r:
        value=json.load(r)
    if "error" in value: raise RuntimeError(value["error"])
    assert value.get("result") is not None
    return value["result"]

def main():
    p=argparse.ArgumentParser();p.add_argument("input",type=Path);p.add_argument("report",type=Path);a=p.parse_args()
    data=json.loads(a.input.read_text());program=data["program"]
    program_by_slot={b["slot"]:b for b in data["programMatches"]};account_by_slot={b["slot"]:b for b in data["accountMatches"]}
    blocks=transactions=program_matches=account_matches=failed=0
    for b in data["all"]:
        expected=rpc(b["slot"])
        assert b["blockhash"]==expected["blockhash"] and b["parentBlockhash"]==expected["previousBlockhash"]
        assert b["parentSlot"]==expected["parentSlot"] and b["transactionCount"]==len(expected["transactions"])
        assert len(b["transactions"])==len(expected["transactions"])
        want_program=[];want_account=[]
        for i,(got,row) in enumerate(zip(b["transactions"],expected["transactions"])):
            tx=row["transaction"];m=row["meta"];msg=tx["message"]
            accounts=msg["accountKeys"]+m.get("loadedAddresses",{}).get("writable",[])+m.get("loadedAddresses",{}).get("readonly",[])
            top=sorted(set(accounts[ix["programIdIndex"]] for ix in msg["instructions"]))
            inner=sorted(set(accounts[ix["programIdIndex"]] for g in (m.get("innerInstructions") or []) for ix in g["instructions"]))
            assert got["index"]==i and got["signature"]==tx["signatures"][0] and got["signatures"]==tx["signatures"]
            assert got["success"]==(m["err"] is None)
            assert sorted(got["topLevelPrograms"])==top and sorted(got["innerPrograms"])==inner
            if program in top or program in inner:want_program.append(got["signature"])
            if program in accounts:want_account.append(got["signature"])
            failed+=int(m["err"] is not None);transactions+=1
        assert [t["signature"] for t in program_by_slot[b["slot"]]["transactions"]]==want_program
        assert [t["signature"] for t in account_by_slot[b["slot"]]["transactions"]]==want_account
        blocks+=1;program_matches+=len(want_program);account_matches+=len(want_account)
    result={"schema":"hivezilla-stream-sdk-rpc-oracle-v1","state":"PASS","blocks":blocks,"transactions":transactions,
        "program_matches":program_matches,"account_matches":account_matches,"failed_transactions":failed,
        "source":"local finalized Agave getBlock","checks":["block and parent hashes","all primary and additional signatures",
        "execution success","top-level program references","recorded CPI programs","loaded address account filters","selected transaction order"],
        "cryptographic_verification":False,"validator_changed":False}
    a.report.write_text(json.dumps(result,indent=2)+"\n");print(json.dumps(result))

if __name__=="__main__":main()
