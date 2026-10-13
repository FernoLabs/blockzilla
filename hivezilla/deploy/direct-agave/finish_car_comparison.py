#!/usr/bin/env python3
"""One-off controller: bring public CAR records to Foundation and compare there.

Native compact files and inventories never leave Foundation. This controller
does not restart or alter the validator, capture worker, or stream services.
It requires the existing SSH access to both operator-owned machines.
"""
import argparse
import json
from pathlib import Path
import shlex
import subprocess
import time

FOUNDATION = "ubuntu@213.239.141.61"
ARCHIVE = "ach@192.168.1.46"
ROOT = "/home/ubuntu/live/hivezilla-validator-20260930"
STATE = ROOT + "/reports/native-car-1049-comparison-20261006"
CONTROL = "/volume1/blockzilla/jobs/native-car-compare-e1049-20261006"
TOOL = ROOT + "/releases/native-car-compare-20261006/compare_car_epoch.py"


def run(command, timeout=900):
    p = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError(f"command failed ({p.returncode}): {p.stderr[-2000:]} {p.stdout[-2000:]}")
    return p.stdout


def remote(host, code, timeout=70):
    return run(["ssh", "-p", "22", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                host, "python3 -c " + shlex.quote(code)], timeout)


def save(folder, name, value):
    folder.mkdir(parents=True, exist_ok=True)
    temporary = folder / (name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(folder / name)


def progress():
    code = f"""from pathlib import Path
import json,re
root=Path({CONTROL!r})
status=json.loads((root/'status.json').read_text())
result={{'state':status['state'],'epoch':1049}}
for name in ['download','compact']:
 p=root/(name+'.json')
 if p.exists(): result[name]=json.loads(p.read_text())
p=root/'1049-progress.json'
if p.exists():
 try: result['progress']=json.loads(p.read_text())
 except json.JSONDecodeError: pass
p=root/'1049-download.log'
if p.exists():
 with p.open('rb') as f:
  f.seek(max(0,p.stat().st_size-16384));text=f.read().decode(errors='replace')
 rows=[r for r in text.splitlines() if 'ETA:' in r and 'DL:' in r]
 if rows: result['download_sample']=re.sub(r'\\x1b\\[[0-9;]*m','',rows[-1])
if status['state']=='FAIL': result['error']=status.get('error',status.get('errors'))
print(json.dumps(result))
"""
    return json.loads(remote(ARCHIVE, code))


def public_copy(names, destination):
    # Every source in this function is a CAR-derived public-data artifact.
    sources = [ARCHIVE + ":" + CONTROL + "/car-export/" + n for n in names]
    run(["scp", "-q", "-3", "-O", "-P", "22", *sources,
         FOUNDATION + ":" + destination + "/"])


def finish(folder):
    remote(ARCHIVE, f"""from pathlib import Path
import subprocess
root=Path({CONTROL!r})
state=root/'car-export'
if not (state/'EXPORT-CAR.json').exists():
 subprocess.run(['nice','-n','19','python3',str(root/'compare_car_epoch.py'),'export-car',
 '--archive','/volume1/blockzilla/archive/epoch-1049',
 '--predecessor','/volume1/blockzilla/archive/epoch-1048','--source',str(root),'--state',str(state)],check=True)
print('public CAR export ready')
""", timeout=1800)
    remote(FOUNDATION, f"from pathlib import Path;Path({STATE + '/car-export'!r}).mkdir(mode=0o700,exist_ok=True)")
    public_copy(["car-blocks.bin", "EXPORT-CAR.json"], STATE + "/car-export")
    report = json.loads(remote(FOUNDATION, f"""import subprocess
p=subprocess.run(['python3',{TOOL!r},'compare-inventories','--native-export',{STATE + '/export'!r},
 '--car-export',{STATE + '/car-export'!r},'--state',{STATE + '/comparison'!r}],capture_output=True,text=True)
if p.returncode: raise RuntimeError(p.stderr+p.stdout)
print(p.stdout)
""", timeout=300))
    save(folder, "RECEIPT.json", report)
    print(json.dumps({"stage": "FULL_BLOCK_COMPARISON", "state": report["state"],
                      "blocks": report["blocks_checked"], "mismatch_blocks": report["mismatch_blocks"]}), flush=True)
    # The compiled sampler has already been tested on the bounded CAR prefix.
    remote(ARCHIVE, f"""from pathlib import Path
import subprocess
root=Path({CONTROL!r})
out=root/'car-export/METADATA-CAR.json'
if not out.exists():
 subprocess.run(['nice','-n','19',str(root/'metadata-check-v3'),
 str(root/'METADATA-SPEC-CAR.json'),str(out)],check=True)
print('public metadata sample ready')
""", timeout=900)
    public_copy(["METADATA-CAR.json"], STATE + "/car-export")
    sample = json.loads(remote(FOUNDATION, f"""from pathlib import Path
import json
root=Path({STATE!r})
native=json.loads((root/'METADATA-NATIVE.json').read_text())
car=json.loads((root/'car-export/METADATA-CAR.json').read_text())
assert native['state']==car['state']=='COMPLETE'
assert native['schema']==car['schema']=='blockzilla-compact-metadata-sample-v3'
assert native['blocks']==car['blocks'] and native['transactions']==car['transactions']
mismatches=[]
reward_order_differences=0
for a,b in zip(native['rows'],car['rows']):
 assert a['slot']==b['slot'] and a['transactions']==b['transactions']
 fields=sorted(k for k in set(a['fields'])|set(b['fields']) if a['fields'].get(k)!=b['fields'].get(k))
 if fields: mismatches.append({{'slot':a['slot'],'fields':fields}})
 if a['reward_original_order_sha256']!=b['reward_original_order_sha256']: reward_order_differences+=1
non_log_mismatches=[{{'slot':row['slot'],'fields':[f for f in row['fields'] if f!='logs']}} for row in mismatches if any(f!='logs' for f in row['fields'])]
result={{'schema':'hivezilla-native-car-metadata-comparison-v1','epoch':1049,
 'state':'PASSED' if not mismatches else 'FAILED','sample_blocks':native['blocks'],
 'sample_transactions':native['transactions'],'mismatches':mismatches,
 'non_log_mismatches':non_log_mismatches,'non_log_fields_match':not non_log_mismatches,
 'native_log_truncated_transactions':sum(len(row['log_truncated_transactions']) for row in native['rows']),
 'car_log_truncated_transactions':sum(len(row['log_truncated_transactions']) for row in car['rows']),
 'reward_order_differences':reward_order_differences,'reward_values_compared_in_sorted_order':True,
 'logs_rendered_before_comparison':True,
 'whole_epoch_execution_metadata_checked':False,'message_instruction_data_checked':False}}
p=root/'comparison/METADATA-RECEIPT.json'
p.write_text(json.dumps(result,indent=2)+'\\n')
print(json.dumps(result))
""", timeout=300))
    save(folder, "METADATA-RECEIPT.json", sample)
    state = "PASSED" if report["state"] == sample["state"] == "PASSED" else "FAILED"
    if report["state"] == "PASSED" and sample["state"] == "FAILED" and sample["non_log_fields_match"]:
        state = "LOG_CONTENT_DIFFERENCE"
    status = {"state": state,
              "epoch": 1049, "full_block_comparison": report["state"], "metadata_sample": sample["state"],
              "native_data_left_foundation": False}
    save(folder, "STATUS.json", status)
    print(json.dumps(status), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.once:
        print(json.dumps(progress()), flush=True)
        return
    deadline = time.monotonic() + 8 * 3600
    failures = 0
    try:
        while time.monotonic() < deadline:
            try:
                value = progress()
            except (RuntimeError, subprocess.TimeoutExpired, json.JSONDecodeError) as error:
                failures += 1
                if failures >= 5:
                    raise
                save(args.state, "STATUS.json", {"state": "WAITING", "stage": "STATUS_RETRY",
                     "epoch": 1049, "failures": failures, "error": str(error)})
                print(json.dumps({"stage": "STATUS_RETRY", "failures": failures}), flush=True)
                time.sleep(60)
                continue
            failures = 0
            save(args.state, "STATUS.json", dict(value, stage="CAR_IMPORT"))
            print(json.dumps(value), flush=True)
            if value["state"] == "COMPLETE":
                finish(args.state)
                return
            if value["state"] == "FAIL":
                raise RuntimeError("CAR import failed: " + str(value.get("error")))
            time.sleep(60)
        raise TimeoutError("CAR import did not finish within eight hours")
    except Exception as error:
        save(args.state, "STATUS.json", {"state": "FAILED", "error": str(error), "epoch": 1049})
        raise


if __name__ == "__main__":
    main()
