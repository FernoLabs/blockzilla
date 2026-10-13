#!/usr/bin/env python3
import argparse,json,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--base',type=Path,required=True);a=p.parse_args();host=a.base/'agave'
snippet=(a.base/'provider/hivezilla/deploy/bank-stream-v2/native_load_test.rs').read_text()
f=host/'geyser-plugin-manager/src/geyser_plugin_manager.rs';s=f.read_text();marker='// Compiled inside the pinned host'
if marker in s:s=s[:s.index(marker)]
f.write_text(s+'\n'+snippet)
f=host/'geyser-plugin-manager/Cargo.toml';s=f.read_text()
if '[dev-dependencies]' not in s:s+='\n[dev-dependencies]\nsolana-runtime = { path = "../runtime", features = ["agave-unstable-api", "dev-context-only-utils"] }\n'
f.write_text(s)
subprocess.run(['/home/ubuntu/.cargo/bin/rustfmt','+1.97.1','--edition','2024',str(host/'geyser-plugin-manager/src/geyser_plugin_manager.rs')],check=True)
subprocess.run(['git','-C',str(host),'add','geyser-plugin-manager'],check=True)
subprocess.run(['git','-C',str(host),'-c','user.name=Blockzilla build','-c','user.email=build@blockzilla.dev','commit','-m','test: load native bank v2 plugin with a real bank queue tuple'],check=True)
revision=subprocess.check_output(['git','-C',str(host),'rev-parse','HEAD'],text=True).strip()
(a.base/'HOST-SOURCE.json').write_text(json.dumps({'base_commit':'825efd18292aff6ffcf9daa0f7612f21b3531a72','agave_revision':revision,'native_load_test':'native_bank_v2_actual_plugin_load'},indent=2)+'\n')
print(revision)
