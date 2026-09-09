#!/usr/bin/env python3
"""Build the short reader comparison from accepted results; never starts tests.

Requires matplotlib. Optional missing-example receipts must be independently
verified before being written to the supplemental artifact.
"""
import hashlib,json,os
from pathlib import Path
os.environ.setdefault('MPLCONFIGDIR','/tmp/blockzilla-reader-report-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

def save_chart(fig, path):
 fig.savefig(path,dpi=150,facecolor='white')
 if path.suffix=='.svg':
  path.write_text('\n'.join(line.rstrip() for line in path.read_text().splitlines())+'\n')

ROOT=Path(__file__).resolve().parents[1];D=ROOT/'docs/benchmarks';A=D/'artifacts';O=A/'reader-speed-summary-20260909';O.mkdir(exist_ok=True)
colors={'compact-v2':'#265eaa','indexer-v3':'#177765','car':'#b65312'}
names={'compact-v2':'V2','indexer-v3':'V3','car':'CAR'}
workloads=['slot-hours','usdc','pumpfun','user-program-index'];labels=['Count / CPI','USDC','Pump.fun','User-program-index']
sources={}
def load(p):
 raw=p.read_bytes();sources[str(p.relative_to(D))]=hashlib.sha256(raw).hexdigest();return json.loads(raw)
baseline=load(A/'all-samples-reader-20260908/results.json')
rows=[]
for r in baseline['cases']:
 if int(r['epoch'])!=900:continue
 assert r['status']=='PASS'
 q=dict(r);q['workload']='user-program-index' if q['workload']=='firewatch' else q['workload'];q['measurement_date']='2026-09-08';rows.append(q)
supplement=A/'epoch900-missing-examples-20260909.json'
if supplement.exists():
 new=load(supplement)
 for r in new['verified_cases']:
  assert r['status']=='PASS' and r['baseline_comparison']['matched'] and r['receipt_verified']
  assert r['mode']=='network' and r['format']=='car' and int(r['epoch'])==900
  rows=[q for q in rows if (q['mode'],q['format'],q['workload'])!=(r['mode'],r['format'],r['workload'])]
  rows.append(dict(r,measurement_date='2026-09-09'))
lookup={(r['mode'],r['format'],r['workload']):r for r in rows}
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'text.color':'#1c2735','svg.fonttype':'none'})
fig,axes=plt.subplots(2,2,figsize=(14,10))
fig.subplots_adjust(left=.08,right=.98,top=.82,bottom=.14,hspace=.46,wspace=.24)
fig.text(.04,.96,'Example speeds — full epoch 900',fontsize=23,fontweight='bold')
fig.text(.04,.917,'Four examples · 476,026,811 transactions · file and network reads',fontsize=12)
for ax,(mode,metric) in zip(axes.flat,[('local','total_tps'),('network','total_tps'),('local','scan_source_mb_s'),('network','scan_source_mb_s')]):
 tps=metric=='total_tps'
 ax.set_title(('File' if mode=='local' else 'Network')+' / '+('covered TPS · log scale' if tps else 'source MB/s'),loc='left',fontweight='bold',pad=12)
 for fi,fmt in enumerate(names):
  for wi,w in enumerate(workloads):
   r=lookup.get((mode,fmt,w))
   if not r:continue
   v=float(r[metric]);x=wi+(fi-1)*.24
   ax.bar(x,v,.22,color=colors[fmt],label=names[fmt] if wi==0 else None)
   text=(f'{v/1e9:.2f}B' if v>=1e9 else (f'{v/1e6:.2f}M' if v>=1e6 else f'{v/1e3:.0f}k')) if tps else (f'{v:.2f}' if v<10 else f'{v:.0f}')
   ax.annotate(text,(x,v),xytext=(0,15 if fi==1 else 4),textcoords='offset points',ha='center',va='bottom',fontsize=8)
 ax.set_xticks(range(4),['Count / CPI','USDC','Pump.fun','User-program\nindex'])
 if tps:
  ax.set_yscale('log');ax.set_ylim(1e4,5e9 if mode=='local' else 1e8)
  ax.yaxis.set_major_formatter(FuncFormatter(lambda v,_: f'{v/1e9:g}B' if v>=1e9 else (f'{v/1e6:g}M' if v>=1e6 else f'{v/1e3:g}k')))
 else:ax.set_ylim(0,max(float(r[metric]) for r in rows if r['mode']==mode)*1.27)
 ax.minorticks_off();ax.grid(axis='y',color='#e5e9ee');ax.set_axisbelow(True)
 for spine in ax.spines.values():spine.set_visible(False)
 ax.tick_params(axis='both',length=0)
handles,labs=axes[0,0].get_legend_handles_labels();fig.legend(handles,labs,ncol=3,loc='upper left',bbox_to_anchor=(.035,.891),frameon=False)
missing=24-len(rows)
status_path=A/'epoch900-missing-examples-status.json'
status=load(status_path) if status_path.exists() else {'state':'RUNNING'}
status_text=('The missing CAR network tests are running.' if status['state'] in ['RUNNING','PREPARING'] else 'The missing-test run stopped; see its status record.') if missing else 'All file and network examples are covered.'
fig.text(.04,.07,f'{len(rows)}/24 completed measurements. '+('Three CAR network application tests are pending.' if missing==3 else f'{missing} pending.' if missing else 'All example tests completed.'),fontsize=10)
fig.text(.04,.044,'TPS includes setup and output. Index skips count as coverage. MB/s uses source bytes / scan time; not disk bandwidth.',fontsize=10)
fig.text(.04,.018,'File results and original network cases predate the latest input changes. CAR file input is zstd; network input is raw.',fontsize=10,color='#526170')
for ext in ['png','svg']:save_chart(fig,O/f'examples.{ext}')
plt.close(fig)
def duration(seconds):
 seconds=round(seconds)
 return f'{seconds//60:,}m {seconds%60:02d}s' if seconds>=60 else f'{seconds}s'

# Compare identical full-epoch requests using time, so index coverage cannot
# force unrelated tests onto a shared logarithmic TPS axis.
for metric,chart,title in [('total_s','network-time','Network examples — time to finish'),
                            ('scan_source_mb_s','network-mbs','Network examples — source read bandwidth')]:
 fig,axes=plt.subplots(2,2,figsize=(14,9.5))
 fig.subplots_adjust(left=.14,right=.97,top=.81,bottom=.14,wspace=.49,hspace=.62)
 fig.text(.04,.95,title,fontsize=23,fontweight='bold')
 fig.text(.04,.905,'Full epoch 900 · one panel per test · '+('shorter bars are faster' if metric=='total_s' else 'bandwidth is not a query-speed ranking'),fontsize=12)
 fig.text(.04,.865,'V2 / V3 and CAR count: 8 September baseline. Missing CAR examples: awaiting verified results.' if missing else 'V2 / V3 and CAR count: 8 September baseline. CAR applications: 9 September.',fontsize=10,color='#526170')
 for ax,w,label in zip(axes.flat,workloads,labels):
  ax.set_title(label+(' · index-selected coverage' if w=='user-program-index' else ''),loc='left',fontweight='bold',fontsize=13,pad=14)
  present=[lookup[('network',fmt,w)] for fmt in names if ('network',fmt,w) in lookup]
  time_axis=metric=='total_s'
  values=[float(r[metric])/(60 if time_axis else 1) for r in present]
  limit=max(values)*1.35
  if time_axis and w!='user-program-index':limit=145
  ax.set_xlim(0,limit);ax.set_ylim(2.6,-.6)
  ticks=[]
  for i,fmt in enumerate(names):
   r=lookup.get(('network',fmt,w))
   ticks.append(names[fmt]+(f"\n{float(r['total_tps']):,.0f} TPS" if r and time_axis else ''))
   if not r:
    note='Running' if status.get('current')==w and status.get('state')=='RUNNING' else ('Queued' if status.get('state')=='RUNNING' else 'No result')
    ax.text(limit*.025,i,note,va='center',color='#7a8492',fontsize=12)
    continue
   v=float(r[metric])/(60 if time_axis else 1)
   ax.barh(i,v,.5,color=colors[fmt])
   shown=duration(float(r[metric])) if time_axis else f"{float(r[metric]):,.2f}"
   ax.text(v+limit*.025,i,shown,va='center',fontsize=12,fontweight='bold')
  ax.set_yticks([0,1,2],ticks);ax.tick_params(length=0)
  ax.set_xlabel('Minutes · lower is better' if time_axis else 'Source MB/s',labelpad=9)
  ax.grid(axis='x',color='#e5e9ee');ax.set_axisbelow(True)
  for spine in ax.spines.values():spine.set_visible(False)
 fig.text(.04,.065,'Time and TPS include setup and output. User-program-index can skip blocks; its TPS measures query coverage.' if metric=='total_s' else 'Source bytes / scan time; setup excluded. Less bandwidth can mean fewer requested bytes, not a slower query.',fontsize=10)
 fig.text(.04,.035,'These full-epoch V2/V3 results predate the latest input changes. Pending tests have no performance value.',fontsize=10,color='#526170')
 for ext in ['png','svg']:save_chart(fig,O/f'{chart}.{ext}')
 plt.close(fig)

refs=[]
for fmt in ['v2','v3']:
 a=load(A/f'{fmt}-reader-window-20260909.json')
 rr=[r for r in a['status']['cases'] if r['mode']=='network' and r['workload']=='transactions' and not r['legacy']]
 assert len(rr)==2 and all(r['valid'] for r in rr)
 refs.append({'reader':fmt.upper(),'test':'Transaction identities','total_tps':sum(r['scan']['transactions'] for r in rr)/sum(r['complete']['seconds'] for r in rr),'runs':2})
a=load(A/'network-reader-comparison-20260909.json')
for label,name in [('mirror-car-mimalloc','CAR reader'),('mirror-jetstreamer-mimalloc','Jetstreamer')]:
 r=next(r for r in a['car'] if r['label']==label)
 refs.append({'reader':name,'test':'Full decode, mimalloc','total_tps':r['total_tps'],'runs':1})
fig,axes=plt.subplots(1,2,figsize=(12,4.5));fig.subplots_adjust(left=.11,right=.98,top=.7,bottom=.24,wspace=.4)
fig.text(.04,.94,'Network reader references — 8,192 blocks',fontsize=21,fontweight='bold')
fig.text(.04,.865,'Same gateway · total TPS includes setup · compare within each panel',fontsize=11)
for ax,group,title,cs in zip(axes,[refs[:2],refs[2:]],['Transaction identities','Full transaction and metadata decode'],[['#265eaa','#177765'],['#b65312','#6c7380']]):
 vals=[r['total_tps'] for r in group];ax.barh([0,1],vals,color=cs,height=.5)
 ax.set_yticks([0,1],[r['reader'] for r in group]);ax.invert_yaxis();ax.set_xlim(0,max(vals)*1.3)
 ax.set_title(title,loc='left',fontsize=12,fontweight='bold',pad=12)
 for i,v in enumerate(vals):ax.text(v+max(vals)*.02,i,f'{v:,.0f}',va='center')
 ax.xaxis.set_major_formatter(FuncFormatter(lambda v,_:f'{v/1e3:g}k'));ax.set_xlabel('Total TPS');ax.grid(axis='x',color='#e5e9ee');ax.set_axisbelow(True)
 for spine in ax.spines.values():spine.set_visible(False)
 ax.tick_params(length=0)
fig.text(.04,.08,'V2/V3 identity scans do less work than full decoding. CAR and Jetstreamer also have different conversion costs.',fontsize=10)
for ext in ['png','svg']:save_chart(fig,O/f'references.{ext}')
plt.close(fig)
lines=['# Reader speeds: Jetstreamer, CAR, V2 and V3','',f'**Epoch 900: {len(rows)}/24 example measurements available.** '+status_text,'','## Network examples: compare time to finish','','**Shorter bars are faster.** Each panel compares the same example over the full epoch. TPS is shown beside each reader. Running or queued tests have no result yet.','','![Network time by example](artifacts/reader-speed-summary-20260909/network-time.png)','','The V2/V3 and CAR count results are the 8 September baseline, before the latest input changes. The three CAR application results will come from the current run. Time includes setup and output. User-program-index can skip blocks; its high TPS measures query coverage.','','[Separate network read-bandwidth graph](artifacts/reader-speed-summary-20260909/network-mbs.png). MB/s measures bytes read, so a reader that skips data can use less bandwidth and finish sooner.','','## Latest short network references','','![Reader reference speeds](artifacts/reader-speed-summary-20260909/references.png)','','V2/V3 use the latest input schedule. CAR and Jetstreamer use the earlier matched mimalloc test. Each reads 8,192 blocks from our gateway. Compare within each panel: identity scans and full decoding do different work. These are separate tests from the full-epoch examples above.','','## File examples','','| Example | V2 TPS | V3 TPS | CAR TPS |','|---|---:|---:|---:|']
for w,label in zip(workloads,labels):
 values=[f"{float(lookup[('local',f,w)]['total_tps']):,.0f}" for f in names]
 lines.append('| '+label+' | '+' | '.join(values)+' |')
lines+=['','File results cover the full epoch and include setup and output. Local CAR uses zstd; network CAR is raw. V3 here is the measured standalone prototype. Network and file cache conditions were not controlled.','','[All TPS and read-rate plots](artifacts/reader-speed-summary-20260909/examples.png) · [Detailed results](reader-comparison-details-20260909.md) · [Latest network changes](full-readers-20260909.md) · [Source data](artifacts/reader-speed-summary-20260909/data.json)','']
(D/'all-samples-reader-comparison-2026-09.md').write_text('\n'.join(lines))
(O/'data.json').write_text(json.dumps({'source_sha256':sources,'scope':'Full epoch900 examples; latest network prefix references kept separate','cases':rows,'references':refs,'missing_cases':24-len(rows)},indent=2)+'\n')
print(f'Updated short report: {len(rows)}/24 example measurements.')
