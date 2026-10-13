# Token indexer checks, 2026-10-04

The separate USDC SQLite consumer is running on the Foundation machine.
These files record tests and observations. They are not a live health report.

- `token-indexer-demo-report.json`: deployment, progress, consumer restart,
  native recovery, historical reads, service PIDs, and raw shred warning.
- `native-proof-status.json`: 292 recovered blocks followed by two live blocks.
- `native-rpc-oracle.json`: every receipt and selected token balance in six
  recovered/live blocks matched local finalized RPC.
- `rpc-oracle.json`: all 49,722 token balance records and 11,638 receipts in
  the first ten live blocks matched local finalized RPC.
- `history-resume-status.json`: epoch 900 Cloudflare reads and restart reached
  six blocks and 10,326 balance records.
- `blocked-history-status.json`: unavailable current-epoch archive retained
  the ten-block checkpoint and recorded a BLOCKED recovery.
- `partial-recovery-cap.json`: an isolated one-chunk history copy ran with
  `--max-blocks 1`. The accepted block remained in SQLite; unavailable later
  history still returned exit status 1 and retained a BLOCKED recovery.
- `build-tests.log` and `decoder-tests.log`: nine CLI and eleven decoder tests.
- `demo-before-restart.json` and `demo-after-restart.json`: the continuous
  database advanced after the corrected consumer restarted.

The running executable is an optimized development build, not a capacity
benchmark. The validator, worker, recorder, origin, and relay were not restarted.
The public stream is not configured. Current-epoch archive publication, complete
epoch coverage, source-session changes, and database retention remain unfinished.
Raw shred recording stopped at its cumulative quota while compact output kept
advancing. Compact-file recovery does not repair that raw capture gap.
