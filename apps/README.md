# Apps

Application frontends are separated from NAS-hosted services.

## Current apps

- [`apps/blockzilla-watcher/`](blockzilla-watcher/README.md): operational watcher UI for live indexing, compaction, and NAS health.
- [`apps/blockzilla-get-block/`](blockzilla-get-block/README.md): read-only Archive V2 edge API/Worker.
- [`apps/old-faithful-get-block/`](old-faithful-get-block/README.md): read-only Old Faithful compatibility edge API/Worker.

## Deployment model

`services/` contains deployable daemons and runtime entrypoints that run on NAS hosts.
`apps/` contains user-facing apps that can be built and deployed independently
(for example on a web host, CDN, or static site service) and point at the
publicly exposed Blockzilla APIs.

When deploying from this repository, keep only service daemons under the
`services/` umbrella on the NAS path, and deploy one or more `apps/` artifacts
from a separate web/runtime host.
