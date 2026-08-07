# Workers

Cloudflare Workers deployed independently of the NAS-hosted services.

## Current workers

- [`workers/blockzilla-get-block/`](blockzilla-get-block/README.md): read-only Archive V2 edge API/Worker.
- [`workers/old-faithful-get-block/`](old-faithful-get-block/README.md): read-only Old Faithful compatibility edge API/Worker.

## Deployment model

`services/` contains deployable daemons and runtime entrypoints that run on NAS hosts.
`workers/` contains Cloudflare Workers that can be built and deployed independently
of the NAS and point at the publicly exposed Blockzilla APIs.

When deploying from this repository, keep only service daemons under the
`services/` umbrella on the NAS path, and deploy one or more `workers/` artifacts
to Cloudflare separately.
