# Validator Tunnel on 213.239.141.61

Update, 5 October 2026: the same tunnel now serves the API-key protected native
compact relay at `https://stream.blockzilla.dev`. Consumers use SSE chunk notices
and finite binary GETs; they do not need SSH or WARP. This is HTTP, not the old
gRPC service. See [the live stream guide](../start/live-stream.md) and the latest
[deployment record](../../hivezilla/deploy/direct-agave/README.md). The earlier
private gRPC preparation below is historical and does not describe this new route.

Server preparation completed on 2026-10-01. The validator kept its original
process, PID 471837. RPC `getHealth` returned `ok`.

## Current state

| Component | State |
| --- | --- |
| Non-voting validator | Running, with no maximum run time |
| Validator restart policy | Restart on failure after 30 seconds |
| Validator boot configuration | Persistent unit and boot symlink installed; reboot not tested |
| Cloudflare Tunnel | Healthy, four connections, cloudflared 2026.9.3 |
| Tunnel boot service | Enabled |
| Private address and relay sockets | Enabled and tested on the server |
| Cloudflare Access owner policy | Prepared; not created |
| Cloudflare private route | Prepared; not created |
| WARP client connection | Not tested; client setup still required |
| Public dashboard hostname | Not configured |
| Hivezilla plugin stream | Future work; no service or port opened |

Recheck on 2026-10-01: the server readiness endpoint reports four ready
connections, and the Cloudflare API reports `hivezilla-validator` as healthy.
The private dashboard relay returns HTTP 200. The existing OAuth login was
refreshed. Current read requests for tunnels, private routes, and Access
applications return HTTP 200. No private route exists for this tunnel, and
the prepared Hivezilla Access application is absent from the returned list.
Earlier creation requests were denied; write permission has not been tested
again. Receiving-device access through WARP remains untested.

The separate remotely managed Tunnel is `hivezilla-validator`, ID
`0cd18a26-d76a-408c-a1fa-55a1151fa3ae`, in account
`8df0426c80fcd1a04ed439f441db9ff8`. It does not use or change the
`blockzilla-receiver` NAS Tunnel or its `10.253.45.2/32` route.

## Network design

[Cloudflare's gRPC guide](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/use-cases/grpc/)
supports gRPC through private subnet routes. It does not support gRPC on a
public Tunnel hostname. Receiving devices need Cloudflare One/WARP, or another
supported private network connection.

The planned route is a single address, `10.253.45.3/32`. The address is assigned
to the server's loopback interface. A separate nftables table drops packets to
this address from physical interfaces. It allows local TCP connections only
to SSH, the dashboard, and the Yellowstone stream, plus replies to established
connections. It does not change gossip ports, public SSH, or other host traffic.

systemd socket relays forward the dashboard and gRPC traffic to the existing
loopback listeners. The validator does not need a restart or a bind-address
change. SSH uses its existing listener.

These addresses are **prepared endpoints**, not working client addresses yet:

| Service | Endpoint after Access and WARP setup |
| --- | --- |
| SSH | `ubuntu@10.253.45.3`, port 22 |
| Validator dashboard | `http://10.253.45.3:8766/` |
| Yellowstone gRPC | `http://10.253.45.3:10000` |

The transport between an enrolled client and Cloudflare, and between
Cloudflare and the machine, is encrypted. The local gRPC listener continues to
use plaintext HTTP/2. A gRPC client must use the endpoint scheme above rather
than assume that port 10000 terminates TLS.

RPC port 8899, plugin metrics port 8999, and Tunnel metrics port 20000 are not
allowed on the private address. They remain local services.

## Finish Cloudflare access

The original Wrangler OAuth login could create and configure tunnels and read
routes. Access application creation returned HTTP 403,
`auth.forbidden`. Gateway settings and device policies also returned HTTP 403.
These were permission limits, not a server connection failure. The later
login refresh restored read access; it has not proved those write permissions.

The proposed settings are saved in
[`access-application.json`](../../hivezilla/deploy/validator-tunnel/access-application.json)
and [`private-route.json`](../../hivezilla/deploy/validator-tunnel/private-route.json).
The policy permits only the account owner's email and applies to TCP traffic
to the complete `/32`. No route was created before this policy could be applied.

Complete the following with an authorized Cloudflare admin session:

1. Create the self-hosted private Access application from the saved settings.
   Check that its only Allow rule is the intended owner's email.
2. Create `10.253.45.3/32` as a private route for `hivezilla-validator`.
3. Check Gateway TCP proxy settings and the receiving device's profile.
   Include the single `/32` in its WARP traffic routes. Avoid changing unrelated
   NAS routes or default profiles for other devices.
4. Enroll the receiving device in the same Zero Trust organization and sign in.
   The server's Tunnel token is not a WARP enrollment credential.
5. Test SSH, the dashboard, and a gRPC subscription from that device. Record the
   result before calling the connection complete.

See the official guides for
[private route setup](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/private-net/cloudflared/connect-cidr/)
and [private Access applications](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/non-http/self-hosted-private-app/).

A public dashboard address can be added separately after its Access policy
and DNS permissions are available. It is not needed for the private stream.

## Installed service files

The reviewed configuration is in
[`hivezilla/deploy/validator-tunnel/`](../../hivezilla/deploy/validator-tunnel/).
The validator unit uses this machine's exact prepared arguments and directory:
`/home/ubuntu/live/hivezilla-validator-20260930`.

On the server:

- `/etc/systemd/system/hivezilla-validator-canary.service` contains the
  persistent validator unit. Its `continuous.conf` drop-in overrides the
  original transient unit's six-hour limit for the current run.
- `/etc/systemd/system/hivezilla-tunnel.service` starts cloudflared with a
  dynamic unprivileged user. systemd supplies its private token as a credential.
- `/etc/cloudflared/hivezilla-validator.token` is root-owned with mode 0600.
  It is not in the repository, command arguments, or status reports.
- `hivezilla-private-address.service` loads the narrow firewall and assigns
  the loopback address. `hivezilla-private@8766.socket` and
  `hivezilla-private@10000.socket` activate the systemd TCP relays.
- `/etc/sysctl.d/90-hivezilla-validator.conf` preserves the validator's existing
  UDP buffer values across a boot. The prior kernel baseline remains in the
  prepared reports directory.

The active validator is still the original transient unit in systemd memory.
The persistent file and boot symlink will apply on the next boot. Do not reboot
or restart just to test this transition. Use `systemctl` for the continuous
service; the finite `hivezilla validator-canary start` command still creates a
transient unit and must not be used to replace this deployment.

Read-only service checks:

```sh
systemctl show hivezilla-validator-canary.service \
  -p ActiveState -p MainPID -p Restart -p RuntimeMaxUSec
systemctl status hivezilla-tunnel.service \
  hivezilla-private-address.service \
  hivezilla-private@8766.socket hivezilla-private@10000.socket
curl --noproxy '*' http://127.0.0.1:20000/ready
```

During the current transient run, `systemctl is-enabled` can report `transient`
even though the persistent file and boot symlink exist. Tunnel and relay units
report `enabled` normally.

## Tests completed

- The service files passed `systemd-analyze verify`. The nftables configuration
  passed its check before installation.
- cloudflared's readiness endpoint reported four ready connections. The
  Cloudflare API reported the dedicated Tunnel as `healthy`.
- HTTP on `10.253.45.3:8766` returned 200 with the Hivezilla dashboard.
- SSH on the private address returned the expected OpenSSH server banner.
- gRPC on `10.253.45.3:10000` returned HTTP/2 SETTINGS. A short Hivezilla probe
  then received 16 updates, including 15 live entry updates, and reported
  Yellowstone `16.0.0+solana.4.3.0` and finalized slot 452245094.
- The validator remained PID 471837 and `getHealth` returned `ok`.

Server evidence is in `reports/tunnel-server-check-20261001.json` and
`reports/tunnel-grpc-probe-20261001.json`. These are server relay checks. They
do not prove a WARP client can connect through the Cloudflare network. No
archive verification or R2 publication was run.

## Add a future Hivezilla stream

Keep the new stream bound to loopback. Test its protocol and chosen port
locally first. Then add that exact port to the private-address firewall and
enable its corresponding socket relay. The existing `/32` Access policy can
protect it without a new Tunnel or a validator bind change. The port is not
reserved or opened until the service exists.
