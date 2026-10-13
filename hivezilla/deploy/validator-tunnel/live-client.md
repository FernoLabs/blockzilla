# Live gRPC client connection

Yellowstone gRPC is running on the non-voting validator at
`ubuntu@213.239.141.61`. Its listener is `127.0.0.1:10000` on that server.
The validator does not need a restart to add a client.

## Working connection through SSH

On the consuming machine, use an SSH identity already authorized on the
validator. Keep this connection running:

```sh
ssh -N -o BatchMode=yes -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
  -L 127.0.0.1:10001:127.0.0.1:10000 ubuntu@213.239.141.61
```

Set the Yellowstone client endpoint to `http://127.0.0.1:10001`.
Use plaintext HTTP/2 inside this local connection; SSH encrypts the network
transport. This local listener is not a public gRPC endpoint. The server
accepted the test subscription without a gRPC token; network access is
controlled by SSH. This does not authorize public exposure of port 10000.

Subscribe to `blocks`, set `include_transactions=true`, and use finalized
commitment for finalized blocks. No account or transaction filter is needed
for a complete block stream. Use the matching Yellowstone protocol/SDK.
The server reports Yellowstone `16.0.0+solana.4.3.0`, protocol `14.0.0`.
It is the prepared modified validator build, not an unmodified upstream build.

The consumer must reconnect if SSH or gRPC disconnects. Save its last durable
slot. Check `SubscribeReplayInfo` before requesting replay: this server keeps
only a short replay window. It is not an archive backfill service. Report a
gap explicitly if the requested slot is outside that window.

The local Mac test on October 2 received eight finalized blocks with
transactions, slots 452578293 through 452578300, in 1.85 seconds. This proves
external access over SSH. It does not prove Cloudflare access or installation
on a second Hetzner machine. See `live-client-test-20261002.json`.

## Cloudflare status

The dedicated tunnel is healthy with four connections. Server-side relays
work at `10.253.45.3:10000`. The Cloudflare private route and Access application
are absent. Creating the prepared Access application with the refreshed
Wrangler login returned HTTP 403, `auth.forbidden`, on October 2.
No private route was created without the intended Access protection.

An authorized Cloudflare account must create the Access application and
private route, then enroll/configure the consuming device with WARP. For a
server consumer, choose an enrollment and access policy that permits that
server; the prepared owner-email policy alone is not proof of unattended
server access. Test a real subscription from that device before switching
the consumer to `http://10.253.45.3:10000`.

Cloudflare documents gRPC through private subnet routing, not public tunnel
hostnames: https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/use-cases/grpc/.

The consuming machine's address and authorized login are still unknown.
The SSH forward is currently installed only on the local Mac, not on another
Hetzner machine. No separate Hivezilla live output service was deployed.
