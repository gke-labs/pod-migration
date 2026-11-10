# Cooperative workload and TCP continuity harness

This document describes a small synthetic workload and an external TCP observer
for testing migration engines and drivers. Neither component migrates anything
by itself, changes the controller's behavior, or is wired into the reconcilers.
They provide a workload whose in-memory state is easy to verify exactly before
and after a checkpoint/restore, and a client that reports whether one
established connection survived the cutover.

| Path | Purpose |
|---|---|
| `controller/internal/cooperative` | In-memory counter, its HTTP control surface, an optional TCP frame stream, and a control client. The client itself accepts any HTTP(S) base URL; loopback-only enforcement lives in `cooperative-control`. |
| `controller/internal/tcpcontinuity` | Frame codec and a single-connection peer that validates nonce, sequence and counter continuity. |
| `controller/cmd/cooperative-counter` | Runs the counter and its control endpoints. |
| `controller/cmd/cooperative-control` | One-shot client for those control endpoints, intended to be exec'd inside the workload container. |
| `controller/cmd/tcp-continuity-peer` | Holds one TCP connection to the counter and reports continuity as JSON. |

## Counter state machine

The counter has a random boot nonce, a monotonic counter, an epoch and a
migration ID. It always cold-starts **paused**, so a restarted process cannot
silently pass for the original.

- `GET /state` returns the current state.
- `POST /pause` with `nonce` and `migrationID` stops ticking at an atomic
  boundary and returns the exact paused state. A pause for a different nonce is
  rejected. Each process accepts **one migration ID for its lifetime**: any
  other ID is rejected with 409, and a same-ID retry after the process was
  resumed returns the current running state instead of pausing again, so a
  delayed retry cannot re-pause an activated destination. The Go client and
  `cooperative-control` report that running-state reply as a conflict (nonzero
  exit). An aborted attempt therefore cannot be re-paused; start a fresh
  counter to retry.
- `POST /resume` with the exact paused state JSON resumes counting. Resume is
  bound to the epoch and is idempotent for the same request.
- `POST /stop` with the exact paused state JSON stops the process permanently.

There is no endpoint that imports state from JSON. After a restore, the only way
to observe the original nonce and counter is for the process memory to have been
restored. A driver can therefore compare the paused state captured before
checkpoint with the state reported after restore, and treat any difference as a
failed restore.

The HTTP endpoints are unauthenticated. Bind them to loopback (the default
`-listen 127.0.0.1:8080`) and reach them with `cooperative-control` from inside
the container rather than exposing them on a network.

```sh
cd controller
go run ./cmd/cooperative-counter -listen 127.0.0.1:8080 -interval 100ms
```

In another terminal, read the paused cold-start state and resume it, then pause
it for a migration using the boot nonce from the state:

```sh
go run ./cmd/cooperative-control -action state
go run ./cmd/cooperative-control -action resume -expected "$(go run ./cmd/cooperative-control -action state)"
go run ./cmd/cooperative-control -action pause -nonce <nonce> -migration-id <id>
```

`cooperative-control` only accepts a literal loopback `-endpoint`, uses no DNS or
proxy, limits `-expected` to 16 KiB, and bounds each call with `-timeout`.
Code that uses `cooperative.Client` directly gets none of those endpoint
restrictions and must supply its own transport policy.

## TCP continuity observation

The counter can also serve an opt-in TCP stream (`-tcp-listen`, disabled by
default). Each frame carries the boot nonce, a per-connection consecutive
sequence number, and the counter. No frames are written while the counter is
paused. Pause drains active, bounded writes (`-tcp-write-timeout`, at most 5s)
before acknowledging, but bytes already queued in the kernel can still reach the
peer afterward. Sequence state exists only in process memory, so a checkpoint
must preserve it together with the socket.

```sh
go run ./cmd/cooperative-counter -tcp-listen 127.0.0.1:9090 -interval 100ms -tcp-write-timeout 250ms
go run ./cmd/cooperative-control -action resume -expected "$(go run ./cmd/cooperative-control -action state)"
go run ./cmd/tcp-continuity-peer -target 127.0.0.1:9090 -duration 60s -max-silence 10s
```

The peer dials once, holds that exact socket, and never reconnects. It exits
nonzero on premature EOF or reset, silence longer than `-max-silence`, malformed
frames, nonce drift, sequence gaps, or counter regression. Its JSON output
records the connection tuple and the outcome. `migrationProven` is always
`false`: a clean local observation proves neither address movement nor
checkpoint/restore, which must be established by the driver under test.

For a request/response variant, add `-tcp-bidirectional` to the counter and
`-bidirectional` to the peer. `-max-frame-gap` adds a response-gap budget,
measured on the client between complete valid responses (including initial and
trailing silence). Exceeding the budget records a failure without closing the
socket early, so a run can distinguish connection survival from acceptable
responsiveness.

## Limits

- Pausing the application does not freeze the kernel TCP stack. Stopping the
  source process closes its sockets and can send FIN or RST, so a cutover that
  stops the source before the destination owns the connection breaks it. The
  peer reports that failure rather than hiding it.
- Preserving a connection across nodes additionally requires fencing the
  source data plane before the final checkpoint and giving the destination
  exclusive ownership of the address. This harness implements none of that.
- The TCP stream and HTTP controls are unauthenticated test endpoints. Do not
  expose them on shared or untrusted networks.
- For a live test, run the peer on an independent node, target the original Pod
  IP directly, and keep the stream open before, during and after cutover.
  `-tcp-listen` requires an explicit IP literal (not `0.0.0.0`, `:port` or a
  hostname), so in a Pod bind it to the Pod IP, for example passed in as
  `$(POD_IP)` through the downward API.
