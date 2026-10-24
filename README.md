# cforward

Cloudflare preferred-IP smart TCP forwarder (Go).

Listens on a local port and forwards TCP traffic to a **Cloudflare preferred IP**
discovered by [CloudflareSpeedTest (cfst)](https://github.com/XIU2/CloudflareSpeedTest).
While forwarding, every IP's latency / speed / success rate / retry rate is monitored
in real time and scored; when the upstream path breaks, the **client connection is kept
and traffic resumes automatically through the next-highest-scoring IP** — transparent to
the client.

中文文档见 [README.zh-CN.md](README.zh-CN.md)。

## Features

- 🎯 **Preferred-IP pool**: cfst results enter the pool (Top 20 by default) with an initial
  score derived from measured latency and download speed
- 📊 **Live scoring**: real forwarding traffic feeds back into the score (EMA-smoothed);
  latency 40% + speed 30% + success rate 20% + retry rate 10%
- ⚡ **Instant failover**: kernel-level dead-path detection via TCP_USER_TIMEOUT (idle
  connections on a live path are never killed); RST/timeout → switch IP and resume at once
- 🔁 **Event-driven refill**: no timers — cfst re-runs when the pool runs low or the best
  score drops too far; cooldown prevents flapping
- 💾 **Persistent state**: `pool.json` keeps scores and stats across restarts
- 🧹 **Auto cleanup**: cumulative failures, stale unseen IPs, and pool overflow are handled
- ⚙️ **Two-way config**: JSON config file + CLI flags (flags win)

## How it works

```
client ──> cforward (:1234) ──> best-scoring CF IP:443 ──> Cloudflare ──> origin
                │
                └─ path broken? → immediately re-dial the next-best IP and resume
                                   (the client connection stays open)
```

## Requirements

- Linux (uses `TCP_USER_TIMEOUT`; Go 1.24+ to build)
- [cfst](https://github.com/XIU2/CloudflareSpeedTest/releases) binary and its `ip.txt`
  placed under `./cfst/` (gitignored)

## Quick start

```sh
# Prepare cfst
mkdir -p cfst && cp /path/to/cfst cfst/ && cp /path/to/ip.txt cfst/

# Build and run
go build -o cforward .
./cforward
```

The first cfst round runs automatically at startup (~1–2 min); client connections are
rejected until the pool is seeded. The pool is persisted in `pool.json`, so a restart
resumes forwarding without waiting for a fresh cfst round.

### Configuration

Every setting is available as a **CLI flag** and in the **JSON config file** (`-config`).
Precedence: CLI flags > config file > defaults.

```sh
./cforward -config config.json
./cforward -config config.json -pool-min 50 -debug   # mix and match
```

See [`config.example.json`](config.example.json) for the file format. Unset fields
fall back to defaults.

### Flags

| Flag | Default | Description |
|---|---|---|
| `-config` | | path to JSON config file (CLI flags take precedence) |
| `-listen` | `:1234` | listen address |
| `-cfst-dir` | `./cfst` | cfst working directory (contains ip.txt) |
| `-cfst-path` | `<cfst-dir>/cfst` | cfst executable path |
| `-target-port` | `443` | upstream port on the CF IP |
| `-pool-file` | `pool.json` | pool persistence file |
| `-pool-min` | `100` | refill pool when IP count drops below this |
| `-pool-max` | `200` | pool size cap; evict lowest score beyond this |
| `-cooldown` | `5m` | min interval before the next refill after a completed round |
| `-fail-evict` | `5` | evict an IP after this many cumulative failures |
| `-dial-timeout` | `3s` | dial timeout |
| `-no-data-timeout` | `10s` | dead-path detection window (TCP_USER_TIMEOUT) |
| `-cfst-url` | `https://speed.cloudflare.com/__down?bytes=3000000` | cfst download speed test URL |
| `-cfst-threads` | `100` | cfst latency test threads (`-n`) |
| `-cfst-tests` | `2` | cfst tests per IP (`-t`) |
| `-cfst-dl-count` | `10` | cfst download test count (`-dn`) |
| `-cfst-dl-time` | `10` | cfst per-IP download test time limit in seconds (`-dt`) |
| `-cfst-top` | `20` | cfst results added to the pool (`-p`) |
| `-cfst-dd` | `true` | skip the cfst download speed test (default); set `-cfst-dd=false` to enable download tests |
| `-min-active-bytes` | `65536` | a segment must move this many bytes to count as success |
| `-min-active-span` | `2s` | or data must keep flowing for at least this long (changing traffic) |
| `-dead-skip` | `60s` | skip IPs with recent no-data/error failures for this long |
| `-proven-window` | `15m` | IPs with ACTIVE traffic within this window are dialed first |
| `-admin-addr` | `127.0.0.1:18080` | HTTP admin endpoint (JSON status); empty disables |
| `-status-interval` | `60s` | status block interval |
| `-version` | | print version and exit |
| `-debug` | `false` | enable debug logging |

## Core mechanics

### Forwarding and failover

- Each client connection is handled independently; dials walk the pool in score order,
  falling back immediately on failure (failed IPs are skipped for the rest of that
  connection)
- The upstream socket uses **TCP keepalive + TCP_USER_TIMEOUT**: when the path really dies
  (data/keepalive probes get no ACK), the kernel surfaces read/write errors within about
  `-no-data-timeout`; idle connections on a live path are never falsely killed
- Upstream breaks (RST/timeout) → keep the client connection, re-dial the next-best IP and
  resume relaying (bytes read but not yet delivered at the moment of the break are lost —
  application-level retransmission covers this)
- Upstream clean close (FIN) → treated as session end; the client reconnects on its own

### Scoring

Score = `0.4 × latency + 0.3 × speed + 0.2 × success + 0.1 × retry`, each normalized to 0–100:

- latency score = `100 × (1 − latency_ms/1000)`, latency = dial duration
- speed score = `100 × (1 − e^(−speed_MBps/20))`
- success rate = segments with ACTIVE traffic ÷ total dial attempts
- retry rate = attributed retries ÷ (retries + successful connections)

**Only active traffic is the gold standard.** A segment counts as a success only when the
upstream sent data back with a real time span (`-min-active-span`, changing/ongoing flow)
or in real volume (`-min-active-bytes`). Any segment that ends without such traffic and is
not a client-side fault — TCP-only silence, a lone small response ("just a 200"), a TLS
handshake with no data, a silent close, a rejected connection — is a failure AND marks
the IP dead for `-dead-skip`, so the very next connection moves on; retry storms never
pile onto a dead IP. Only client-side faults are neutral. Failures drive ConsecFail (score
×0.1 after 3) and CumFail (`-fail-evict` evicts); a fresh cfst round revives dead-flagged
IPs. Dials prefer IPs **proven by recent active traffic** (`-proven-window`): a
mid-session switch lands on a known-good path instead of an untested high-score one.
A client that connects but never sends a byte (`idle`) is neutral — switching cannot
help a silent client.

Initial scores come from cfst's latency/download speed (U and R start at 1.0); live stats
update via EMA (α=0.3, fed only by active segments) and fully take over after 5 samples.

### Traffic: latency-only by default

Download speed tests are **off by default** (`-cfst-dd=true`): each cfst round costs only a
few KB instead of `-cfst-dl-count × url size` (~30 MB). Results are sorted by latency and
live throughput (measured from real forwarded traffic, EMA α=0.3) fills the speed score
after a few connections — initial ranking stays latency-monotonic either way. The only
trade-off: "connects but cannot download" IPs are no longer filtered in one shot; live
scoring and eviction clean them up within a few connections. For interactive use
(web browsing) latency-first ranking is usually preferable.

For heavy-download workloads the download test can be re-enabled with
`-cfst-dd=false`; to keep its traffic in check, lower `-cfst-dl-count` and shrink the
payload via `-cfst-url` (e.g. `bytes=300000`) — a round then costs ~1 MB.

### Refill (event-driven, no timers)

A cfst round triggers when: starting up / pool size < `-pool-min` / best score < 50.
After a completed round, refills wait at least `-cooldown`; never while a round is running.

## Logging

Key events go through slog; a status block is printed every `-status-interval`.
Every relay segment is logged at info level on start and end, with dial latency,
duration, up/down bytes, throughput, switch and retry counts:

```
time=... level=INFO msg="forward start" client=127.0.0.1:54420 ip=172.64.53.76 lat_ms=93 target_port=443
time=... level=INFO msg="forward end" client=127.0.0.1:54420 ip=172.64.53.76 ok=true lat_ms=93 dur=367ms up_bytes=1753 down_bytes=207472 speed_mbps=0.57 switches=0 retries=0
```

The status block also lists every active forward with live throughput:

```
── cforward status ──────────────────────────────
  pool: 59 IPs (min 100 / max 200)    active conns: 9
  #    IP                     score    lat    speed
  1    162.159.39.83          68.6    92ms   1.6MB/s
  2    172.64.53.254          68.5    93ms   1.6MB/s
  active forwards: 2
  #    client                   upstream          age    speed        up       down     dial
  1    127.0.0.1:54420          172.64.53.76      12s    1.20M     1.7M     12.0M     93ms
```

## Admin endpoint

`GET /status` on the admin address (loopback by default) returns a JSON snapshot:
version, uptime, pool (size/top/proven/dead-skipped), active forwards with live byte
counts, and cfst scheduler state:

```sh
curl -s 127.0.0.1:18080/status | jq .pool.top[0]
```

## Android app

An Android shell app (foreground service + status UI) lives in
[`android/`](android/) — the same Go core, cross-compiled and bundled. See
[`android/README.md`](android/README.md) for build and setup.

## Development

```sh
go test -race ./...   # failover, dial fallback, cfst output parsing, scoring, eviction
```

## Credits

- [XIU2/CloudflareSpeedTest](https://github.com/XIU2/CloudflareSpeedTest) — the
  preferred-IP scanner that powers this project. cforward runs it to discover the best
  Cloudflare IPs, and the Android app bundles its official releases (binary + `ip.txt`,
  plus the GPL-3.0 license text) so no extra setup is needed. The project is licensed
  under the GNU GPL v3.0 (third-party notice: see [NOTICE](NOTICE)); thanks to XIU2
  and all contributors.

## License

[MIT](LICENSE)
