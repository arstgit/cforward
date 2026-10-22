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
| `-cfst-dd` | `false` | skip the cfst download speed test; saves traffic, initial score becomes latency-based |
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
- success rate = dials that succeeded and whose transport did not fail ÷ total dials
- retry rate = attributed retries ÷ (retries + successful connections)

Initial scores come from cfst's latency/download speed (U and R start at 1.0); live stats
update via EMA (α=0.3) and fully take over after 5 samples. 3 consecutive failures crush
the score ×0.1; `-fail-evict` cumulative failures evict the IP.

### Saving mobile data (`-cfst-dd`)

With `-cfst-dd` enabled, cfst skips the download speed test (per-round traffic drops from
`-cfst-dl-count × url size` — ~30 MB by default — to a few KB). Results are then sorted by
latency and the initial score carries no speed term (0.4L + 0.2U + 0.1R); ranking stays
latency-monotonic, and the live throughput EMA fills the speed score in after a few
connections, so the scoring impact is minimal. The trade-off: one-shot filtering of
"connects but cannot download" IPs is lost — live scoring and eviction cover it after a
few connections. For interactive proxy use (web browsing) latency-first ranking is
usually preferable; keep the download test for heavy-download workloads.

To trim traffic while keeping the download test: lower `-cfst-dl-count` and shrink the
payload via `-cfst-url` (e.g. `bytes=300000`) — a round then costs ~1 MB.

### Refill (event-driven, no timers)

A cfst round triggers when: starting up / pool size < `-pool-min` / best score < 50.
After a completed round, refills wait at least `-cooldown`; never while a round is running.

## Logging

Key events go through slog; a status block is printed every `-status-interval`:

```
── cforward status ──────────────────────────────
  pool: 59 IPs (min 100 / max 200)    active conns: 9
  #    IP                     score    lat    speed
  1    162.159.39.83          68.6    92ms   1.6MB/s
  2    172.64.53.254          68.5    93ms   1.6MB/s
```

## Development

```sh
go test -race ./...   # failover, dial fallback, cfst output parsing, scoring, eviction
```

## Credits

- [XIU2/CloudflareSpeedTest](https://github.com/XIU2/CloudflareSpeedTest) — the preferred-IP scanner

## License

[MIT](LICENSE)
