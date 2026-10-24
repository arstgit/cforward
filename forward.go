package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Linux TCP_USER_TIMEOUT (18 in linux/tcp.h): data or keepalive probes left
// unacknowledged for this long abort the connection and surface as read/write errors.
const tcpUserTimeoutOpt = 18

// activeConns counts live client connections (used by the status block).
var activeConns atomic.Int64

// Forwarder listens on a port and forwards to the highest-scoring IP in the pool.
// When the upstream path breaks, the client connection is kept and forwarding
// resumes through the next-best IP immediately.
type Forwarder struct {
	pool           *Pool
	targetPort     int
	dialTimeout    time.Duration
	noDataTimeout  time.Duration
	deadSkip       time.Duration // IPs with repeated failures are skipped for this long
	deadAfter      int           // consecutive failures before an IP is dead-skipped
	provenWindow   time.Duration // IPs with ACTIVE traffic within this window are dialed first
	maxPerIP       int           // soft cap on concurrent segments per IP (0 = unlimited)
	minActiveBytes int64         // a segment must carry this much traffic to count as success
	minActiveSpan  time.Duration // ...or data must keep flowing for at least this long
}

func NewForwarder(pool *Pool, targetPort int, dialTimeout, noDataTimeout, deadSkip time.Duration, deadAfter int, minActiveSpan, provenWindow time.Duration, minActiveBytes int64, maxPerIP int) *Forwarder {
	return &Forwarder{
		pool:           pool,
		targetPort:     targetPort,
		dialTimeout:    dialTimeout,
		noDataTimeout:  noDataTimeout,
		deadSkip:       deadSkip,
		deadAfter:      deadAfter,
		provenWindow:   provenWindow,
		minActiveSpan:  minActiveSpan,
		minActiveBytes: minActiveBytes,
		maxPerIP:       maxPerIP,
	}
}

// Serve accepts connections until ctx is cancelled.
func (f *Forwarder) Serve(ctx context.Context, ln net.Listener) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("accept", "err", err)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.handle(conn)
		}()
	}
}

// tuneUpstream makes the kernel report a genuinely dead path in time:
// keepalive + TCP_USER_TIMEOUT — if data or keepalive probes get no ACK within
// ~userTimeout, reads/writes error out (applies to active and idle connections;
// slow-but-alive services on a live path are never falsely killed).
func tuneUpstream(conn *net.TCPConn, userTimeout time.Duration) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(userTimeout / 2)
	rc, err := conn.SyscallConn()
	if err != nil {
		slog.Debug("upstream syscallconn", "err", err)
		return
	}
	ms := int(userTimeout / time.Millisecond)
	var serr error
	rc.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpUserTimeoutOpt, ms)
	})
	if serr != nil {
		slog.Debug("upstream tcp_user_timeout", "err", serr)
	}
}

// dialBest dials candidates in two passes: first the IPs proven by recent ACTIVE
// traffic (they actually serve data, not just score well), then the rest — both in
// score order, skipping the IPs in skip (failed IPs are added to skip so they are
// not retried within one client connection).
// Returns the connection, the winning IP, the failed-attempt count, and the dial
// latency of the winning IP in ms.
func (f *Forwarder) dialBest(skip map[string]bool) (net.Conn, *IPEntry, int, float64) {
	candidates := f.pool.Candidates()
	retries := 0
	tryDial := func(cand *IPEntry) (net.Conn, float64, bool) {
		ip := cand.IP
		start := time.Now()
		conn, err := net.DialTimeout("tcp",
			net.JoinHostPort(ip, strconv.Itoa(f.targetPort)), f.dialTimeout)
		latMs := float64(time.Since(start).Microseconds()) / 1000.0
		if err != nil {
			slog.Debug("dial failed", "ip", ip, "err", err, "lat_ms", latMs)
			f.pool.ReportDial(ip, false, latMs)
			skip[ip] = true
			retries++
			return nil, latMs, false
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tuneUpstream(tc, f.noDataTimeout)
		}
		f.pool.ReportDial(ip, true, latMs)
		return conn, latMs, true
	}

	proven := func(cand *IPEntry) bool {
		return !cand.LastActive.IsZero() && time.Since(cand.LastActive) <= f.provenWindow
	}
	// Pass 1: proven-good IPs (recent active traffic). Load-balanced: among proven
	// candidates, those with the fewest live segments go first (ties broken by
	// score), so a client's connection storm spreads across several known-good
	// IPs instead of piling onto a single one (single point of failure).
	var provenList []*IPEntry
	for _, cand := range candidates {
		if skip[cand.IP] || !proven(cand) {
			continue
		}
		provenList = append(provenList, cand)
	}
	sort.Slice(provenList, func(i, j int) bool {
		ci, cj := f.activeCount(provenList[i].IP), f.activeCount(provenList[j].IP)
		if ci != cj {
			return ci < cj
		}
		return provenList[i].Score() > provenList[j].Score()
	})
	for _, cand := range provenList {
		if f.atLimit(cand.IP) {
			// Overloaded: let the unproven pool take over instead of piling every
			// connection onto one proven IP (single point of failure).
			continue
		}
		if conn, lat, ok := tryDial(cand); ok {
			return conn, cand, retries, lat
		}
	}
	// Pass 2: everything else (unproven fallbacks and cold-start), load-balanced the
	// same way.
	var rest []*IPEntry
	for _, cand := range candidates {
		if skip[cand.IP] {
			continue
		}
		if time.Now().Before(cand.DeadUntil) {
			// Temporarily dead (recent failures): skip without counting a retry.
			continue
		}
		rest = append(rest, cand)
	}
	sort.Slice(rest, func(i, j int) bool {
		ci, cj := f.activeCount(rest[i].IP), f.activeCount(rest[j].IP)
		if ci != cj {
			return ci < cj
		}
		return rest[i].Score() > rest[j].Score()
	})
	var leastLoaded *IPEntry
	for _, cand := range rest {
		if f.atLimit(cand.IP) {
			if leastLoaded == nil {
				leastLoaded = cand // rest is sorted by load: first over-limit one is least loaded
			}
			continue
		}
		if conn, lat, ok := tryDial(cand); ok {
			return conn, cand, retries, lat
		}
	}
	if leastLoaded != nil {
		// Every candidate is over the soft cap: still serve the least-loaded one
		// rather than rejecting the connection.
		if conn, lat, ok := tryDial(leastLoaded); ok {
			return conn, leastLoaded, retries, lat
		}
	}
	return nil, nil, retries, 0
}

// activeCount returns how many live relay segments currently use this IP.
func (f *Forwarder) activeCount(ip string) int {
	n := 0
	activeSegs.Range(func(_, v any) bool {
		if v.(*segInfo).ip == ip {
			n++
		}
		return true
	})
	return n
}

// atLimit reports whether the IP already carries the max allowed concurrent
// segments (soft cap; 0 = unlimited).
func (f *Forwarder) atLimit(ip string) bool {
	return f.maxPerIP > 0 && f.activeCount(ip) >= f.maxPerIP
}

// segInfo tracks one live relay segment (for per-segment logs and the status block).
// Byte counters are atomic: copyDir goroutines write them while the status block reads.
type segInfo struct {
	client     net.Conn
	clientAddr string
	ip         string
	dialLatMS  int64 // dial latency of this segment's upstream, ms
	start      time.Time
	c2u        atomic.Int64 // client→upstream bytes
	u2c        atomic.Int64 // upstream→client bytes
	firstFlow  atomic.Int64 // UnixNano of the first data byte in either direction
	lastFlow   atomic.Int64 // UnixNano of the last data byte
}

// flowSpan returns how long data kept flowing during this segment.
// A lone burst (e.g. a single response) has a near-zero span; ongoing traffic
// (“changing flow”) spans seconds. 0 when no data moved.
func (s *segInfo) flowSpan() time.Duration {
	first := s.firstFlow.Load()
	last := s.lastFlow.Load()
	if first == 0 || last <= first {
		return 0
	}
	return time.Duration(last - first)
}

func (s *segInfo) touch() {
	now := time.Now().UnixNano()
	s.firstFlow.CompareAndSwap(0, now)
	s.lastFlow.Store(now)
}

// activeSegs holds all live relay segments, keyed by the client connection.
var activeSegs sync.Map

func newSeg(client net.Conn, clientAddr, ip string, latMs float64) *segInfo {
	seg := &segInfo{
		client:     client,
		clientAddr: clientAddr,
		ip:         ip,
		dialLatMS:  int64(latMs),
		start:      time.Now(),
	}
	activeSegs.Store(client, seg)
	return seg
}

func (s *segInfo) done() { activeSegs.Delete(s.client) }

// handle drives one client connection. Core loop:
// dial (fall back on failure) → relay → upstream breaks → re-dial the next-best IP
// → resume relaying, until the client disconnects.
func (f *Forwarder) handle(client net.Conn) {
	defer client.Close()
	defer activeSegs.Delete(client)
	activeConns.Add(1)
	defer activeConns.Add(-1)
	clientAddr := client.RemoteAddr().String()

	skip := make(map[string]bool)
	upstream, used, retries, latMs := f.dialBest(skip)
	if upstream == nil {
		slog.Warn("no candidates, rejecting", "client", clientAddr)
		return
	}

	seg := newSeg(client, clientAddr, used.IP, latMs)
	slog.Info("forward start", "client", clientAddr, "ip", used.IP,
		"lat_ms", seg.dialLatMS, "target_port", f.targetPort)
	switchCount := 0
	for {
		res := f.relay(client, upstream, seg)
		upstream.Close()
		seg.done()

		c2u, u2c := seg.c2u.Load(), seg.u2c.Load()
		total := c2u + u2c
		verdict, ok := f.classify(res, c2u, u2c, seg.flowSpan())
		speed := 0.0
		if res.dur > 0 {
			speed = float64(total) / res.dur.Seconds() / 1e6 // MB/s, both directions
		}
		slog.Info("forward end",
			"client", clientAddr, "ip", used.IP, "ok", ok, "verdict", verdict,
			"lat_ms", seg.dialLatMS, "dur", res.dur.Round(time.Millisecond),
			"up_bytes", c2u, "down_bytes", u2c, "flow_span", seg.flowSpan().Round(time.Millisecond),
			"speed_mbps", round2(speed), "switches", switchCount, "retries", retries)

		switch verdict {
		case "active":
			// Gold standard: ongoing changing traffic (upstream sent data across time
			// or in volume). Only this feeds Successes and the speed EMA.
			f.pool.ReportConn(used.IP, true, total, res.dur, retries)
		case "tiny":
			// Neutral: a complete exchange happened (e.g. a full TLS handshake) but
			// no real traffic followed. The TCP+TLS path works — the stall is at the
			// application layer, not the IP's fault. Counting it as a failure would
			// let a client's connection storm burn good IPs. It neither rewards nor
			// penalizes: Successes stays flat, so U drifts down only slowly.
			f.pool.ReportNeutral(used.IP, retries)
		case "client_err", "idle":
			// Neutral: the client broke the connection, or connected but never sent a
			// byte. Neither rewards nor penalizes the IP.
		default:
			// Fail: up_err, no_data or no_response — the upstream never served usable
			// data. Drives ConsecFail/CumFail. Dead-skip only after -dead-after
			// consecutive failures (stability-first: one bad segment on a flaky
			// long-distance path must not burn the IP).
			f.pool.ReportConn(used.IP, false, total, res.dur, retries)
			if f.pool.MaybeMarkDead(used.IP, f.deadSkip, f.deadAfter) {
				slog.Warn("upstream failed repeatedly, marking dead",
					"ip", used.IP, "client", clientAddr, "verdict", verdict,
					"up_bytes", c2u, "down_bytes", u2c,
					"dur", res.dur.Round(time.Millisecond))
			}
		}

		// Upstream ended normally (EOF) or the client is gone → connection over.
		if res.upErr == nil || res.clientErr != nil || res.upEOF {
			return
		}

		// Upstream broke: switch to the next-best IP and keep relaying.
		skip[used.IP] = true
		switchCount++
		f.pool.MarkDead(used.IP, f.deadSkip)
		slog.Info("upstream broken, switching", "from", used.IP,
			"client", clientAddr, "err", res.upErr, "switch_count", switchCount)

		newUp, newUsed, r, lat := f.dialBest(skip)
		if newUp == nil {
			slog.Warn("no more candidates, dropping client", "client", clientAddr,
				"switch_count", switchCount)
			return
		}
		upstream, used, retries = newUp, newUsed, r
		seg = newSeg(client, clientAddr, used.IP, lat)
		slog.Info("forward start", "client", clientAddr, "ip", used.IP,
			"lat_ms", seg.dialLatMS, "switch_count", switchCount)
	}
}

// classify judges a finished segment. Only ACTIVE traffic is the gold standard:
// the upstream sent data back with a real time span (ongoing, changing flow) or in
// real volume. Everything else that is not a client-side fault is a failure.
// Stability-first: a single failed segment never marks an IP dead by itself —
// dead-skipping requires -dead-after consecutive failures (decided by the caller),
// so transient stalls on a flaky long-distance path do not churn the pool.
//
// Verdicts:
//   - active:       real traffic flowed (u2c>0 and flowSpan/volume thresholds met) → success
//   - up_err:       upstream-side transport error → failure + in-loop switch (re-dial now)
//   - no_data:      accepted TCP, nothing came back, ended quickly → failure
//   - idle:         client connected but never sent a byte → NEUTRAL (client's choice)
//   - no_response:  one-way only, upstream never sent a byte → failure
//   - tiny:         only a small exchange (a lone 200 / TLS handshake only) → failure
//   - client_err:   client broke the connection → neutral (not the IP's fault)
func (f *Forwarder) classify(res relayResult, c2u, u2c int64, flowSpan time.Duration) (verdict string, ok bool) {
	total := c2u + u2c
	switch {
	case res.upErr != nil:
		return "up_err", false
	case u2c > 0 && (flowSpan >= f.minActiveSpan || total >= f.minActiveBytes):
		return "active", true
	case res.clientErr != nil:
		return "client_err", false
	case total == 0 && res.dur < 2*time.Second:
		// Accepted TCP but closed immediately with zero bytes: a rejecting/dead endpoint.
		return "no_data", false
	case total == 0:
		// The client connected but never sent a byte for a while: switching IPs cannot
		// help a silent client, and the IP did nothing wrong — neutral.
		return "idle", false
	case u2c == 0:
		return "no_response", false
	default:
		return "tiny", false
	}
}

// relayResult is the outcome of one relay segment.
type relayResult struct {
	dur       time.Duration
	upErr     error // upstream-side error → re-dial and continue
	clientErr error // client-side error → give up
	clientEOF bool  // clean client EOF (half-close; client can still receive)
	upEOF     bool  // clean upstream EOF
}

// relay copies bytes in both directions until both directions end.
//
// Exit matrix (each goroutine closes connections as needed to unblock the other;
// Close is idempotent):
//
//	A (client→upstream):
//	  client EOF        → close nothing (B keeps pushing to the half-closed client)
//	  client read error → close both
//	  upstream write err → close upstream (B then exits)
//	B (upstream→client):
//	  upstream EOF      → close both (A's pending write/read then exits)
//	  upstream read err → close upstream (A's next write fails and exits; if A is
//	                      blocked reading the client, it exits on the client's next
//	                      message — the connection stays alive meanwhile)
//	  client write err  → close both
//
// The upstream socket carries TCP_USER_TIMEOUT: on a dead path reads/writes error
// within ~noDataTimeout (upErr); idle connections on a live path are never killed.
func (f *Forwarder) relay(a, b net.Conn, seg *segInfo) relayResult {
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan dirErr, 2)
	stop := make(chan struct{})
	defer close(stop)

	// Zombie guard: once the client has fully closed (clean EOF) and the upstream
	// sends no NEW data within noDataTimeout, close the upstream so the segment
	// ends instead of lingering forever (frees the fd and keeps the active list
	// accurate). The client is gone, so this never penalizes the IP — the relay
	// ends with a plain clientEOF verdict.
	var clientEOFat atomic.Int64 // UnixNano when A saw the clean client EOF
	var u2cAtEOF atomic.Int64    // u2c counter at that moment
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				t0 := clientEOFat.Load()
				if t0 != 0 && seg.u2c.Load() == u2cAtEOF.Load() &&
					time.Since(time.Unix(0, t0)) > f.noDataTimeout {
					b.Close()
					return
				}
			}
		}
	}()

	// closeFor closes connections according to the failing side:
	// client-side error → close both (client is gone, nothing to resume);
	// upstream-side error → close only upstream (client stays open for re-dial).
	closeFor := func(dir string) {
		if dir == "client" {
			a.Close()
			b.Close()
		} else {
			b.Close()
		}
	}

	copyDir := func(dir string, src, dst net.Conn) {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			nr, rerr := src.Read(buf)
			if nr > 0 {
				if _, werr := dst.Write(buf[:nr]); werr != nil {
					// Write failed: the error belongs to the target side.
					errCh <- dirErr{side: otherSide(dir), err: werr}
					closeFor(otherSide(dir))
					return
				}
				if dir == "client" {
					seg.c2u.Add(int64(nr))
				} else {
					seg.u2c.Add(int64(nr))
				}
				seg.touch()
			}
			if rerr != nil {
				if rerr == io.EOF {
					errCh <- dirErr{side: dir, eof: true}
					if dir == "up" {
						// Upstream closed cleanly: the whole relay is over.
						a.Close()
						b.Close()
					} else {
						// Clean client EOF: arm the zombie guard.
						clientEOFat.Store(time.Now().UnixNano())
						u2cAtEOF.Store(seg.u2c.Load())
					}
				} else {
					// Read failed: the error belongs to the source side.
					errCh <- dirErr{side: dir, err: rerr}
					closeFor(dir)
				}
				return
			}
		}
	}

	wg.Add(2)
	go copyDir("client", a, b) // read client → write upstream
	go copyDir("up", b, a)     // read upstream → write client
	wg.Wait()

	res := relayResult{dur: time.Since(start)}
	// Combine both directions. Critical rules:
	// 1. errors.Is(net.ErrClosed) is a fake error caused by our own closes → ignore.
	// 2. If any real error exists, every EOF is suspect (our closes can produce fake
	//    EOFs) and is dropped. Example: upstream RST → A's write fails (upErr), A
	//    closes upstream → B reads a fake EOF; keeping upEOF would misread "upstream
	//    broke" as "server closed cleanly" and skip the switch.
	for i := 0; i < 2; i++ {
		e := <-errCh
		if e.eof {
			if e.side == "client" {
				res.clientEOF = true
			} else {
				res.upEOF = true
			}
			continue
		}
		if errors.Is(e.err, net.ErrClosed) {
			continue
		}
		if e.side == "client" {
			if res.clientErr == nil {
				res.clientErr = e.err
			}
		} else {
			if res.upErr == nil {
				res.upErr = e.err
			}
		}
	}
	if res.upErr != nil || res.clientErr != nil {
		res.upEOF = false
		res.clientEOF = false
	}
	return res
}

func otherSide(dir string) string {
	if dir == "client" {
		return "up"
	}
	return "client"
}

type dirErr struct {
	side string // "client" or "up"
	err  error
	eof  bool
}
