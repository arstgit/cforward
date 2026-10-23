package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
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
	pool          *Pool
	targetPort    int
	dialTimeout   time.Duration
	noDataTimeout time.Duration
}

func NewForwarder(pool *Pool, targetPort int, dialTimeout, noDataTimeout time.Duration) *Forwarder {
	return &Forwarder{
		pool:          pool,
		targetPort:    targetPort,
		dialTimeout:   dialTimeout,
		noDataTimeout: noDataTimeout,
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

// dialBest dials candidates in score order, skipping the IPs in skip (IPs that
// failed are added to skip so they are not retried within one client connection).
// Returns the connection, the winning IP, the failed-attempt count, and the dial
// latency of the winning IP in ms.
func (f *Forwarder) dialBest(skip map[string]bool) (net.Conn, *IPEntry, int, float64) {
	candidates := f.pool.Candidates()
	retries := 0
	for _, cand := range candidates {
		if skip[cand.IP] {
			continue
		}
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
			continue
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tuneUpstream(tc, f.noDataTimeout)
		}
		f.pool.ReportDial(ip, true, latMs)
		return conn, cand, retries, latMs
	}
	return nil, nil, retries, 0
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
		// ok = no upstream-side error (client-side disconnects do not count against the IP)
		ok := res.upErr == nil
		speed := 0.0
		if res.dur > 0 {
			speed = float64(c2u+u2c) / res.dur.Seconds() / 1e6 // MB/s
		}
		f.pool.ReportConn(used.IP, ok, c2u, res.dur, retries)
		slog.Info("forward end",
			"client", clientAddr, "ip", used.IP, "ok", ok,
			"lat_ms", seg.dialLatMS, "dur", res.dur.Round(time.Millisecond),
			"up_bytes", c2u, "down_bytes", u2c,
			"speed_mbps", round2(speed), "switches", switchCount, "retries", retries)

		// Upstream ended normally (EOF) or the client is gone → connection over.
		if res.upErr == nil || res.clientErr != nil || res.upEOF {
			return
		}

		// Upstream broke: switch to the next-best IP and keep relaying.
		skip[used.IP] = true
		switchCount++
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
			}
			if rerr != nil {
				if rerr == io.EOF {
					errCh <- dirErr{side: dir, eof: true}
					if dir == "up" {
						// Upstream closed cleanly: the whole relay is over.
						a.Close()
						b.Close()
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
