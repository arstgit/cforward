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
// Returns the connection, the winning IP, and the number of failed attempts.
func (f *Forwarder) dialBest(skip map[string]bool) (net.Conn, *IPEntry, int) {
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
		return conn, cand, retries
	}
	return nil, nil, retries
}

// handle drives one client connection. Core loop:
// dial (fall back on failure) → relay → upstream breaks → re-dial the next-best IP
// → resume relaying, until the client disconnects.
func (f *Forwarder) handle(client net.Conn) {
	defer client.Close()
	activeConns.Add(1)
	defer activeConns.Add(-1)
	clientAddr := client.RemoteAddr().String()

	skip := make(map[string]bool)
	upstream, used, retries := f.dialBest(skip)
	if upstream == nil {
		slog.Warn("no candidates, rejecting", "client", clientAddr)
		return
	}

	switchCount := 0
	for {
		res := f.relay(client, upstream)
		upstream.Close()

		// ok = no upstream-side error (client-side disconnects do not count against the IP)
		f.pool.ReportConn(used.IP, res.upErr == nil, res.clientBytes, res.dur, retries)

		// Upstream ended normally (EOF) or the client is gone → connection over.
		if res.upErr == nil || res.clientErr != nil || res.upEOF {
			slog.Debug("connection ended", "client", clientAddr, "ip", used.IP,
				"up_err", res.upErr, "client_err", res.clientErr,
				"client_eof", res.clientEOF, "up_eof", res.upEOF)
			return
		}

		// Upstream broke: switch to the next-best IP and keep relaying.
		skip[used.IP] = true
		switchCount++
		slog.Info("upstream broken, switching", "from", used.IP,
			"client", clientAddr, "err", res.upErr, "switch_count", switchCount)

		newUp, newUsed, r := f.dialBest(skip)
		if newUp == nil {
			slog.Warn("no more candidates, dropping client", "client", clientAddr,
				"switch_count", switchCount)
			return
		}
		upstream, used, retries = newUp, newUsed, r
	}
}

// relayResult is the outcome of one relay segment.
type relayResult struct {
	clientBytes int64
	dur         time.Duration
	upErr       error // upstream-side error → re-dial and continue
	clientErr   error // client-side error → give up
	clientEOF   bool  // clean client EOF (half-close; client can still receive)
	upEOF       bool  // clean upstream EOF
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
func (f *Forwarder) relay(a, b net.Conn) relayResult {
	start := time.Now()
	var wg sync.WaitGroup
	var c2u, u2c int64
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

	copyDir := func(dir string, src, dst net.Conn, counter *int64) {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		var total int64
		for {
			nr, rerr := src.Read(buf)
			if nr > 0 {
				if _, werr := dst.Write(buf[:nr]); werr != nil {
					// Write failed: the error belongs to the target side.
					errCh <- dirErr{side: otherSide(dir), err: werr}
					closeFor(otherSide(dir))
					return
				}
				total += int64(nr)
				*counter = total
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
	go copyDir("client", a, b, &c2u) // read client → write upstream
	go copyDir("up", b, a, &u2c)     // read upstream → write client
	wg.Wait()

	res := relayResult{clientBytes: c2u, dur: time.Since(start)}
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
