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

// Linux TCP_USER_TIMEOUT（linux/tcp.h 中为 18）：数据/保活探测无 ACK 超过该时长即
// 判定路径死亡，读写报错。
const tcpUserTimeoutOpt = 18

// 活跃连接数（周期日志用）。
var activeConns atomic.Int64

// Forwarder 监听端口并转发到池内最高分 IP。
// 上游断流时保持客户端连接，立刻换下一个高分 IP 重拨继续转发。
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

// Serve 阻塞接受连接直到 ctx 取消。
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

// tuneUpstream 让内核在路径真正死亡时按时报错：
// keepalive + TCP_USER_TIMEOUT —— 数据或保活探测在约 noDataTimeout 内无 ACK
// 即视为路径死亡，读写返回错误（活跃与空闲连接都适用，活路径上的慢服务不误杀）。
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

// dialBest 按分数降序拨号；skip 中的 IP 跳过（失败过的 IP 会被加入 skip，同一条
// 客户端连接内不再重试）。返回连接、命中的 IP、本次尝试的失败次数。
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

// handle 一个客户端连接。核心循环：
// 拨号（失败降级）→ 转发 → 上游断 → 换下一个高分 IP 重拨 → 继续转发，直到客户端断开。
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

		// ok = 上游方向无错误（客户端侧断开不影响 IP 评价）
		f.pool.ReportConn(used.IP, res.upErr == nil, res.clientBytes, res.dur, retries)

		// 上游正常结束（EOF）或客户端已死 → 本连接结束
		if res.upErr == nil || res.clientErr != nil || res.upEOF {
			slog.Debug("connection ended", "client", clientAddr, "ip", used.IP,
				"up_err", res.upErr, "client_err", res.clientErr,
				"client_eof", res.clientEOF, "up_eof", res.upEOF)
			return
		}

		// 上游断了：换下一个高分 IP 继续转发
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

// relayResult 一段转发的结局。
type relayResult struct {
	clientBytes int64
	dur         time.Duration
	upErr       error // 上游方向错误 → 换 IP 重连继续
	clientErr   error // 客户端方向错误 → 放弃
	clientEOF   bool  // 客户端正常 EOF（半关闭，仍可接收）
	upEOF       bool  // 上游正常 EOF
}

// relay 双向拷贝，直到两方向都结束。
//
// 退出矩阵（每方向 goroutine 结束时按需关闭连接以解除对端阻塞，关闭幂等）：
//
//	A(client→upstream):
//	  client EOF → 不关（B 继续向半关闭的 client 回传）
//	  client 读错 → 关双方
//	  upstream 写错 → 关 upstream（B 随之退出）
//	B(upstream→client):
//	  upstream EOF → 关双方（A 的后续写/读随之退出）
//	  upstream 读错 → 关 upstream（A 的下次写报错退出；若 A 阻塞在 client 读上，
//	                    则等 client 下次发言时报错退出，期间连接保持）
//	  client 写错 → 关双方
//
// 上游 socket 带 TCP_USER_TIMEOUT：路径死亡时读写在 ~noDataTimeout 内报错（upErr），
// 活路径上的空闲连接永不误杀。
func (f *Forwarder) relay(a, b net.Conn) relayResult {
	start := time.Now()
	var wg sync.WaitGroup
	var c2u, u2c int64
	errCh := make(chan dirErr, 2)

	// closeFor 按失败方向关闭连接：
	// 错误在 client 侧 → 双方都关（客户端已死，无续传意义）
	// 错误在 up 侧 → 只关 upstream（client 留给重连续传）
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
					// 写目标失败：错误属于目标一侧
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
						// 上游主动关闭：整个转发结束，解除 A 的阻塞
						a.Close()
						b.Close()
					}
				} else {
					// 读源失败：错误属于源一侧
					errCh <- dirErr{side: dir, err: rerr}
					closeFor(dir)
				}
				return
			}
		}
	}

	wg.Add(2)
	go copyDir("client", a, b, &c2u) // 读 client → 写 upstream
	go copyDir("up", b, a, &u2c)     // 读 upstream → 写 client
	wg.Wait()

	res := relayResult{clientBytes: c2u, dur: time.Since(start)}
	// 合并两个方向的结果。关键规则：
	// 1. errors.Is(net.ErrClosed) 是"我们自己关闭对端"造成的伪错误，忽略；
	// 2. 一旦存在真实错误，任何 EOF 都可能是我们关闭连接造成的伪信号，丢弃。
	//    （例：上游 RST → A 写失败报 upErr，A 随即关 upstream → B 读到 EOF 伪信号，
	//     若保留 upEOF 会把"上游断了"误判成"服务端正常关闭"而不切换）
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
	side string // "client" 或 "up"
	err  error
	eof  bool
}
