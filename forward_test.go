package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- 假上游 ----

type fakeUpstream struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func listenOn(t *testing.T, ip string, port int) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", net.JoinHostPort(ip, itoa(port)))
	if err != nil {
		t.Fatalf("listen %s:%d: %v", ip, port, err)
	}
	return ln
}

func newFakeUpstream(t *testing.T, ln net.Listener) *fakeUpstream {
	u := &fakeUpstream{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			u.mu.Lock()
			u.conns = append(u.conns, c)
			u.mu.Unlock()
			go io.Copy(c, c) // 回显
		}
	}()
	return u
}

// die 模拟 IP 整体死亡：关 listener（新拨号被拒）+ 以 linger(0) 掐断所有连接。
// 注意：Linux 上 linger(0) 可能发 RST（转发器同连接续传）也可能发 FIN（转发器视为
// 服务端正常关闭 → 关客户端连接），两种都是合法语义，测试两者都覆盖。
func (u *fakeUpstream) die() {
	u.ln.Close()
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, c := range u.conns {
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetLinger(0)
		}
		c.Close()
	}
}

// ---- 核心需求：上游断流 → 快速换 IP 恢复转发 ----

func TestSwitchOnUpstreamDeath(t *testing.T) {
	// 找一个空闲端口，两个假上游共用（同端口不同 IP，模拟 CF 的 IP:443 形态）
	port := freePort(t)

	up1 := newFakeUpstream(t, listenOn(t, "127.0.0.1", port))
	up2 := newFakeUpstream(t, listenOn(t, "127.0.0.2", port))
	defer up1.ln.Close()
	defer up2.ln.Close()

	pool := NewPool(t.TempDir()+"/pool.json", 200, 5)
	// up1 延迟更低 → 分更高，先被选中
	pool.AddCfstResults([]CfstResult{
		{IP: "127.0.0.1", Latency: 10, Speed: 50},
		{IP: "127.0.0.2", Latency: 20, Speed: 40},
	})

	fw := NewForwarder(pool, port, time.Second, 2*time.Second)
	ln := listenOn(t, "127.0.0.1", 0)
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fw.Serve(ctx, ln)

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(client)

	// 第一段：数据走 up1（最高分）
	if !echoRound(t, br, client, "hello-1") {
		t.Fatal("first echo failed")
	}

	// 上游 up1 死亡
	up1.die()

	// 恢复转发：
	// RST 路径 → 同一客户端连接自动换 up2 续传（echo 直接成功）；
	// FIN 路径 → 转发器关客户端连接，客户端重连后拨号落到 up2（up1 已拒绝）。
	recovered := false
	for attempt := 0; attempt < 8 && !recovered; attempt++ {
		if echoRound(t, br, client, "hello-2") {
			recovered = true
			break
		}
		client.Close()
		client, err = net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		br = bufio.NewReader(client)
	}
	defer client.Close()
	if !recovered {
		t.Fatal("no echo after upstream death; forwarder did not recover")
	}

	// 验证统计：up1 有传输失败或拨号失败；up2 被成功拨号并接管
	time.Sleep(200 * time.Millisecond)
	var p1, p2 *IPEntry
	for _, e := range pool.Candidates() {
		switch e.IP {
		case "127.0.0.1":
			p1 = e
		case "127.0.0.2":
			p2 = e
		}
	}
	if p1 == nil {
		t.Fatalf("up1 entry missing")
	}
	if p1.TransFail == 0 && !(p1.Attempts > 0 && p1.Successes == 0) {
		t.Fatalf("up1 should have a transport or dial failure: %+v", p1)
	}
	if p2 == nil || p2.Successes == 0 {
		t.Fatalf("up2 should have been dialed successfully after switch: %+v", p2)
	}
	t.Logf("switch OK: up1 trans_fail=%d cum_fail=%d dial=%d/%d | up2 succ=%d lat=%.1fms",
		p1.TransFail, p1.CumFail, p1.Successes, p1.Attempts, p2.Successes, p2.LatEMA)
}

// 拨号阶段失败立即降级：最高分 IP 已死（连接被拒）→ 新客户端连接应落到下一个
func TestDialFallback(t *testing.T) {
	port := freePort(t)

	// up1 分数更高但已死（端口无监听）；up2 存活
	up2 := newFakeUpstream(t, listenOn(t, "127.0.0.2", port))
	defer up2.ln.Close()

	pool := NewPool(t.TempDir()+"/pool.json", 200, 5)
	pool.AddCfstResults([]CfstResult{
		{IP: "127.0.0.1", Latency: 5, Speed: 60}, // 更高分
		{IP: "127.0.0.2", Latency: 20, Speed: 40},
	})

	fw := NewForwarder(pool, port, time.Second, 2*time.Second)
	ln := listenOn(t, "127.0.0.1", 0)
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fw.Serve(ctx, ln)

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	br := bufio.NewReader(client)
	if !echoRound(t, br, client, "hello-fallback") {
		t.Fatal("no echo; fallback dial failed")
	}

	time.Sleep(100 * time.Millisecond)
	p1, _ := poolTop(pool, "127.0.0.1")
	p2, _ := poolTop(pool, "127.0.0.2")
	if p1 == nil || p1.Attempts == 0 || p1.Successes != 0 {
		t.Fatalf("dead up1 should have a failed dial recorded: %+v", p1)
	}
	if p2 == nil || p2.Successes == 0 {
		t.Fatalf("up2 should have been dialed successfully: %+v", p2)
	}
	t.Logf("fallback OK: up1 attempts=%d fails=%d | up2 success=%d",
		p1.Attempts, p1.Attempts-p1.Successes, p2.Successes)
}

func echoRound(t *testing.T, br *bufio.Reader, w io.Writer, msg string) bool {
	t.Helper()
	if _, err := w.Write([]byte(msg + "\n")); err != nil {
		return false
	}
	// 每次尝试独立 deadline，避免一次超时拖死后续重试
	w.(net.Conn).SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := br.ReadString('\n')
	if err != nil {
		return false
	}
	return strings.TrimSpace(line) == msg
}

// ---- cfst stdout 解析 ----

func TestParseCfstOutput(t *testing.T) {
	// 远端实测输出（进度条噪声 + 结果表），-dd 模式速度列为 0.00
	out := `# XIU2/CloudflareSpeedTest v2.3.5 

开始延迟测速（模式：TCP, 端口：443, 范围：0 ~ 9999 ms, 丢包：1.00)
1 / 3 [----------------------------↖______________________________________________________] 可用: 1  3 / 3 [----------------------------------------------------------------------------------] 可用: 3  IP 地址           已发送  已接收  丢包率  平均延迟  下载速度(MB/s)  地区码  
104.16.1.34       1       1       0.00    186.80    0.00            N/A     
104.21.41.113     1       1       0.00    206.66    0.00            N/A     
172.67.72.14      1       1       0.00    206.92    0.00            N/A     
`
	got, err := ParseCfstOutput([]byte(out))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 results, got %d: %+v", len(got), got)
	}
	if got[0].IP != "104.16.1.34" || got[0].Latency != 186.80 {
		t.Errorf("first: %+v", got[0])
	}
	if got[2].IP != "172.67.72.14" {
		t.Errorf("third: %+v", got[2])
	}

	// 带真实下载速度的表格（stdout 格式，非 CSV）
	tab := `IP 地址           已发送  已接收  丢包率  平均延迟  下载速度(MB/s)  地区码  
104.21.41.113     4       4       0.00    282.97    15.00           SJC     
104.16.1.34       4       4       0.00    272.78    12.73           LAX     
`
	got2, err := ParseCfstOutput([]byte(tab))
	if err != nil {
		t.Fatal(err)
	}
	if len(got2) != 2 || got2[0].Speed != 15.00 || got2[1].Speed != 12.73 {
		t.Fatalf("tab parse: %+v", got2)
	}

	// 噪声但无结果 → 报错
	if _, err := ParseCfstOutput([]byte("4 / 5955 [bar] 可用: 4\n")); err == nil {
		t.Fatal("want error on noise-only output")
	}
}

// ---- 计分 ----

func TestScoring(t *testing.T) {
	fast := &IPEntry{IP: "1.1.1.1", HasBase: true, BaseLatency: 50, BaseSpeed: 30}
	slow := &IPEntry{IP: "1.1.1.2", HasBase: true, BaseLatency: 500, BaseSpeed: 2}
	if fast.Score() <= slow.Score() {
		t.Fatalf("fast should outscore slow: %v vs %v", fast.Score(), slow.Score())
	}
	t.Logf("fast=%.1f slow=%.1f", fast.Score(), slow.Score())

	// 连败惩罚 ×0.1
	pen := &IPEntry{IP: "1.1.1.3", HasBase: true, BaseLatency: 50, BaseSpeed: 30}
	before := pen.Score()
	pen.ConsecFail = 3
	if got := pen.Score(); got != before*penaltyFactor {
		t.Fatalf("penalty: before=%.2f after=%.2f want %.2f", before, got, before*penaltyFactor)
	}

	// 实时样本接管：LatEMA/SpeedEMA 替换基线
	tk := &IPEntry{IP: "1.1.1.4", HasBase: true, BaseLatency: 50, BaseSpeed: 30,
		Samples: 5, LatEMA: 900, SpeedEMA: 30}
	// 接管后延迟分 = 100*(1-900/1000)=10，总分 ≈ 57；未接管则用基线 50ms，总分 ≈ 91
	if s := tk.Score(); s < 50 || s > 65 {
		t.Fatalf("live latency should dominate after 5 samples, score=%.1f (want ~57)", s)
	}
}

// ---- 池子生命周期 ----

func TestPoolEviction(t *testing.T) {
	pool := NewPool(t.TempDir()+"/pool.json", 200, 5)
	pool.AddCfstResults([]CfstResult{
		{IP: "1.1.1.1", Latency: 10, Speed: 10},
		{IP: "1.1.1.2", Latency: 10, Speed: 10},
	})

	// 累计失败 5 次 → 淘汰
	for i := 0; i < 5; i++ {
		pool.ReportDial("1.1.1.1", false, 0)
	}
	if pool.Size() != 1 {
		t.Fatalf("want 1 entry after eviction, got %d", pool.Size())
	}
	if _, ok := poolTop(pool, "1.1.1.2"); !ok {
		t.Fatal("1.1.1.2 should remain")
	}

	// 连续 3 轮未出现且无样本 → 淘汰
	pool.AddCfstResults([]CfstResult{{IP: "1.1.1.3", Latency: 10, Speed: 10}})
	pool.AddCfstResults([]CfstResult{{IP: "1.1.1.3", Latency: 10, Speed: 10}})
	pool.AddCfstResults([]CfstResult{{IP: "1.1.1.3", Latency: 10, Speed: 10}})
	if _, ok := poolTop(pool, "1.1.1.2"); ok {
		t.Fatal("stale 1.1.1.2 should be evicted after 3 rounds unseen")
	}

	// 池上限：max=2，入 3 个踢最低分
	small := NewPool(t.TempDir()+"/pool.json", 2, 5)
	small.AddCfstResults([]CfstResult{
		{IP: "1.1.1.1", Latency: 100, Speed: 1},
		{IP: "1.1.1.2", Latency: 50, Speed: 10},
		{IP: "1.1.1.3", Latency: 60, Speed: 9},
	})
	if small.Size() != 2 {
		t.Fatalf("pool should cap at 2, got %d", small.Size())
	}
	if _, ok := poolTop(small, "1.1.1.1"); ok {
		t.Fatal("lowest-scoring 1.1.1.1 should be evicted on overflow")
	}

	// 持久化往返
	pool.Save()
	loaded := NewPool(pool.file, 200, 5)
	if err := loaded.Load(); err != nil {
		t.Fatal(err)
	}
	if loaded.Size() != pool.Size() {
		t.Fatalf("persist roundtrip: want %d got %d", pool.Size(), loaded.Size())
	}
}

func poolTop(p *Pool, ip string) (*IPEntry, bool) {
	for _, e := range p.Candidates() {
		if e.IP == ip {
			return e, true
		}
	}
	return nil, false
}

func freePort(t *testing.T) int {
	t.Helper()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	probe.Close()
	return port
}

func itoa(n int) string { return strconv.Itoa(n) }
