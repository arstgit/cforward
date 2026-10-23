package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- Fake upstream ----

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
			go io.Copy(c, c) // echo back
		}
	}()
	return u
}

// closeConns closes the accepted connections (ends open relay segments) without
// touching the listener.
func (u *fakeUpstream) closeConns() {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, c := range u.conns {
		c.Close()
	}
}

// die simulates a dead IP: closes the listener (new dials are refused) and kills all
// connections with linger(0). Note: on Linux linger(0) may send RST (forwarder resumes
// on the same client connection) or FIN (forwarder treats it as a clean server close and
// closes the client connection); both are legal semantics, so the test covers both.
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

// ---- Core requirement: upstream breaks → fast switch to another IP ----

func TestSwitchOnUpstreamDeath(t *testing.T) {
	// Find a free port shared by both fake upstreams (same port, different IPs, mimicking CF IP:443).
	port := freePort(t)

	up1 := newFakeUpstream(t, listenOn(t, "127.0.0.1", port))
	up2 := newFakeUpstream(t, listenOn(t, "127.0.0.2", port))
	defer up1.ln.Close()
	defer up2.ln.Close()

	pool := NewPool(t.TempDir()+"/pool.json", 200, 5)
	// up1 has lower latency → higher score → picked first
	pool.AddCfstResults([]CfstResult{
		{IP: "127.0.0.1", Latency: 10, Speed: 50},
		{IP: "127.0.0.2", Latency: 20, Speed: 40},
	})

	fw := NewForwarder(pool, port, time.Second, 2*time.Second, time.Minute, time.Hour, time.Hour, 1)
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

	// Segment 1: traffic goes through up1 (highest score)
	if !echoRound(t, br, client, "hello-1") {
		t.Fatal("first echo failed")
	}

	// up1 dies
	up1.die()

	// Recovery:
	// RST path → same client connection automatically resumes via up2 (echo succeeds directly);
	// FIN path → forwarder closes the client connection; the client reconnects and the dial
	// falls through to up2 (up1 now refuses).
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
	// End the live up2 segment so its verdict is reported before we assert.
	up2.closeConns()

	// Verify stats: up1 has a transport or dial failure; up2 was dialed successfully and took over
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

// Dial-stage fallback: the highest-scoring IP is dead (connection refused) → the new client connection must land on the next one
func TestDialFallback(t *testing.T) {
	port := freePort(t)

	// up1 scores higher but is dead (nothing listening); up2 is alive
	up2 := newFakeUpstream(t, listenOn(t, "127.0.0.2", port))
	defer up2.ln.Close()

	pool := NewPool(t.TempDir()+"/pool.json", 200, 5)
	pool.AddCfstResults([]CfstResult{
		{IP: "127.0.0.1", Latency: 5, Speed: 60}, // higher score
		{IP: "127.0.0.2", Latency: 20, Speed: 40},
	})

	fw := NewForwarder(pool, port, time.Second, 2*time.Second, time.Minute, time.Hour, time.Hour, 1)
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
	up2.closeConns() // end the segment so its verdict is reported

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
	// Fresh deadline per attempt, so one timeout does not kill the remaining retries
	w.(net.Conn).SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := br.ReadString('\n')
	if err != nil {
		return false
	}
	return strings.TrimSpace(line) == msg
}

// ---- cfst stdout parsing ----

// Regression: relative path + cmd.Dir resolution bug (the child chdirs first, then the
// relative path is resolved a second time → ENOTDIR). A fake script stands in for cfst
// (the real cfst is never executed).
func TestRunCfstRelativePath(t *testing.T) {
	dir := t.TempDir()
	cfstDir := filepath.Join(dir, "cfst")
	if err := os.MkdirAll(cfstDir, 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(cfstDir, "cfst")
	fake := "#!/bin/sh\nprintf '1 / 3 [bar] 可用: 1  IP 地址           已发送  已接收  丢包率  平均延迟  下载速度(MB/s)  地区码  \\n104.16.1.34       1       1       0.00    186.80    0.00            N/A     \\n'\n"
	if err := os.WriteFile(script, []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}

	// Reproduce the deployment shape: temp dir as cwd, relative paths
	t.Chdir(dir)
	results, err := RunCfst(context.Background(), "./cfst", "./cfst/cfst", CfstOpts{
		URL:     "https://speed.cloudflare.com/__down?bytes=3000000",
		Threads: 100, Tests: 2, DLCount: 10, DLTime: 10, Top: 20,
	})
	if err != nil {
		t.Fatalf("RunCfst with relative paths failed: %v", err)
	}
	if len(results) != 1 || results[0].IP != "104.16.1.34" {
		t.Fatalf("unexpected results: %+v", results)
	}
}

// Config loading: JSON file merged over defaults
func TestLoadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	content := `{
		"listen": ":9999",
		"pool_min": 50,
		"cooldown": "10m",
		"cfst_threads": 200,
		"cfst_dl_time": 20,
		"debug": true
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	// Explicit fields take effect
	if cfg.Listen != ":9999" || cfg.PoolMin != 50 || cfg.CfstThreads != 200 || !cfg.Debug {
		t.Fatalf("explicit fields: %+v", cfg)
	}
	// Duration strings parse
	if cfg.Cooldown.Std() != 10*time.Minute {
		t.Fatalf("cooldown: %v", cfg.Cooldown.Std())
	}
	// Unset fields keep defaults
	if cfg.PoolMax != 200 || cfg.CfstTop != 20 || cfg.NoDataTimeout.Std() != 10*time.Second {
		t.Fatalf("defaults: %+v", cfg)
	}
}

// ---- cfst download-test-less (-dd) mode ----

func TestCfstArgsDD(t *testing.T) {
	if !defaultConfig().CfstDD {
		t.Fatal("download speed test must be off by default")
	}
	base := CfstOpts{URL: "https://x", Threads: 100, Tests: 2, DLCount: 10, DLTime: 10, Top: 20}
	args := cfstArgs(base)
	for _, a := range args {
		if a == "-dd" {
			t.Fatal("-dd must not be present when DD is false")
		}
	}

	base.DD = true
	args = cfstArgs(base)
	found := false
	for _, a := range args {
		if a == "-dd" {
			found = true
		}
	}
	if !found {
		t.Fatalf("-dd missing in args: %v", args)
	}
}

// In -dd mode cfst reports speed 0.00, so the initial score carries no speed term:
// ranking stays latency-monotonic (identical to download-test mode) and the live
// throughput EMA fills the speed score in after a few connections.
func TestScoringDDMode(t *testing.T) {
	lowLat := &IPEntry{IP: "1.1.1.1", HasBase: true, BaseLatency: 60, BaseSpeed: 0}
	highLat := &IPEntry{IP: "1.1.1.2", HasBase: true, BaseLatency: 300, BaseSpeed: 0}
	if lowLat.Score() <= highLat.Score() {
		t.Fatalf("dd-mode ranking must be latency-monotonic: %v vs %v",
			lowLat.Score(), highLat.Score())
	}
	// No speed term: score = 0.4L + 0.2U + 0.1R
	want := 0.4*100*(1-60.0/latNormMs) + 0.2*100 + 0.1*100
	if got := lowLat.Score(); got != want {
		t.Fatalf("dd-mode score: got %.3f want %.3f", got, want)
	}
	// Live speed takes over after liveSamplesTakeover connections.
	live := &IPEntry{IP: "1.1.1.3", HasBase: true, BaseLatency: 60, BaseSpeed: 0,
		Samples: 5, LatEMA: 60, SpeedEMA: 30}
	if live.Score() <= lowLat.Score() {
		t.Fatalf("live speed should raise the score above the no-speed baseline")
	}
}

func TestFindArg(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"-listen", ":9", "-config", "a.json"}, "a.json"},
		{[]string{"-config=a.json", "-listen", ":9"}, "a.json"},
		{[]string{"--config", "b.json"}, "b.json"},
		{[]string{"-listen", ":9"}, ""},
	}
	for _, c := range cases {
		if got := findArg(c.args, "config"); got != c.want {
			t.Fatalf("findArg(%v) = %q, want %q", c.args, got, c.want)
		}
	}
}

func TestParseCfstOutput(t *testing.T) {
	// Real captured output (progress-bar noise + result table), -dd mode: speed column is 0.00
	// Fixture: cfst's real stdout is Chinese (upstream project); keep verbatim.
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

	// Table with real download speeds (stdout format, not CSV)
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

	// Noise only, no results → error
	if _, err := ParseCfstOutput([]byte("4 / 5955 [bar] 可用: 4\n")); err == nil {
		t.Fatal("want error on noise-only output")
	}
}

// ---- Scoring ----

func TestScoring(t *testing.T) {
	fast := &IPEntry{IP: "1.1.1.1", HasBase: true, BaseLatency: 50, BaseSpeed: 30}
	slow := &IPEntry{IP: "1.1.1.2", HasBase: true, BaseLatency: 500, BaseSpeed: 2}
	if fast.Score() <= slow.Score() {
		t.Fatalf("fast should outscore slow: %v vs %v", fast.Score(), slow.Score())
	}
	t.Logf("fast=%.1f slow=%.1f", fast.Score(), slow.Score())

	// Consecutive-failure penalty ×0.1
	pen := &IPEntry{IP: "1.1.1.3", HasBase: true, BaseLatency: 50, BaseSpeed: 30}
	before := pen.Score()
	pen.ConsecFail = 3
	if got := pen.Score(); got != before*penaltyFactor {
		t.Fatalf("penalty: before=%.2f after=%.2f want %.2f", before, got, before*penaltyFactor)
	}

	// Live sample takeover: LatEMA/SpeedEMA replace the baseline
	tk := &IPEntry{IP: "1.1.1.4", HasBase: true, BaseLatency: 50, BaseSpeed: 30,
		Samples: 5, LatEMA: 900, SpeedEMA: 30}
	// After takeover: latency score = 100*(1-900/1000)=10, total ≈ 57; without takeover the
	// 50ms baseline would give ≈ 91.
	if s := tk.Score(); s < 50 || s > 65 {
		t.Fatalf("live latency should dominate after 5 samples, score=%.1f (want ~57)", s)
	}
}

// ---- Pool lifecycle ----

func TestPoolEviction(t *testing.T) {
	pool := NewPool(t.TempDir()+"/pool.json", 200, 5)
	pool.AddCfstResults([]CfstResult{
		{IP: "1.1.1.1", Latency: 10, Speed: 10},
		{IP: "1.1.1.2", Latency: 10, Speed: 10},
	})

	// 5 cumulative failures → evicted
	for i := 0; i < 5; i++ {
		pool.ReportDial("1.1.1.1", false, 0)
	}
	if pool.Size() != 1 {
		t.Fatalf("want 1 entry after eviction, got %d", pool.Size())
	}
	if _, ok := poolTop(pool, "1.1.1.2"); !ok {
		t.Fatal("1.1.1.2 should remain")
	}

	// Unseen for 3 rounds with no samples → evicted
	pool.AddCfstResults([]CfstResult{{IP: "1.1.1.3", Latency: 10, Speed: 10}})
	pool.AddCfstResults([]CfstResult{{IP: "1.1.1.3", Latency: 10, Speed: 10}})
	pool.AddCfstResults([]CfstResult{{IP: "1.1.1.3", Latency: 10, Speed: 10}})
	if _, ok := poolTop(pool, "1.1.1.2"); ok {
		t.Fatal("stale 1.1.1.2 should be evicted after 3 rounds unseen")
	}

	// Pool cap: max=2, adding 3 evicts the lowest
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

	// Persistence round-trip
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

// ---- Active-traffic gold standard ----

// classify 单元测试：只有"有变化的流量"才算成功。
func TestClassify(t *testing.T) {
	f := NewForwarder(nil, 443, time.Second, 10*time.Second, time.Minute, 2*time.Second, time.Hour, 64*1024)
	cases := []struct {
		name     string
		res      relayResult
		up, down int64
		span     time.Duration
		wantOK   bool
		wantDead bool
	}{
		{"sustained flow", relayResult{upEOF: true}, 300, 1200, 3 * time.Second, true, false},
		{"big volume burst", relayResult{upEOF: true}, 4096, 128 * 1024, 200 * time.Millisecond, true, false},
		{"one 200 only", relayResult{upEOF: true}, 300, 250, 50 * time.Millisecond, false, true},
		{"no data quick close", relayResult{upEOF: true}, 300, 0, 0, false, true},
		{"rejected before send", relayResult{upEOF: true}, 0, 0, 0, false, true},
		{"client silent", relayResult{upEOF: true}, 0, 0, 0, false, false},
		{"transport error", relayResult{upErr: errTest}, 300, 0, 0, false, true},
		{"client rst", relayResult{clientErr: errTest}, 0, 0, 0, false, false},
		{"client rst after traffic", relayResult{clientErr: errTest}, 1024, 200 * 1024, 4 * time.Second, true, false},
	}
	for _, c := range cases {
		if c.name == "client silent" {
			c.res.dur = 5 * time.Second
		}
		v, ok, dead := f.classify(c.res, c.up, c.down, c.span)
		if ok != c.wantOK || dead != c.wantDead {
			t.Errorf("%s: verdict=%s ok=%v dead=%v want ok=%v dead=%v", c.name, v, ok, dead, c.wantOK, c.wantDead)
		}
	}
}

// 只有活跃段才算成功：拨号成功但无活跃流量 → U=0。
func TestTCPConnectivityNotSuccess(t *testing.T) {
	e := &IPEntry{Attempts: 5, Successes: 0} // 5 次拨号全部连通，但没有任何活跃段
	if e.successRate() != 0 {
		t.Fatalf("TCP-only connectivity must not count: rate=%v", e.successRate())
	}
	e2 := &IPEntry{Attempts: 5, Successes: 3}
	if e2.successRate() != 0.6 {
		t.Fatalf("rate: %v", e2.successRate())
	}
}

// 端到端：只回一个小应答的 IP 不算好，连续小应答后分数被打折、切到真能传流量的 IP。
func TestTinyResponseNotActive(t *testing.T) {
	port := freePort(t)

	// up1: 收到请求只回 100 字节就关闭（"一个 200"）
	ln1 := listenOn(t, "127.0.0.1", port)
	defer ln1.Close()
	go func() {
		for {
			c, err := ln1.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, 1024)
				c.Read(buf)
				c.Write(make([]byte, 100))
				c.Close()
			}(c)
		}
	}()

	// up2: 回 1500 字节（≥1024 阈值 → 活跃）
	ln2 := listenOn(t, "127.0.0.2", port)
	defer ln2.Close()
	go func() {
		for {
			c, err := ln2.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, 1024)
				c.Read(buf)
				c.Write(make([]byte, 1500))
				c.Close()
			}(c)
		}
	}()

	pool := NewPool(t.TempDir()+"/pool.json", 200, 5)
	pool.AddCfstResults([]CfstResult{
		{IP: "127.0.0.1", Latency: 10, Speed: 50}, // 高分
		{IP: "127.0.0.2", Latency: 500, Speed: 5},
	})

	fw := NewForwarder(pool, port, time.Second, 2*time.Second, time.Minute, time.Hour, time.Hour, 1024)
	ln := listenOn(t, "127.0.0.1", 0)
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fw.Serve(ctx, ln)

	dial := func() (net.Conn, []byte) {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		c.Write([]byte("ping"))
		buf := make([]byte, 2048)
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _ := c.Read(buf)
		c.Close()
		return c, buf[:n]
	}

	// 第一段撞 up1：只回 100B 小应答 → tiny → 失败 + 拉黑
	if _, n := dial(); len(n) != 100 {
		t.Fatalf("round 1: want 100B tiny response, got %dB", len(n))
	}
	time.Sleep(150 * time.Millisecond) // let the segment report land
	p1, _ := poolTop(pool, "127.0.0.1")
	if p1 == nil || p1.CumFail == 0 || !p1.DeadUntil.After(time.Now()) {
		t.Fatalf("up1 should be failed and dead-flagged after one tiny segment: %+v", p1)
	}

	// 第二段：up1 已被拉黑 → 直接落到 up2，拿到足量数据（活跃）
	if _, n := dial(); len(n) != 1500 {
		t.Fatalf("round 2: want 1500B from up2, got %dB", len(n))
	}
	time.Sleep(150 * time.Millisecond) // let the segment report land
	if p1.Attempts != 1 {
		t.Fatalf("up1 must not be dialed again while dead: attempts=%d", p1.Attempts)
	}
	p2, _ := poolTop(pool, "127.0.0.2")
	if p2 == nil || p2.Successes == 0 {
		t.Fatalf("up2 should have an active success: %+v", p2)
	}
	t.Logf("tiny→dead-skip→switch OK: up1 attempts=%d cum_fail=%d | up2 successes=%d",
		p1.Attempts, p1.CumFail, p2.Successes)
	t.Logf("tiny→penalized→switch OK: up1 consec=%d | up2 successes=%d", p1.ConsecFail, p2.Successes)
}

// 端到端：完全无数据的快速关闭 → 立即拉黑，下一个连接直接跳过。
func TestNoDataMarkedDead(t *testing.T) {
	port := freePort(t)

	// up1: accept 后立刻关闭（TCP 连通但零数据）
	ln1 := listenOn(t, "127.0.0.1", port)
	defer ln1.Close()
	go func() {
		for {
			c, err := ln1.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { c.Close() }(c)
		}
	}()
	up2 := newFakeUpstream(t, listenOn(t, "127.0.0.2", port))
	defer up2.ln.Close()

	pool := NewPool(t.TempDir()+"/pool.json", 200, 5)
	pool.AddCfstResults([]CfstResult{
		{IP: "127.0.0.1", Latency: 10, Speed: 50}, // 更高分
		{IP: "127.0.0.2", Latency: 500, Speed: 5},
	})

	fw := NewForwarder(pool, port, time.Second, 2*time.Second, time.Minute, time.Hour, time.Hour, 1)
	ln := listenOn(t, "127.0.0.1", 0)
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fw.Serve(ctx, ln)

	// 第一个连接撞 up1：无数据 → 失败 + 拉黑
	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c1.Write([]byte("ping"))
	c1.SetReadDeadline(time.Now().Add(time.Second))
	io.Copy(io.Discard, c1)
	c1.Close()
	time.Sleep(200 * time.Millisecond)

	p1, _ := poolTop(pool, "127.0.0.1")
	if p1 == nil || p1.CumFail == 0 || !p1.DeadUntil.After(time.Now()) {
		t.Fatalf("up1 should be failed and dead-flagged: %+v", p1)
	}

	// 第二个连接：必须跳过 up1 落到 up2
	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c2)
	if !echoRound(t, br, c2, "hello") {
		t.Fatal("second connection should reach up2")
	}
	c2.Close()
	up2.closeConns() // end the up2 segment so its verdict is reported
	time.Sleep(200 * time.Millisecond)
	if p1.Attempts != 1 {
		t.Fatalf("up1 must not be dialed again while dead: attempts=%d", p1.Attempts)
	}
	p2, _ := poolTop(pool, "127.0.0.2")
	if p2 == nil || p2.Successes == 0 {
		t.Fatalf("up2 should have served: %+v", p2)
	}
	t.Logf("no-data → dead-skip OK: up1 attempts=%d cum_fail=%d | up2 successes=%d",
		p1.Attempts, p1.CumFail, p2.Successes)
}

// 客户端自己断开不算 IP 的过错（中性）。
func TestClientRSTNeutral(t *testing.T) {
	port := freePort(t)
	up := newFakeUpstream(t, listenOn(t, "127.0.0.1", port))
	defer up.ln.Close()

	pool := NewPool(t.TempDir()+"/pool.json", 200, 5)
	pool.AddCfstResults([]CfstResult{{IP: "127.0.0.1", Latency: 10, Speed: 50}})

	fw := NewForwarder(pool, port, time.Second, 2*time.Second, time.Minute, time.Hour, time.Hour, 1)
	ln := listenOn(t, "127.0.0.1", 0)
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fw.Serve(ctx, ln)

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tc := c.(*net.TCPConn)
	tc.SetLinger(0) // RST：客户端过错
	c.Close()
	time.Sleep(300 * time.Millisecond)

	e, _ := poolTop(pool, "127.0.0.1")
	if e == nil || e.CumFail != 0 || e.Successes != 0 {
		t.Fatalf("client RST must be neutral: %+v", e)
	}
}

var errTest = errors.New("test error")

// 优先已验证 IP：高分但从未跑过活跃流量的 IP 排在"近期有活跃流量"的 IP 之后。
func TestPreferProvenIP(t *testing.T) {
	port := freePort(t)
	upA := newFakeUpstream(t, listenOn(t, "127.0.0.1", port))
	defer upA.ln.Close()
	upB := newFakeUpstream(t, listenOn(t, "127.0.0.2", port))
	defer upB.ln.Close()

	pool := NewPool(t.TempDir()+"/pool.json", 200, 5)
	pool.AddCfstResults([]CfstResult{
		{IP: "127.0.0.1", Latency: 5, Speed: 60},  // 更高分但未验证
		{IP: "127.0.0.2", Latency: 500, Speed: 5}, // 低分但最近有活跃流量
	})
	pool.mu.Lock()
	pool.entries["127.0.0.2"].LastActive = time.Now()
	pool.mu.Unlock()

	fw := NewForwarder(pool, port, time.Second, 2*time.Second, time.Minute, time.Hour, time.Hour, 1)
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
	if !echoRound(t, br, client, "hello-proven") {
		t.Fatal("no echo; proven IP not reached")
	}
	upB.closeConns() // end the segment so stats land
	time.Sleep(150 * time.Millisecond)

	pA, _ := poolTop(pool, "127.0.0.1")
	pB, _ := poolTop(pool, "127.0.0.2")
	if pA == nil || pA.Attempts != 0 {
		t.Fatalf("unproven high-score IP must not be dialed first: %+v", pA)
	}
	if pB == nil || pB.Attempts != 1 {
		t.Fatalf("proven IP should be dialed first: %+v", pB)
	}
	t.Logf("proven-first OK: unproven attempts=%d | proven attempts=%d", pA.Attempts, pB.Attempts)
}
