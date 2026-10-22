package main

import (
	"encoding/json"
	"log/slog"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

// ---- 算法常量（不进 flag，调它们属于改算法） ----

const (
	// 计分权重
	weightLat   = 0.4
	weightSpeed = 0.3
	weightSucc  = 0.2
	weightRetry = 0.1

	// EMA 平滑系数（实时延迟/速度）
	emaAlpha = 0.3

	// 实时样本数达到该值后完全接管初始分，之前按样本数加权过渡
	liveSamplesTakeover = 5

	// 连续失败次数达到该值 → 分数打骨折
	consecFailPenalty = 3
	// 打骨折系数
	penaltyFactor = 0.1

	// cfst 每轮入池数量（-p 参数）
	cfstTopN    = 20
	initRetries = 0

	// 延迟归一化上限（1000ms → 0 分）
	latNormMs = 1000.0
	// 速度归一化饱和参数（20MB/s → 63 分）
	speedNorm = 20.0
)

// IPEntry 池中一个 IP 的全部状态。
type IPEntry struct {
	IP string `json:"ip"`

	// cfst 基线（每次 cfst 命中时刷新）
	BaseLatency float64 `json:"base_latency"` // ms
	BaseSpeed   float64 `json:"base_speed"`   // MB/s
	HasBase     bool    `json:"has_base"`

	// 实时统计
	Samples   int     `json:"samples"`    // 服务过的连接数
	LatEMA    float64 `json:"lat_ema"`    // 拨号耗时 EMA (ms)
	SpeedEMA  float64 `json:"speed_ema"`  // 吞吐 EMA (MB/s)
	Attempts  int     `json:"attempts"`   // 拨号尝试次数
	Successes int     `json:"successes"`  // 拨号成功次数
	TransFail int     `json:"trans_fail"` // 传输失败次数
	Retries   int     `json:"retries"`    // 归因到本 IP 的降级重试次数

	ConsecFail int `json:"consec_fail"` // 连续失败
	CumFail    int `json:"cum_fail"`    // 累计失败（淘汰依据）

	LastSeenRound int       `json:"last_seen_round"` // 最近一次出现在 cfst 结果的轮次
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// scoreFactor 当前是否处于连败惩罚中。
func (e *IPEntry) penalized() bool { return e.ConsecFail >= consecFailPenalty }

// latency 参与打分的延迟：样本不足时用 cfst 基线过渡。
func (e *IPEntry) latency() float64 {
	if e.Samples >= liveSamplesTakeover {
		return e.LatEMA
	}
	if e.HasBase {
		// 按样本数加权过渡：samples/5 权重给实时，剩余给基线
		w := float64(e.Samples) / liveSamplesTakeover
		return e.LatEMA*w + e.BaseLatency*(1-w)
	}
	return e.LatEMA
}

func (e *IPEntry) speed() float64 {
	if e.Samples >= liveSamplesTakeover {
		return e.SpeedEMA
	}
	if e.HasBase {
		w := float64(e.Samples) / liveSamplesTakeover
		return e.SpeedEMA*w + e.BaseSpeed*(1-w)
	}
	return e.SpeedEMA
}

// successRate 拨号成功且传输未失败的比率。
func (e *IPEntry) successRate() float64 {
	if e.Attempts == 0 {
		return 1.0
	}
	ok := float64(e.Successes - e.TransFail)
	if ok < 0 {
		ok = 0
	}
	return ok / float64(e.Attempts)
}

// retryRate 归因重试次数占（重试+成功连接）的比例。
func (e *IPEntry) retryRate() float64 {
	denom := float64(e.Retries + e.Samples)
	if denom == 0 {
		return 0
	}
	return float64(e.Retries) / denom
}

// Score 按公式计算总分：0.4L + 0.3S + 0.2U + 0.1R，连败惩罚 ×0.1。
func (e *IPEntry) Score() float64 {
	L := 100 * (1 - clamp(e.latency()/latNormMs, 0, 1))
	S := 100 * (1 - math.Exp(-e.speed()/speedNorm))
	U := 100 * e.successRate()
	R := 100 * (1 - e.retryRate())
	s := weightLat*L + weightSpeed*S + weightSucc*U + weightRetry*R
	if e.penalized() {
		s *= penaltyFactor
	}
	return s
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// ---- 池子 ----

type Pool struct {
	mu      sync.Mutex
	entries map[string]*IPEntry
	rounds  int // 成功完成 cfst 补池的轮次

	changed chan struct{} // 非阻塞变更通知（buffered 1）

	// 持久化
	file      string
	dirty     bool
	saveTimer *time.Timer

	// 限制（由配置注入）
	max       int
	failEvict int
}

func NewPool(file string, max, failEvict int) *Pool {
	p := &Pool{
		entries:   make(map[string]*IPEntry),
		changed:   make(chan struct{}, 1),
		file:      file,
		max:       max,
		failEvict: failEvict,
	}
	return p
}

func (p *Pool) notify() {
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

// Load 启动时从 pool.json 恢复。
func (p *Pool) Load() error {
	b, err := os.ReadFile(p.file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var list []*IPEntry
	if err := json.Unmarshal(b, &list); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range list {
		if e == nil || e.IP == "" {
			continue
		}
		p.entries[e.IP] = e
	}
	slog.Info("pool loaded from disk", "count", len(p.entries), "file", p.file)
	return nil
}

func (p *Pool) markDirty() {
	p.mu.Lock()
	p.dirty = true
	if p.saveTimer == nil {
		p.saveTimer = time.AfterFunc(5*time.Second, func() { p.Save() })
	}
	p.mu.Unlock()
}

// Save 写 pool.json（内部用，也供退出时 flush 调用）。
func (p *Pool) Save() {
	p.mu.Lock()
	if p.saveTimer != nil {
		p.saveTimer.Stop()
		p.saveTimer = nil
	}
	if !p.dirty {
		p.mu.Unlock()
		return
	}
	list := make([]*IPEntry, 0, len(p.entries))
	for _, e := range p.entries {
		list = append(list, e)
	}
	p.dirty = false
	p.mu.Unlock()

	sort.Slice(list, func(i, j int) bool { return list[i].Score() > list[j].Score() })
	b, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		slog.Error("marshal pool", "err", err)
		return
	}
	tmp := p.file + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		slog.Error("write pool tmp", "err", err)
		return
	}
	if err := os.Rename(tmp, p.file); err != nil {
		slog.Error("rename pool", "err", err)
	}
}

// Close flush 并停止后台保存。
func (p *Pool) Close() {
	p.mu.Lock()
	if p.saveTimer != nil {
		p.saveTimer.Stop()
		p.saveTimer = nil
	}
	p.mu.Unlock()
	p.Save()
}

// CfstResult 一行 cfst 结果。
type CfstResult struct {
	IP      string
	Latency float64 // ms
	Speed   float64 // MB/s
}

// AddCfstResults 把一轮 cfst 结果并入池子：
// 新 IP 用 cfst 数据建初始分；已有 IP 只刷新基线。
// 返回新增/刷新数量。
func (p *Pool) AddCfstResults(results []CfstResult) (added, refreshed int) {
	p.mu.Lock()
	p.rounds++
	round := p.rounds
	now := time.Now()
	for _, r := range results {
		e, ok := p.entries[r.IP]
		if !ok {
			e = &IPEntry{
				IP:            r.IP,
				BaseLatency:   r.Latency,
				BaseSpeed:     r.Speed,
				HasBase:       true,
				LatEMA:        r.Latency,
				SpeedEMA:      r.Speed,
				LastSeenRound: round,
				CreatedAt:     now,
				UpdatedAt:     now,
			}
			p.entries[r.IP] = e
			added++
		} else {
			e.BaseLatency = r.Latency
			e.BaseSpeed = r.Speed
			e.HasBase = true
			e.LastSeenRound = round
			e.UpdatedAt = now
			refreshed++
		}
	}
	// 连续 3 轮未出现且实时样本 <5 → 移出
	evicted := 0
	for ip, e := range p.entries {
		if e.LastSeenRound <= round-3 && e.Samples < liveSamplesTakeover {
			delete(p.entries, ip)
			evicted++
		}
	}
	// 池子上限，超了踢分数最低
	over := len(p.entries) - p.max
	if over > 0 {
		p.evictLowestLocked(over)
	}
	p.mu.Unlock()

	if added > 0 || refreshed > 0 || evicted > 0 {
		slog.Info("cfst round merged",
			"round", round, "added", added, "refreshed", refreshed,
			"stale_evicted", evicted, "pool_size", p.Size())
		p.markDirty()
		p.notify()
	}
	return added, refreshed
}

func (p *Pool) evictLowestLocked(n int) {
	type kv struct {
		ip    string
		score float64
	}
	list := make([]kv, 0, len(p.entries))
	for ip, e := range p.entries {
		list = append(list, kv{ip, e.Score()})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].score < list[j].score })
	for _, k := range list[:n] {
		delete(p.entries, k.ip)
		slog.Info("evicted (pool overflow)", "ip", k.ip, "score", k.score)
	}
}

// Size 当前池子大小。
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

// BestScore 池内最高分；空池返回 0。
func (p *Pool) BestScore() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	best := 0.0
	for _, e := range p.entries {
		if s := e.Score(); s > best {
			best = s
		}
	}
	return best
}

// Candidates 按分数降序返回候选 IP 的**拷贝**（调用方可在锁外安全读取计分字段）。
func (p *Pool) Candidates() []*IPEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	list := make([]*IPEntry, 0, len(p.entries))
	for _, e := range p.entries {
		c := *e
		list = append(list, &c)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Score() > list[j].Score() })
	return list
}

// Top 返回前 n 名（日志用），n<=0 表示全部。返回的是拷贝，锁外可安全读。
func (p *Pool) Top(n int) []*IPEntry {
	all := p.Candidates()
	if n <= 0 || n >= len(all) {
		return all
	}
	return all[:n]
}

// ReportDial 记录一次拨号结果。ok=true 时 latencyMs 为拨号耗时。
func (p *Pool) ReportDial(ip string, ok bool, latencyMs float64) {
	p.mu.Lock()
	e := p.entries[ip]
	if e == nil {
		p.mu.Unlock()
		return
	}
	e.Attempts++
	if ok {
		e.Successes++
		e.ConsecFail = 0
		if e.Samples == 0 {
			e.LatEMA = latencyMs
		} else {
			e.LatEMA = emaAlpha*latencyMs + (1-emaAlpha)*e.LatEMA
		}
		e.UpdatedAt = time.Now()
	} else {
		e.ConsecFail++
		e.CumFail++
		e.UpdatedAt = time.Now()
		penalized := e.ConsecFail == consecFailPenalty
		evict := e.CumFail >= p.failEvict
		p.mu.Unlock()
		if penalized {
			slog.Warn("ip penalized", "ip", ip, "consec_fail", e.ConsecFail, "score", e.Score())
		}
		if evict {
			p.Evict(ip, "cumulative failures")
		} else {
			p.markDirty()
			p.notify()
		}
		return
	}
	p.mu.Unlock()
	p.markDirty()
	p.notify()
}

// ReportConn 记录一次成功建立并结束的连接。
// ok: 传输是否正常结束（无错误）；bytes: 客户端→上游总字节；duration: 连接时长；
// retries: 该连接建立过程中经历的降级拨号次数（归因到最终 IP）。
func (p *Pool) ReportConn(ip string, ok bool, bytes int64, duration time.Duration, retries int) {
	p.mu.Lock()
	e := p.entries[ip]
	if e == nil {
		p.mu.Unlock()
		return
	}
	e.Samples++
	e.Retries += retries
	if !ok {
		e.TransFail++
		e.ConsecFail++
		e.CumFail++
	} else {
		e.ConsecFail = 0
	}
	if duration > 0 && bytes > 0 {
		speed := float64(bytes) / duration.Seconds() / 1e6 // MB/s
		if e.Samples == 1 {
			e.SpeedEMA = speed
		} else {
			e.SpeedEMA = emaAlpha*speed + (1-emaAlpha)*e.SpeedEMA
		}
	}
	e.UpdatedAt = time.Now()
	evict := e.CumFail >= p.failEvict
	p.mu.Unlock()
	if evict {
		p.Evict(ip, "cumulative failures")
		return
	}
	p.markDirty()
	p.notify()
}

// Evict 把 IP 移出池子并记录日志。
func (p *Pool) Evict(ip, reason string) {
	p.mu.Lock()
	e := p.entries[ip]
	if e == nil {
		p.mu.Unlock()
		return
	}
	delete(p.entries, ip)
	score := e.Score()
	p.mu.Unlock()
	slog.Warn("ip evicted", "ip", ip, "reason", reason, "score", score, "pool_size", p.Size())
	p.markDirty()
	p.notify()
}

// Changed 返回变更通知 channel。
func (p *Pool) Changed() <-chan struct{} { return p.changed }
