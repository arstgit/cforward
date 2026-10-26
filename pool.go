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

// ---- Scoring constants (algorithm parameters, intentionally not configurable) ----

const (
	// Score weights: 0.4L + 0.3S + 0.2U + 0.1R
	weightLat   = 0.4
	weightSpeed = 0.3
	weightSucc  = 0.2
	weightRetry = 0.1

	// EMA smoothing factor for live latency/speed
	emaAlpha = 0.3

	// Live stats fully take over the cfst baseline after this many samples;
	// before that, baseline and live values are blended by sample count.
	liveSamplesTakeover = 5

	// Consecutive failures at/above this many crush the score...
	consecFailPenalty = 3
	// ...by this factor.
	penaltyFactor = 0.1

	// Latency normalization ceiling (1000ms → 0 points)
	latNormMs = 1000.0
	// Speed normalization saturation parameter (20MB/s → ~63 points)
	speedNorm = 20.0
)

// IPEntry is all state for one IP in the pool.
type IPEntry struct {
	IP string `json:"ip"`

	// cfst baseline (refreshed every time a cfst round hits this IP)
	BaseLatency float64 `json:"base_latency"` // ms
	BaseSpeed   float64 `json:"base_speed"`   // MB/s
	HasBase     bool    `json:"has_base"`

	// Live stats
	Samples   int     `json:"samples"`    // connections served
	LatEMA    float64 `json:"lat_ema"`    // dial latency EMA (ms)
	SpeedEMA  float64 `json:"speed_ema"`  // throughput EMA (MB/s)
	Attempts  int     `json:"attempts"`   // dial attempts
	Successes int     `json:"successes"`  // successful dials
	TransFail int     `json:"trans_fail"` // transport failures
	Retries   int     `json:"retries"`    // downgrade dials attributed to this IP

	ConsecFail int `json:"consec_fail"` // consecutive failures (penalty trigger)
	CumFail    int `json:"cum_fail"`    // cumulative failures (eviction trigger)

	LastSeenRound int       `json:"last_seen_round"` // last cfst round this IP appeared in
	DeadUntil     time.Time `json:"dead_until"`      // dials skip this IP until this time (repeated failures)
	LastActive    time.Time `json:"last_active"`     // last data exchange (status display)
	Verified      bool      `json:"verified"`        // has served REAL traffic: permanently preferred until evicted
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// penalized reports whether the IP is under the consecutive-failure penalty.
func (e *IPEntry) penalized() bool { return e.ConsecFail >= consecFailPenalty }

// latency returns the latency used for scoring: the cfst baseline is blended in
// until enough live samples have been collected.
func (e *IPEntry) latency() float64 {
	if e.Samples >= liveSamplesTakeover {
		return e.LatEMA
	}
	if e.HasBase {
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

// successRate is the share of dial attempts that led to ACTIVE traffic segments
// (the gold standard). TCP-only connectivity never counts.
func (e *IPEntry) successRate() float64 {
	if e.Attempts == 0 {
		return 1.0
	}
	return float64(e.Successes) / float64(e.Attempts)
}

// retryRate is attributed retries over (retries + successful connections).
func (e *IPEntry) retryRate() float64 {
	denom := float64(e.Retries + e.Samples)
	if denom == 0 {
		return 0
	}
	return float64(e.Retries) / denom
}

// Score computes 0.4L + 0.3S + 0.2U + 0.1R, crushed by penaltyFactor when penalized.
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

// ---- Pool ----

type Pool struct {
	mu      sync.Mutex
	entries map[string]*IPEntry
	rounds  int // successful cfst refill rounds

	changed chan struct{} // non-blocking change notification (buffered 1)

	// Persistence
	file      string
	dirty     bool
	saveTimer *time.Timer

	// Limits (injected from config)
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

// Load restores the pool from pool.json at startup.
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

// Save writes pool.json (debounced; also used for the shutdown flush).
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

// Close flushes and stops the background saver.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.saveTimer != nil {
		p.saveTimer.Stop()
		p.saveTimer = nil
	}
	p.mu.Unlock()
	p.Save()
}

// CfstResult is one result row from cfst.
type CfstResult struct {
	IP      string
	Latency float64 // ms
	Speed   float64 // MB/s
}

// AddCfstResults merges one cfst round into the pool:
// new IPs get an initial score from cfst data; existing IPs only refresh their baseline.
// Returns the number of added/refreshed entries.
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
			e.DeadUntil = time.Time{} // fresh measurement revives a dead-flagged IP
			e.UpdatedAt = now
			refreshed++
		}
	}
	// IPs unseen for 3 rounds with fewer than liveSamplesTakeover samples are stale → evict.
	evicted := 0
	for ip, e := range p.entries {
		if e.LastSeenRound <= round-3 && e.Samples < liveSamplesTakeover {
			delete(p.entries, ip)
			evicted++
		}
	}
	// Enforce the pool cap: evict the lowest-scoring entries beyond it.
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

// Size returns the current pool size.
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

// BestScore returns the highest score in the pool; 0 when empty.
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

// Candidates returns candidate IPs sorted by score, descending, as copies
// (safe to read outside the lock).
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

// Top returns the top n entries for logging (n<=0 means all). Copies, safe to read.
func (p *Pool) Top(n int) []*IPEntry {
	all := p.Candidates()
	if n <= 0 || n >= len(all) {
		return all
	}
	return all[:n]
}

// ReportDial records one dial result. When ok, latencyMs is the dial duration.
// ReportDial records one dial result. Dial connectivity alone is NOT a goodness
// signal — only segments with active traffic (ReportConn ok=true) count as
// successes. A successful dial only feeds the latency EMA.
func (p *Pool) ReportDial(ip string, ok bool, latencyMs float64) {
	p.mu.Lock()
	e := p.entries[ip]
	if e == nil {
		p.mu.Unlock()
		return
	}
	e.Attempts++
	if ok {
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

// ReportConn records one established and finished connection segment.
// ok: the segment carried ACTIVE traffic (the gold standard); bytes: total bytes
// both directions; duration: segment duration; retries: downgrade dials during
// setup (attributed to the final IP). Only ok segments feed Successes and the
// speed EMA; failures drive ConsecFail/CumFail towards penalty and eviction.
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
		e.Successes++
		e.ConsecFail = 0
		e.LastActive = time.Now()
		e.Verified = true // carried real traffic: locked in as a good IP
	}
	if ok && duration > 0 && bytes > 0 {
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

// VerifiedCount returns how many IPs have served real traffic (locked-in good IPs).
func (p *Pool) VerifiedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, e := range p.entries {
		if e.Verified {
			n++
		}
	}
	return n
}

// DeadSkippedCount returns how many IPs are currently dead-flagged (skipped by dials).
func (p *Pool) DeadSkippedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	now := time.Now()
	for _, e := range p.entries {
		if e.DeadUntil.After(now) {
			n++
		}
	}
	return n
}

// ReportNeutral records a segment that is neither a success nor a failure
// (client-side fault, silent client, or a complete-but-idle exchange like a TLS
// handshake with no app data). It counts as a sample so live stats take over, but
// does not touch Successes, TransFail, ConsecFail or CumFail — the IP is neither
// rewarded nor burned by client behavior.
func (p *Pool) ReportNeutral(ip string, retries int) {
	p.mu.Lock()
	e := p.entries[ip]
	if e != nil {
		e.Samples++
		e.Retries += retries
		e.LastActive = time.Now()
		e.UpdatedAt = time.Now()
	}
	p.mu.Unlock()
	p.markDirty()
	p.notify()
}

// MaybeMarkDead flags an IP as temporarily dead once it has accumulated deadAfter
// consecutive failures (stability-first: a single bad segment must not burn an IP).
// Returns true when the IP was newly marked. dialBest skips dead IPs until DeadUntil.
func (p *Pool) MaybeMarkDead(ip string, deadSkip time.Duration, deadAfter int) bool {
	p.mu.Lock()
	e := p.entries[ip]
	if e == nil || e.ConsecFail < deadAfter || !e.DeadUntil.IsZero() {
		p.mu.Unlock()
		return false
	}
	e.DeadUntil = time.Now().Add(deadSkip)
	e.UpdatedAt = time.Now()
	p.mu.Unlock()
	p.markDirty()
	p.notify()
	return true
}

// MarkDead flags an IP as temporarily dead: dialBest skips it until DeadUntil.
// Called after any upstream-side failure (transport error or no-data segment) so
// the next client connection immediately moves on instead of retrying a dead IP.
func (p *Pool) MarkDead(ip string, deadSkip time.Duration) {
	p.mu.Lock()
	e := p.entries[ip]
	if e != nil {
		e.DeadUntil = time.Now().Add(deadSkip)
		e.UpdatedAt = time.Now()
	}
	p.mu.Unlock()
	p.markDirty()
	p.notify()
}

// Evict removes an IP from the pool and logs the event.
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

// Changed returns the change-notification channel.
func (p *Pool) Changed() <-chan struct{} { return p.changed }
