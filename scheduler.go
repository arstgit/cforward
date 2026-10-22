package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	// 池内最高分低于此值触发补池（数量够但全是烂 IP 也刷新）
	scoreFloorTrigger = 50.0
	// 单轮 cfst 总时长上限（含下载测速），超时 kill
	cfstMaxDuration = 10 * time.Minute
	// 调度评估周期（事件驱动为主，此为兜底）
	evalInterval = 10 * time.Second
)

// Scheduler 事件驱动补池：
// 启动即跑；池内 IP < poolMin 或最高分 < scoreFloorTrigger 时补池；
// 一轮完成后不足则至少冷却 cooldown 再补；正在跑时不重复触发。
type Scheduler struct {
	pool *Pool
	cfg  Config

	mu      sync.Mutex
	running bool
	lastRun time.Time
	lastDur time.Duration
	lastErr error
}

func (s *Scheduler) Run(ctx context.Context) {
	// 启动即触发（池子空/不足时立即跑第一轮）
	s.wake()

	t := time.NewTicker(evalInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.pool.Changed():
			s.eval()
		case <-t.C:
			s.eval()
		}
	}
}

func (s *Scheduler) wake() {
	s.eval()
}

// needRefill 是否满足补池条件（不加锁，评估时读快照）。
func (s *Scheduler) needRefill() bool {
	if s.pool.Size() < s.cfg.PoolMin {
		return true
	}
	return s.pool.BestScore() < scoreFloorTrigger
}

func (s *Scheduler) eval() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	if time.Since(s.lastRun) < s.cfg.Cooldown.Std() {
		s.mu.Unlock()
		return
	}
	if !s.needRefill() {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	go s.runOnce()
}

// runOnce 执行一轮 cfst 并把结果并入池子。
func (s *Scheduler) runOnce() {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), cfstMaxDuration)
	defer cancel()

	slog.Info("cfst round starting", "dir", s.cfg.CfstDir)
	opts := CfstOpts{
		URL:     s.cfg.CfstURL,
		Threads: s.cfg.CfstThreads,
		Tests:   s.cfg.CfstTests,
		DLCount: s.cfg.CfstDLCount,
		DLTime:  s.cfg.CfstDLTime,
		Top:     s.cfg.CfstTop,
	}
	results, err := RunCfst(ctx, s.cfg.CfstDir, s.cfg.CfstPath, opts)

	s.mu.Lock()
	s.running = false
	s.lastRun = time.Now()
	s.lastDur = time.Since(start)
	s.lastErr = err
	s.mu.Unlock()

	if err != nil {
		slog.Error("cfst round failed", "err", err, "duration", time.Since(start))
		// 失败轮不计轮次；冷却后 scheduler 会自动重试
		return
	}
	added, refreshed := s.pool.AddCfstResults(results)
	slog.Info("cfst round done", "ips", len(results), "added", added,
		"refreshed", refreshed, "duration", time.Since(start))
}
