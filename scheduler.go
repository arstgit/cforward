package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	// Refill when the best score in the pool drops below this (plenty of IPs,
	// but all bad → still refresh).
	scoreFloorTrigger = 50.0
	// Upper bound for one cfst round (includes download tests); killed on timeout.
	cfstMaxDuration = 10 * time.Minute
	// Scheduling evaluation period (event-driven primarily; this is the fallback).
	evalInterval = 10 * time.Second
)

// Scheduler refills the pool event-driven:
// runs immediately at startup; refills when pool size < poolMin or the best score
// < scoreFloorTrigger; after a completed round, waits at least cooldown before
// refilling again; never triggers while a round is already running.
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
	// Kick off immediately at startup (empty/insufficient pool).
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

// needRefill reports whether a refill round is due (reads snapshots, no locks needed).
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

// runOnce executes one cfst round and merges the results into the pool.
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
		DD:      s.cfg.CfstDD,
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
		// Failed rounds do not advance the round counter; the scheduler retries
		// automatically after the cooldown.
		return
	}
	added, refreshed := s.pool.AddCfstResults(results)
	slog.Info("cfst round done", "ips", len(results), "added", added,
		"refreshed", refreshed, "duration", time.Since(start))
}
