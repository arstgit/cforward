package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Duration is a time.Duration that unmarshals from JSON strings like "5m".
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) Std() time.Duration { return time.Duration(d) }

// Config holds every tunable setting. Load order: defaults < JSON config file < CLI flags.
type Config struct {
	Listen         string   `json:"listen"`
	CfstDir        string   `json:"cfst_dir"`
	CfstPath       string   `json:"cfst_path"`
	TargetPort     int      `json:"target_port"`
	PoolFile       string   `json:"pool_file"`
	PoolMin        int      `json:"pool_min"`
	PoolMax        int      `json:"pool_max"`
	Cooldown       Duration `json:"cooldown"`
	FailEvict      int      `json:"fail_evict"`
	DialTimeout    Duration `json:"dial_timeout"`
	NoDataTimeout  Duration `json:"no_data_timeout"`
	CfstURL        string   `json:"cfst_url"`
	CfstThreads    int      `json:"cfst_threads"`     // cfst -n
	CfstTests      int      `json:"cfst_tests"`       // cfst -t
	CfstDLCount    int      `json:"cfst_dl_count"`    // cfst -dn
	CfstDLTime     int      `json:"cfst_dl_time"`     // cfst -dt
	CfstTop        int      `json:"cfst_top"`         // cfst -p: results added to pool
	CfstDD         bool     `json:"cfst_dd"`          // cfst -dd: skip the download speed test
	MinActiveBytes int64    `json:"min_active_bytes"` // a segment must move this much to count as success
	MinActiveSpan  Duration `json:"min_active_span"`  // ...or keep flowing for at least this long
	DeadSkip       Duration `json:"dead_skip"`        // skip IPs with recent failures for this long
	ProvenWindow   Duration `json:"proven_window"`    // IPs with ACTIVE traffic within this window are dialed first
	StatusInterval Duration `json:"status_interval"`
	AdminAddr      string   `json:"admin_addr"` // HTTP admin endpoint (JSON status); empty disables
	Debug          bool     `json:"debug"`
}

func defaultConfig() Config {
	return Config{
		Listen:         ":1234",
		CfstDir:        "./cfst",
		TargetPort:     443,
		PoolFile:       "pool.json",
		PoolMin:        100,
		PoolMax:        200,
		Cooldown:       Duration(5 * time.Minute),
		FailEvict:      5,
		DialTimeout:    Duration(3 * time.Second),
		NoDataTimeout:  Duration(10 * time.Second),
		CfstURL:        "https://speed.cloudflare.com/__down?bytes=3000000",
		CfstThreads:    100,
		CfstTests:      2,
		CfstDLCount:    10,
		CfstDLTime:     10,
		CfstTop:        20,
		CfstDD:         true, // latency-only by default (saves traffic); opt in to download tests
		MinActiveBytes: 64 * 1024,
		MinActiveSpan:  Duration(2 * time.Second),
		DeadSkip:       Duration(60 * time.Second),
		ProvenWindow:   Duration(15 * time.Minute),
		StatusInterval: Duration(60 * time.Second),
		AdminAddr:      "127.0.0.1:18080",
	}
}

// loadConfig reads a JSON config file; fields not present keep their defaults.
func loadConfig(path string) (Config, error) {
	cfg := defaultConfig()
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// findArg scans the raw CLI args for the value of -config, before any flags are registered.
func findArg(args []string, name string) string {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "-"+name || a == "--"+name {
			if i+1 < len(args) {
				return args[i+1]
			}
			return ""
		}
		if strings.HasPrefix(a, "-"+name+"=") {
			return strings.TrimPrefix(a, "-"+name+"=")
		}
	}
	return ""
}

// normalize fills default paths and converts relative paths to absolute.
func (c *Config) normalize() {
	if c.CfstPath == "" {
		c.CfstPath = filepath.Join(c.CfstDir, "cfst")
	}
	// Relative cfst paths must become absolute: with cmd.Dir set, os/exec chdirs the
	// child first and then resolves the executable path, so a relative path is
	// resolved a second time inside the new directory → ENOTDIR.
	if !filepath.IsAbs(c.CfstDir) {
		if abs, err := filepath.Abs(c.CfstDir); err == nil {
			c.CfstDir = abs
		}
	}
	if !filepath.IsAbs(c.CfstPath) {
		if abs, err := filepath.Abs(c.CfstPath); err == nil {
			c.CfstPath = abs
		}
	}
}

// version is injected at build time: go build -ldflags "-X main.version=v1.0.0"
var version = "dev"

func main() {
	// Two-phase parsing: pick out -config manually, load the config file, then register
	// every flag with the config-file values as defaults so CLI flags naturally win.
	configPath := findArg(os.Args[1:], "config")
	cfg := defaultConfig()
	if configPath != "" {
		loaded, err := loadConfig(configPath)
		if err != nil {
			slog.Error("load config", "path", configPath, "err", err)
			os.Exit(1)
		}
		cfg = loaded
	}

	fs := flag.NewFlagSet("cforward", flag.ExitOnError)
	fs.StringVar(&configPath, "config", configPath, "path to JSON config file (CLI flags take precedence)")
	fs.StringVar(&cfg.Listen, "listen", cfg.Listen, "listen address")
	fs.StringVar(&cfg.CfstDir, "cfst-dir", cfg.CfstDir, "cfst working directory (contains ip.txt)")
	fs.StringVar(&cfg.CfstPath, "cfst-path", cfg.CfstPath, "cfst executable path (default <cfst-dir>/cfst)")
	fs.IntVar(&cfg.TargetPort, "target-port", cfg.TargetPort, "upstream port on the CF IP")
	fs.StringVar(&cfg.PoolFile, "pool-file", cfg.PoolFile, "pool persistence file")
	fs.IntVar(&cfg.PoolMin, "pool-min", cfg.PoolMin, "refill pool when IP count drops below this")
	fs.IntVar(&cfg.PoolMax, "pool-max", cfg.PoolMax, "pool size cap; evict lowest score beyond this")
	fs.DurationVar((*time.Duration)(&cfg.Cooldown), "cooldown", cfg.Cooldown.Std(), "min interval before the next refill after a completed round")
	fs.IntVar(&cfg.FailEvict, "fail-evict", cfg.FailEvict, "evict an IP after this many cumulative failures")
	fs.DurationVar((*time.Duration)(&cfg.DialTimeout), "dial-timeout", cfg.DialTimeout.Std(), "dial timeout")
	fs.DurationVar((*time.Duration)(&cfg.NoDataTimeout), "no-data-timeout", cfg.NoDataTimeout.Std(), "dead-path detection window (TCP_USER_TIMEOUT)")
	fs.StringVar(&cfg.CfstURL, "cfst-url", cfg.CfstURL, "cfst download speed test URL")
	fs.IntVar(&cfg.CfstThreads, "cfst-threads", cfg.CfstThreads, "cfst latency test threads (-n)")
	fs.IntVar(&cfg.CfstTests, "cfst-tests", cfg.CfstTests, "cfst tests per IP (-t)")
	fs.IntVar(&cfg.CfstDLCount, "cfst-dl-count", cfg.CfstDLCount, "cfst download test count (-dn)")
	fs.IntVar(&cfg.CfstDLTime, "cfst-dl-time", cfg.CfstDLTime, "cfst per-IP download test time limit in seconds (-dt)")
	fs.IntVar(&cfg.CfstTop, "cfst-top", cfg.CfstTop, "cfst results added to the pool (-p)")
	fs.BoolVar(&cfg.CfstDD, "cfst-dd", cfg.CfstDD, "skip the cfst download speed test (default on; set -cfst-dd=false to enable download tests)")
	fs.Int64Var(&cfg.MinActiveBytes, "min-active-bytes", cfg.MinActiveBytes, "a segment must move this many bytes to count as success")
	fs.DurationVar((*time.Duration)(&cfg.MinActiveSpan), "min-active-span", cfg.MinActiveSpan.Std(), "or data must keep flowing for at least this long (changing traffic)")
	fs.DurationVar((*time.Duration)(&cfg.DeadSkip), "dead-skip", cfg.DeadSkip.Std(), "skip IPs with recent no-data/error failures for this long")
	fs.DurationVar((*time.Duration)(&cfg.ProvenWindow), "proven-window", cfg.ProvenWindow.Std(), "IPs with ACTIVE traffic within this window are dialed first")
	fs.StringVar(&cfg.AdminAddr, "admin-addr", cfg.AdminAddr, "HTTP admin endpoint (JSON status); empty disables")
	fs.DurationVar((*time.Duration)(&cfg.StatusInterval), "status-interval", cfg.StatusInterval.Std(), "status block interval")
	var showVersion bool
	fs.BoolVar(&showVersion, "version", false, "print version and exit")
	fs.BoolVar(&cfg.Debug, "debug", cfg.Debug, "enable debug logging")
	fs.Parse(os.Args[1:])

	if showVersion {
		fmt.Println("cforward", version)
		os.Exit(0)
	}

	cfg.normalize()

	level := slog.LevelInfo
	if cfg.Debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	slog.Info("cforward starting", "config", configPath, "listen", cfg.Listen,
		"cfst_dir", cfg.CfstDir, "target_port", cfg.TargetPort)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startedAt := time.Now()

	pool := NewPool(cfg.PoolFile, cfg.PoolMax, cfg.FailEvict)
	if err := pool.Load(); err != nil {
		slog.Warn("pool load failed", "err", err)
	}

	// Pool refill scheduler.
	sched := &Scheduler{pool: pool, cfg: cfg}
	go sched.Run(ctx)

	// TCP forwarder.
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		slog.Error("listen", "addr", cfg.Listen, "err", err)
		os.Exit(1)
	}
	slog.Info("forwarder listening", "addr", cfg.Listen, "target_port", cfg.TargetPort)
	fw := NewForwarder(pool, cfg.TargetPort, cfg.DialTimeout.Std(), cfg.NoDataTimeout.Std(),
		cfg.DeadSkip.Std(), cfg.MinActiveSpan.Std(), cfg.ProvenWindow.Std(), cfg.MinActiveBytes)
	go fw.Serve(ctx, ln)

	// Periodic status block.
	go func() {
		t := time.NewTicker(cfg.StatusInterval.Std())
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				printStatus(&cfg, pool, sched)
			}
		}
	}()

	// HTTP admin endpoint.
	if cfg.AdminAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
			writeStatusJSON(w, &cfg, pool, sched, startedAt)
		})
		admin := &http.Server{Addr: cfg.AdminAddr, Handler: mux}
		go func() {
			slog.Info("admin endpoint listening", "addr", cfg.AdminAddr)
			if err := admin.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("admin endpoint", "err", err)
			}
		}()
		defer admin.Close()
	}

	<-ctx.Done()
	slog.Info("shutting down")
	pool.Close()
}

// printStatus prints a readable status block (Top-10 pool table + active forwards).
func printStatus(cfg *Config, pool *Pool, sched *Scheduler) {
	top := pool.Top(10)
	var b strings.Builder
	fmt.Fprintf(&b, "── cforward status ──────────────────────────────\n")
	cfstState := "idle"
	if running, _, lastDur, lastErr := sched.Status(); running {
		cfstState = "running"
	} else if lastErr != nil {
		cfstState = "last round failed"
	} else if lastDur > 0 {
		cfstState = "last round " + lastDur.Round(time.Second).String() + " ago"
	}
	fmt.Fprintf(&b, "  pool: %d IPs (min %d / max %d)    active conns: %d    dead-skipped: %d    cfst: %s\n",
		pool.Size(), cfg.PoolMin, cfg.PoolMax, activeConns.Load(),
		pool.DeadSkippedCount(), cfstState)
	if len(top) == 0 {
		fmt.Fprintln(&b, "  (pool empty, waiting for the first cfst round…)")
	} else {
		fmt.Fprintf(&b, "  %-3s  %-16s  %6s  %6s  %11s\n", "#", "IP", "score", "lat", "speed")
		for i, e := range top {
			fmt.Fprintf(&b, "  %-3d  %-16s  %6.1f  %6.0fms  %6.1fMB/s\n",
				i+1, e.IP, e.Score(), e.latency(), e.speed())
		}
	}

	// Active relay segments with live throughput.
	var segs []*segInfo
	activeSegs.Range(func(_, v any) bool {
		segs = append(segs, v.(*segInfo))
		return true
	})
	sort.Slice(segs, func(i, j int) bool { return segs[i].start.Before(segs[j].start) })
	fmt.Fprintf(&b, "  active forwards: %d\n", len(segs))
	if len(segs) == 0 {
		fmt.Fprintln(&b, "  (none)")
	} else {
		fmt.Fprintf(&b, "  %-3s  %-22s  %-16s  %7s  %9s  %9s  %9s  %7s\n",
			"#", "client", "upstream", "age", "speed", "up", "down", "dial")
		for i, s := range segs {
			age := time.Since(s.start)
			up, down := s.c2u.Load(), s.u2c.Load()
			speed := 0.0
			if age > 0 {
				speed = float64(up+down) / age.Seconds() / 1e6 // MB/s
			}
			fmt.Fprintf(&b, "  %-3d  %-22s  %-16s  %6s  %7.2fM  %8s  %8s  %5dms\n",
				i+1, s.clientAddr, s.ip, age.Round(time.Second), speed,
				humanBytes(up), humanBytes(down), s.dialLatMS)
		}
	}
	fmt.Print(b.String())
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fG", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fM", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.0fK", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func round2(v float64) float64 { return float64(int(v*100+0.5)) / 100 }

// writeStatusJSON serves the admin /status endpoint.
func writeStatusJSON(w http.ResponseWriter, cfg *Config, pool *Pool, sched *Scheduler, startedAt time.Time) {
	running, lastRun, lastDur, lastErr := sched.Status()

	top := pool.Top(20)
	active := []map[string]any{}
	activeSegs.Range(func(_, v any) bool {
		s := v.(*segInfo)
		age := time.Since(s.start)
		up, down := s.c2u.Load(), s.u2c.Load()
		active = append(active, map[string]any{
			"client":      s.clientAddr,
			"ip":          s.ip,
			"dial_lat_ms": s.dialLatMS,
			"age_sec":     age.Round(time.Millisecond).Seconds(),
			"up_bytes":    up,
			"down_bytes":  down,
		})
		return true
	})

	type cfstState struct {
		Running      bool   `json:"running"`
		LastRun      string `json:"last_run"`
		LastDuration string `json:"last_duration"`
		LastError    string `json:"last_error"`
		NextRefill   string `json:"next_refill"` // approx: cooldown from lastRun when still insufficient
	}
	st := cfstState{Running: running}
	if !lastRun.IsZero() {
		st.LastRun = lastRun.Format(time.RFC3339)
	}
	if lastDur > 0 {
		st.LastDuration = lastDur.Round(time.Millisecond).String()
	}
	if lastErr != nil {
		st.LastError = lastErr.Error()
	}

	resp := map[string]any{
		"version":     version,
		"uptime_sec":  int64(time.Since(startedAt).Seconds()),
		"listen":      cfg.Listen,
		"target_port": cfg.TargetPort,
		"pool": map[string]any{
			"size":         pool.Size(),
			"min":          cfg.PoolMin,
			"max":          cfg.PoolMax,
			"best_score":   round2(pool.BestScore()),
			"proven":       pool.ProvenCount(cfg.ProvenWindow.Std()),
			"dead_skipped": pool.DeadSkippedCount(),
			"top":          top,
		},
		"active_conns":    activeConns.Load(),
		"active_segments": active,
		"cfst":            st,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
