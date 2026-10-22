package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Duration 支持 JSON 中以 "5m" 字符串形式配置时长。
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

// Config 全部可调配置。加载顺序：默认值 < JSON 配置文件 < 命令行 flag。
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
	CfstThreads    int      `json:"cfst_threads"`  // cfst -n
	CfstTests      int      `json:"cfst_tests"`    // cfst -t
	CfstDLCount    int      `json:"cfst_dl_count"` // cfst -dn
	CfstDLTime     int      `json:"cfst_dl_time"`  // cfst -dt
	CfstTop        int      `json:"cfst_top"`      // cfst -p 入池数量
	StatusInterval Duration `json:"status_interval"`
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
		StatusInterval: Duration(60 * time.Second),
	}
}

// loadConfig 从 JSON 文件加载配置；文件中未出现的字段保持默认值。
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

// findArg 手动扫描命令行里 -config 的值（注册全部 flag 之前就需要用到）。
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

// normalize 补齐默认路径并处理相对路径。
func (c *Config) normalize() {
	if c.CfstPath == "" {
		c.CfstPath = filepath.Join(c.CfstDir, "cfst")
	}
	// cfst 相对路径必须转绝对：os/exec 设置 cmd.Dir 后，子进程先 chdir 再解析
	// 可执行文件路径，相对路径会在新目录下被二次解析 → ENOTDIR。
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

func main() {
	// 两阶段解析：先手动摘出 -config，加载配置文件，再以配置文件值为 flag 默认值注册，
	// 这样命令行 flag 自然覆盖配置文件。
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

	fs := flag.NewFlagSet("cfstnat", flag.ExitOnError)
	fs.StringVar(&configPath, "config", configPath, "JSON 配置文件路径（命令行 flag 优先级更高）")
	fs.StringVar(&cfg.Listen, "listen", cfg.Listen, "监听地址")
	fs.StringVar(&cfg.CfstDir, "cfst-dir", cfg.CfstDir, "cfst 工作目录（含 ip.txt）")
	fs.StringVar(&cfg.CfstPath, "cfst-path", cfg.CfstPath, "cfst 可执行文件路径（默认 <cfst-dir>/cfst）")
	fs.IntVar(&cfg.TargetPort, "target-port", cfg.TargetPort, "转发目标端口（CF IP 上）")
	fs.StringVar(&cfg.PoolFile, "pool-file", cfg.PoolFile, "池子持久化文件")
	fs.IntVar(&cfg.PoolMin, "pool-min", cfg.PoolMin, "池内 IP 低于此数触发补池")
	fs.IntVar(&cfg.PoolMax, "pool-max", cfg.PoolMax, "池子上限，超出踢最低分")
	fs.DurationVar((*time.Duration)(&cfg.Cooldown), "cooldown", cfg.Cooldown.Std(), "补池完成后不足时，下次补池的最小间隔")
	fs.IntVar(&cfg.FailEvict, "fail-evict", cfg.FailEvict, "累计失败达到此数移出池子")
	fs.DurationVar((*time.Duration)(&cfg.DialTimeout), "dial-timeout", cfg.DialTimeout.Std(), "拨号超时")
	fs.DurationVar((*time.Duration)(&cfg.NoDataTimeout), "no-data-timeout", cfg.NoDataTimeout.Std(), "路径死亡判定时长（TCP_USER_TIMEOUT）")
	fs.StringVar(&cfg.CfstURL, "cfst-url", cfg.CfstURL, "cfst 下载测速地址")
	fs.IntVar(&cfg.CfstThreads, "cfst-threads", cfg.CfstThreads, "cfst 延迟测速线程数 (-n)")
	fs.IntVar(&cfg.CfstTests, "cfst-tests", cfg.CfstTests, "cfst 每 IP 测速次数 (-t)")
	fs.IntVar(&cfg.CfstDLCount, "cfst-dl-count", cfg.CfstDLCount, "cfst 下载测速数量 (-dn)")
	fs.IntVar(&cfg.CfstDLTime, "cfst-dl-time", cfg.CfstDLTime, "cfst 单 IP 下载测速时长上限秒 (-dt)")
	fs.IntVar(&cfg.CfstTop, "cfst-top", cfg.CfstTop, "cfst 结果入池数量 (-p)")
	fs.DurationVar((*time.Duration)(&cfg.StatusInterval), "status-interval", cfg.StatusInterval.Std(), "状态块打印间隔")
	fs.BoolVar(&cfg.Debug, "debug", cfg.Debug, "debug 日志")
	fs.Parse(os.Args[1:])

	cfg.normalize()

	level := slog.LevelInfo
	if cfg.Debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	slog.Info("cfstnat starting", "config", configPath, "listen", cfg.Listen,
		"cfst_dir", cfg.CfstDir, "target_port", cfg.TargetPort)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := NewPool(cfg.PoolFile, cfg.PoolMax, cfg.FailEvict)
	if err := pool.Load(); err != nil {
		slog.Warn("pool load failed", "err", err)
	}

	// 补池调度器
	sched := &Scheduler{pool: pool, cfg: cfg}
	go sched.Run(ctx)

	// 转发
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		slog.Error("listen", "addr", cfg.Listen, "err", err)
		os.Exit(1)
	}
	slog.Info("forwarder listening", "addr", cfg.Listen, "target_port", cfg.TargetPort)
	fw := NewForwarder(pool, cfg.TargetPort, cfg.DialTimeout.Std(), cfg.NoDataTimeout.Std())
	go fw.Serve(ctx, ln)

	// 周期状态块
	go func() {
		t := time.NewTicker(cfg.StatusInterval.Std())
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				printStatus(&cfg, pool)
			}
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	pool.Close()
}

// printStatus 打印可读的状态块（Top10 表格）。
func printStatus(cfg *Config, pool *Pool) {
	top := pool.Top(10)
	var b strings.Builder
	fmt.Fprintf(&b, "── cfstnat 状态 ────────────────────────────────\n")
	fmt.Fprintf(&b, "  pool: %d IPs (min %d / max %d)    活跃连接: %d\n",
		pool.Size(), cfg.PoolMin, cfg.PoolMax, activeConns.Load())
	if len(top) == 0 {
		fmt.Fprintln(&b, "  （池子为空，等待 cfst 建池…）")
	} else {
		fmt.Fprintf(&b, "  %-3s  %-16s  %6s  %6s  %11s\n", "#", "IP", "分数", "延迟", "速度")
		for i, e := range top {
			fmt.Fprintf(&b, "  %-3d  %-16s  %6.1f  %6.0fms  %6.1fMB/s\n",
				i+1, e.IP, e.Score(), e.latency(), e.speed())
		}
	}
	fmt.Print(b.String())
}
