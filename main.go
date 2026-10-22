package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

type Config struct {
	listen        string
	cfstDir       string
	cfstPath      string
	targetPort    int
	poolFile      string
	poolMin       int
	poolMax       int
	cooldown      time.Duration
	failEvict     int
	dialTimeout   time.Duration
	noDataTimeout time.Duration
	cfstURL       string
	debug         bool
}

func parseFlags() Config {
	var c Config
	flag.StringVar(&c.listen, "listen", ":1234", "监听地址")
	flag.StringVar(&c.cfstDir, "cfst-dir", "./cfst", "cfst 工作目录（含 ip.txt）")
	flag.StringVar(&c.cfstPath, "cfst-path", "", "cfst 可执行文件路径（默认 <cfst-dir>/cfst）")
	flag.IntVar(&c.targetPort, "target-port", 443, "转发目标端口（CF IP 上）")
	flag.StringVar(&c.poolFile, "pool-file", "pool.json", "池子持久化文件")
	flag.IntVar(&c.poolMin, "pool-min", 100, "池内 IP 低于此数触发补池")
	flag.IntVar(&c.poolMax, "pool-max", 200, "池子上限，超了踢最低分")
	flag.DurationVar(&c.cooldown, "cooldown", 5*time.Minute, "补池完成后不足时，距下次补池的最小间隔")
	flag.IntVar(&c.failEvict, "fail-evict", 5, "累计失败达到此数移出池子")
	flag.DurationVar(&c.dialTimeout, "dial-timeout", 3*time.Second, "拨号超时")
	flag.DurationVar(&c.noDataTimeout, "no-data-timeout", 10*time.Second, "双向无流量判死路径的超时")
	flag.StringVar(&c.cfstURL, "cfst-url", "https://speed.cloudflare.com/__down?bytes=3000000", "cfst 下载测速地址")
	flag.BoolVar(&c.debug, "debug", false, "debug 日志")
	flag.Parse()
	if c.cfstPath == "" {
		c.cfstPath = c.cfstDir + "/cfst"
	}
	// 关键：cfst 相对路径必须转绝对。os/exec 设置 cmd.Dir 后，子进程先 chdir
	// 再解析可执行文件路径，相对路径会在新目录下被二次解析（如 ./cfst/cfst 在
	// chdir 到 ./cfst 后变成找 ./cfst/cfst/cfst → ENOTDIR）。
	if !filepath.IsAbs(c.cfstDir) {
		if abs, err := filepath.Abs(c.cfstDir); err == nil {
			c.cfstDir = abs
		}
	}
	if !filepath.IsAbs(c.cfstPath) {
		if abs, err := filepath.Abs(c.cfstPath); err == nil {
			c.cfstPath = abs
		}
	}
	return c
}

func main() {
	cfg := parseFlags()

	level := slog.LevelInfo
	if cfg.debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := NewPool(cfg.poolFile, cfg.poolMax, cfg.failEvict)
	if err := pool.Load(); err != nil {
		slog.Warn("pool load failed", "err", err)
	}

	// 补池调度器
	sched := &Scheduler{
		pool:    pool,
		cfg:     cfg,
		cfstDir: cfg.cfstDir,
	}
	go sched.Run(ctx)

	// 转发
	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		slog.Error("listen", "addr", cfg.listen, "err", err)
		os.Exit(1)
	}
	slog.Info("forwarder listening", "addr", cfg.listen, "target_port", cfg.targetPort)
	fw := NewForwarder(pool, cfg.targetPort, cfg.dialTimeout, cfg.noDataTimeout)
	go fw.Serve(ctx, ln)

	// 周期日志：Top5 + 活跃连接
	go periodicLog(ctx, pool)

	<-ctx.Done()
	slog.Info("shutting down")
	pool.Close()
}

func periodicLog(ctx context.Context, pool *Pool) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			top := pool.Top(5)
			if len(top) == 0 {
				slog.Info("pool status", "size", 0, "active_conns", activeConns.Load())
				continue
			}
			attrs := make([]any, 0, 2+len(top)*4)
			attrs = append(attrs, "size", pool.Size(), "active_conns", activeConns.Load())
			for i, e := range top {
				attrs = append(attrs,
					fmt.Sprintf("top%d_ip", i+1), e.IP,
					fmt.Sprintf("top%d_score", i+1), round2(e.Score()),
					fmt.Sprintf("top%d_lat", i+1), round2(e.latency()),
					fmt.Sprintf("top%d_speed", i+1), round2(e.speed()))
			}
			slog.Info("pool top5", attrs...)
		}
	}
}

func round2(v float64) float64 { return float64(int(v*100+0.5)) / 100 }
