package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// cfst 固定参数（算法/性能相关，编译期常量）。
// 注意 -tlr 0：让 cfst 自己过滤掉任何丢包的 IP（丢包 >0 的 IP 不输出）。
var cfstArgs = []string{
	"-n", "100", // 延迟测速线程
	"-t", "2", // 每 IP 测速次数
	"-dn", "10", // 下载测速数量
	"-dt", "10", // 下载测速时长上限
	"-p", "20", // 显示结果数 = 入池数
	"-tlr", "0", // 丢包率上限 0：只保留零丢包 IP
	"-o", "", // 不写 result.csv，从 stdout 解析
}

// 结果行：行首是 IPv4，后跟 已发送 已接收 丢包率 平均延迟 下载速度(MB/s) 地区码
// 进度条噪声形如 "123 / 5955 [bar] 可用: 45"，行首是数字+空格+/，不会命中。
var resultLineRe = regexp.MustCompile(`^(\d{1,3}(?:\.\d{1,3}){3})\s+(\d+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(\S+)\s*$`)

// ParseCfstOutput 从 cfst stdout 提取结果行，返回 IP 列表（按输出顺序 = 分数降序）。
func ParseCfstOutput(out []byte) ([]CfstResult, error) {
	var results []CfstResult
	// 进度条用 \r 刷新在同一行内，先按 \n 分行再按 \r 切分，避免 \r 粘连行
	for _, line := range bytes.Split(out, []byte{'\n'}) {
		fields := strings.Fields(string(line))
		if len(fields) != 7 {
			continue
		}
		m := resultLineRe.FindStringSubmatch(strings.Join(fields, " "))
		if m == nil {
			continue
		}
		lat, err1 := strconv.ParseFloat(m[5], 64)
		speed, err2 := strconv.ParseFloat(m[6], 64)
		if err1 != nil || err2 != nil {
			continue
		}
		results = append(results, CfstResult{IP: m[1], Latency: lat, Speed: speed})
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no result lines found in cfst output")
	}
	return results, nil
}

// RunCfst 在 cfstDir 下执行 cfst，返回解析出的结果。
// 轮次语义：解析出 ≥1 个 IP 才算成功轮。
func RunCfst(ctx context.Context, cfstDir, cfstPath, url string) ([]CfstResult, error) {
	// 关键：cmd.Dir 会让子进程先 chdir 再解析可执行文件路径，相对路径会被二次解析
	// （./cfst/cfst 在 chdir 到 ./cfst 后变成找 ./cfst/cfst/cfst → ENOTDIR）。
	// 在父进程内先转绝对（相对当前 cwd）。
	var err error
	if cfstDir, err = filepath.Abs(cfstDir); err != nil {
		return nil, fmt.Errorf("cfst dir abs: %v", err)
	}
	if cfstPath, err = filepath.Abs(cfstPath); err != nil {
		return nil, fmt.Errorf("cfst path abs: %v", err)
	}

	args := append(append([]string{}, cfstArgs...), "-url", url)
	cmd := exec.CommandContext(ctx, cfstPath, args...)
	cmd.Dir = cfstDir

	start := time.Now()
	out, err := cmd.Output()
	slog.Debug("cfst exited", "err", err, "duration", time.Since(start), "stdout_bytes", len(out))

	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("cfst %w", ctx.Err())
		}
		// 只有"已启动但非零退出"才值得尝试解析 stdout；
		// fork/exec 失败（找不到文件、无权限等）直接报错，不混淆解析信息。
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			return nil, fmt.Errorf("cfst exec failed: %w", err)
		}
		results, perr := ParseCfstOutput(out)
		if perr != nil {
			return nil, fmt.Errorf("cfst run failed (exit %d): %v; output parse: %v",
				exitErr.ExitCode(), err, perr)
		}
		return results, nil
	}
	return ParseCfstOutput(out)
}
