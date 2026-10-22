package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
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
	args := append(append([]string{}, cfstArgs...), "-url", url)
	cmd := exec.CommandContext(ctx, cfstPath, args...)
	cmd.Dir = cfstDir

	start := time.Now()
	out, err := cmd.Output()
	slog.Debug("cfst exited", "err", err, "duration", time.Since(start), "stdout_bytes", len(out))

	// 即使退出码非零，只要 stdout 里有结果行也算成功（cfst 部分场景返回码不可靠）
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("cfst %w", ctx.Err())
		}
		results, perr := ParseCfstOutput(out)
		if perr != nil {
			return nil, fmt.Errorf("cfst run failed: %v; output parse: %v", err, perr)
		}
		return results, nil
	}
	return ParseCfstOutput(out)
}
