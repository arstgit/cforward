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

// CfstOpts holds tunable cfst subprocess parameters.
// (-tlr 0 packet-loss filtering and -o "" stdout output are semantic requirements and stay fixed.)
type CfstOpts struct {
	URL     string
	Threads int // -n
	Tests   int // -t
	DLCount int // -dn
	DLTime  int // -dt
	Top     int // -p: number of results added to the pool
}

func cfstArgs(o CfstOpts) []string {
	return []string{
		"-n", strconv.Itoa(o.Threads),
		"-t", strconv.Itoa(o.Tests),
		"-dn", strconv.Itoa(o.DLCount),
		"-dt", strconv.Itoa(o.DLTime),
		"-p", strconv.Itoa(o.Top),
		"-tlr", "0", // packet-loss upper bound 0: keep only zero-loss IPs
		"-o", "", // do not write result.csv; parse stdout instead
		"-url", o.URL,
	}
}

// Result rows start with an IPv4 address followed by:
// sent received loss% avg-latency speed(MB/s) region-code
// Progress-bar noise looks like "123 / 5955 [bar] 可用: 45" and never matches.
var resultLineRe = regexp.MustCompile(`^(\d{1,3}(?:\.\d{1,3}){3})\s+(\d+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(\S+)\s*$`)

// ParseCfstOutput extracts result rows from cfst stdout (output order = score order).
func ParseCfstOutput(out []byte) ([]CfstResult, error) {
	var results []CfstResult
	// The progress bar refreshes in-place with \r on a single line: split on \n first,
	// then let strings.Fields strip any residual \r-joined segments.
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

// RunCfst executes cfst inside cfstDir and returns parsed results.
// Round semantics: a round only counts if at least one IP was parsed.
func RunCfst(ctx context.Context, cfstDir, cfstPath string, o CfstOpts) ([]CfstResult, error) {
	// Critical: with cmd.Dir set, the child chdirs first and then resolves the
	// executable path, so a relative path would be resolved a second time inside the
	// new directory (./cfst/cfst after chdir to ./cfst looks for ./cfst/cfst/cfst →
	// ENOTDIR). Resolve to absolute paths in the parent first.
	var err error
	if cfstDir, err = filepath.Abs(cfstDir); err != nil {
		return nil, fmt.Errorf("cfst dir abs: %v", err)
	}
	if cfstPath, err = filepath.Abs(cfstPath); err != nil {
		return nil, fmt.Errorf("cfst path abs: %v", err)
	}

	cmd := exec.CommandContext(ctx, cfstPath, cfstArgs(o)...)
	cmd.Dir = cfstDir

	start := time.Now()
	out, err := cmd.Output()
	slog.Debug("cfst exited", "err", err, "duration", time.Since(start), "stdout_bytes", len(out))

	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("cfst %w", ctx.Err())
		}
		// Only a non-zero exit after a successful spawn is worth parsing stdout for;
		// fork/exec failures (missing file, no permission, ...) are reported directly
		// so the parse error does not confuse the real cause.
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
