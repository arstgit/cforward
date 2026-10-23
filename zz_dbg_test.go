package main

import (
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"testing"
)

func TestDbgTiny(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	for i := 0; i < 40; i++ {
		ok := t.Run("iter", func(t *testing.T) { TestTinyResponseNotActive(t) })
		if !ok {
			buf := make([]byte, 2<<20)
			n := runtime.Stack(buf, true)
			fmt.Printf("=== STACKS (iter %d) ===\n%s\n", i, buf[:n])
			t.FailNow()
		}
	}
}
