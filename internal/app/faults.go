package app

import (
	"context"
	"os"
	"path/filepath"
	"time"
)

// No HTTP fault interface. Explicit process-local test opt-in, point and gate
// directory are required. The host harness observes reached and kills this
// process, or creates release. Waiting is bounded and cancellation-aware.
type faultEventKey struct{}

func faultContext(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, faultEventKey{}, id)
}
func faultPoint(ctx context.Context, point string) {
	if expected := os.Getenv("FAULT_EVENT_ID"); expected != "" && ctx.Value(faultEventKey{}) != expected {
		return
	}
	if os.Getenv("FAULT_INJECTION_ENABLED") != "1" || os.Getenv("FAULT_POINT") != point {
		return
	}
	dir := os.Getenv("FAULT_GATE_DIR")
	if dir == "" {
		return
	}
	if os.MkdirAll(dir, 0700) != nil {
		return
	}
	if os.WriteFile(filepath.Join(dir, "reached"), []byte(point), 0600) != nil {
		return
	}
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-tick.C:
			if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
				return
			}
		}
	}
}
