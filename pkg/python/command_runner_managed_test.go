//go:build !windows

package python

import (
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"go.uber.org/zap"
)

// TestCommandRunner_ManagedCommandStopsOnCancel verifies that a managed command
// is tied to the run context and reaped when it is cancelled. Managed commands
// use graceful streaming teardown rather than the immediate cancellation used
// by one-shot commands.
func TestCommandRunner_ManagedCommandStopsOnCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, executor.ContextLogger, zap.NewNop().Sugar())

	runner := &CommandRunner{}
	cmd := &CommandInstance{
		Name:    "sleep",
		Args:    []string{"120"},
		Managed: true,
	}

	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx, &git.Repo{Path: t.TempDir()}, cmd)
	}()

	// Let the child start, then cancel; the managed teardown sends SIGTERM to the
	// process group, which stops `sleep` well within the grace window.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Returned promptly: the managed child was signalled and reaped.
	case <-time.After(managedStopGrace + 5*time.Second):
		t.Fatal("managed command did not stop after context cancellation")
	}
}
