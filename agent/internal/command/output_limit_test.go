package command

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestRunLeavesOutputUnlimitedAtZero verifies that the zero value preserves all output.
func TestRunLeavesOutputUnlimitedAtZero(t *testing.T) {
	output := strings.Repeat("x", 4096)
	var stdout bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{Argv: []string{"printf", "%s", output}},
		nil,
		&stdout,
		nil,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got := stdout.String(); got != output {
		t.Errorf("stdout length = %d, want %d", len(got), len(output))
	}
}

// TestRunRejectsNegativeOutputLimitBeforeStart verifies validation happens before executing the program.
func TestRunRejectsNegativeOutputLimitBeforeStart(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")

	result, err := Run(
		context.Background(),
		Request{
			Argv:                    []string{"touch", marker},
			MaxOutputBytesPerStream: -1,
		},
		nil,
		io.Discard,
		io.Discard,
	)
	if !errors.Is(err, ErrInvalidOutputLimit) {
		t.Fatalf("Run() error = %v, want errors.Is(error, ErrInvalidOutputLimit)", err)
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("marker stat error = %v, want os.ErrNotExist", statErr)
	}
}

// TestRunAllowsStdoutBelowLimit verifies output below the per-stream limit remains complete.
func TestRunAllowsStdoutBelowLimit(t *testing.T) {
	var stdout bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv:                    []string{"printf", "%s", "four"},
			MaxOutputBytesPerStream: 5,
		},
		nil,
		&stdout,
		nil,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got := stdout.String(); got != "four" {
		t.Errorf("stdout = %q, want %q", got, "four")
	}
}

// TestRunAllowsStdoutEqualToLimit verifies reaching the limit exactly is not an overflow.
func TestRunAllowsStdoutEqualToLimit(t *testing.T) {
	var stdout bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv:                    []string{"printf", "%s", "exact"},
			MaxOutputBytesPerStream: 5,
		},
		nil,
		&stdout,
		nil,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got := stdout.String(); got != "exact" {
		t.Errorf("stdout = %q, want %q", got, "exact")
	}
}

// TestRunStopsWhenStdoutExceedsLimit verifies stdout is truncated and the command is canceled.
func TestRunStopsWhenStdoutExceedsLimit(t *testing.T) {
	var stdout bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv:                    []string{"printf", "%s", "too much"},
			MaxOutputBytesPerStream: 5,
		},
		nil,
		&stdout,
		nil,
	)
	if !errors.Is(err, ErrOutputLimitExceeded) {
		t.Fatalf("Run() error = %v, want errors.Is(error, ErrOutputLimitExceeded)", err)
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
	if got := stdout.String(); got != "too m" {
		t.Errorf("stdout = %q, want %q", got, "too m")
	}
}

// TestRunStopsWhenStderrExceedsLimit verifies stderr has the same independent limit behavior.
func TestRunStopsWhenStderrExceedsLimit(t *testing.T) {
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv:                    []string{"sh", "-c", "printf 'too much' >&2"},
			MaxOutputBytesPerStream: 5,
		},
		nil,
		nil,
		&stderr,
	)
	if !errors.Is(err, ErrOutputLimitExceeded) {
		t.Fatalf("Run() error = %v, want errors.Is(error, ErrOutputLimitExceeded)", err)
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
	if got := stderr.String(); got != "too m" {
		t.Errorf("stderr = %q, want %q", got, "too m")
	}
}

// TestRunLimitsOutputStreamsIndependently verifies one stream does not consume the other's allowance.
func TestRunLimitsOutputStreamsIndependently(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv:                    []string{"sh", "-c", "printf '12345'; printf 'abcde' >&2"},
			MaxOutputBytesPerStream: 5,
		},
		nil,
		&stdout,
		&stderr,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got := stdout.String(); got != "12345" {
		t.Errorf("stdout = %q, want %q", got, "12345")
	}
	if got := stderr.String(); got != "abcde" {
		t.Errorf("stderr = %q, want %q", got, "abcde")
	}
}

// TestRunEnforcesOutputLimitForNilWriter verifies discarded stdout and stderr are still counted.
func TestRunEnforcesOutputLimitForNilWriter(t *testing.T) {
	tests := []struct {
		name string
		argv []string
	}{
		{name: "stdout", argv: []string{"printf", "%s", "discarded"}},
		{name: "stderr", argv: []string{"sh", "-c", "printf 'discarded' >&2"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := Run(
				context.Background(),
				Request{
					Argv:                    test.argv,
					MaxOutputBytesPerStream: 3,
				},
				nil,
				nil,
				nil,
			)
			if !errors.Is(err, ErrOutputLimitExceeded) {
				t.Fatalf("Run() error = %v, want errors.Is(error, ErrOutputLimitExceeded)", err)
			}
			if result.ExitCode != -1 {
				t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
			}
		})
	}
}

// TestOutputLimitStateCancelsOnce verifies concurrent overflows share one cancellation action.
func TestOutputLimitStateCancelsOnce(t *testing.T) {
	var cancelCount atomic.Int32
	state := &outputLimitState{
		exceeded: make(chan struct{}),
		cancel: func() {
			cancelCount.Add(1)
		},
	}
	start := make(chan struct{})
	var writers sync.WaitGroup
	writers.Add(2)
	for range 2 {
		go func() {
			defer writers.Done()
			<-start
			state.markExceeded()
		}()
	}

	close(start)
	writers.Wait()
	if got := cancelCount.Load(); got != 1 {
		t.Errorf("cancel count = %d, want 1", got)
	}
	if !state.isExceeded() {
		t.Error("isExceeded() = false, want true")
	}
}

// TestRunStopsInfiniteOutput verifies an unbounded producer returns before the backup timeout.
func TestRunStopsInfiniteOutput(t *testing.T) {
	result, err := Run(
		context.Background(),
		Request{
			Argv:                    []string{"yes"},
			Timeout:                 5 * time.Second,
			MaxOutputBytesPerStream: 1024,
		},
		nil,
		io.Discard,
		io.Discard,
	)
	if !errors.Is(err, ErrOutputLimitExceeded) {
		t.Fatalf("Run() error = %v, want errors.Is(error, ErrOutputLimitExceeded)", err)
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
}

// TestRunOutputLimitKillsProcessGroup verifies an overflowing descendant is terminated with its group.
func TestRunOutputLimitKillsProcessGroup(t *testing.T) {
	workingDirectory := t.TempDir()
	gate := filepath.Join(workingDirectory, "gate")
	if err := syscall.Mkfifo(gate, 0o600); err != nil {
		t.Fatalf("create synchronization pipe: %v", err)
	}
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv: []string{
				"sh",
				"-c",
				`sh -c 'read value < gate; exec yes' & child=$!; printf '%s\n' "$child" >&2; printf 'go\n' > gate; wait`,
			},
			Cwd:                     workingDirectory,
			Timeout:                 5 * time.Second,
			MaxOutputBytesPerStream: 64,
		},
		nil,
		io.Discard,
		&stderr,
	)
	if !errors.Is(err, ErrOutputLimitExceeded) {
		t.Fatalf("Run() error = %v, want errors.Is(error, ErrOutputLimitExceeded)", err)
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}

	childPID, parseErr := strconv.Atoi(strings.TrimSpace(stderr.String()))
	if parseErr != nil {
		t.Fatalf("parse child PID from stderr %q: %v", stderr.String(), parseErr)
	}
	defer func() {
		_ = syscall.Kill(childPID, syscall.SIGKILL)
	}()
	waitForProcessExit(t, childPID)
}
