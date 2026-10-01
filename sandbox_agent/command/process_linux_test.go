package command

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestConfigureProcessGroup isolates the command and makes cancellation safe before startup.
func TestConfigureProcessGroup(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), "true")

	configureProcessGroup(cmd)

	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatal("configureProcessGroup() did not enable Setpgid")
	}
	if cmd.Cancel == nil {
		t.Fatal("configureProcessGroup() left Cancel nil")
	}
	if err := cmd.Cancel(); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("Cancel() error = %v, want errors.Is(error, os.ErrProcessDone)", err)
	}
}

// TestRunCancellationKillsProcessGroup verifies that parent cancellation stops the command and its descendant.
func TestRunCancellationKillsProcessGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parentPID, childPID, outcomes := startProcessTree(t, ctx, 30*time.Second)
	finished := false
	defer func() {
		if !finished {
			_ = syscall.Kill(parentPID, syscall.SIGKILL)
			_ = syscall.Kill(childPID, syscall.SIGKILL)
		}
	}()

	assertIsolatedProcessGroup(t, parentPID)
	cancel()
	outcome := waitForRunOutcome(t, outcomes)
	if !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("Run() error = %v, want errors.Is(error, context.Canceled)", outcome.err)
	}
	if outcome.result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", outcome.result.ExitCode)
	}

	waitForProcessExit(t, parentPID)
	waitForProcessExit(t, childPID)
	finished = true
	if err := syscall.Kill(os.Getpid(), 0); err != nil {
		t.Fatalf("test process is not running after group cancellation: %v", err)
	}
}

// TestRunTimeoutKillsProcessGroup verifies that a request timeout stops the command and its descendant.
func TestRunTimeoutKillsProcessGroup(t *testing.T) {
	parentPID, childPID, outcomes := startProcessTree(t, context.Background(), time.Second)
	finished := false
	defer func() {
		if !finished {
			_ = syscall.Kill(parentPID, syscall.SIGKILL)
			_ = syscall.Kill(childPID, syscall.SIGKILL)
		}
	}()

	assertIsolatedProcessGroup(t, parentPID)
	outcome := waitForRunOutcome(t, outcomes)
	if !errors.Is(outcome.err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want errors.Is(error, context.DeadlineExceeded)", outcome.err)
	}
	if outcome.result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", outcome.result.ExitCode)
	}

	waitForProcessExit(t, parentPID)
	waitForProcessExit(t, childPID)
	finished = true
}

// startProcessTree starts a shell and background child, returning their PIDs only after both exist.
func startProcessTree(t *testing.T, ctx context.Context, timeout time.Duration) (int, int, <-chan runOutcome) {
	t.Helper()
	stdoutReader, stdoutWriter := io.Pipe()
	t.Cleanup(func() {
		_ = stdoutReader.Close()
	})
	outcomes := make(chan runOutcome, 1)
	lines := make(chan lineOutcome, 1)

	go func() {
		result, err := Run(
			ctx,
			Request{
				Argv: []string{
					"sh",
					"-c",
					`sleep 30 >/dev/null 2>&1 & child=$!; printf '%s %s\n' "$$" "$child"; wait`,
				},
				Timeout: timeout,
			},
			nil,
			stdoutWriter,
			io.Discard,
		)
		_ = stdoutWriter.Close()
		outcomes <- runOutcome{result: result, err: err}
	}()
	go func() {
		line, err := bufio.NewReader(stdoutReader).ReadString('\n')
		lines <- lineOutcome{line: line, err: err}
	}()

	var started lineOutcome
	select {
	case started = <-lines:
		if started.err != nil {
			t.Fatalf("read process tree PIDs: %v", started.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for process tree PIDs")
	}

	fields := strings.Fields(started.line)
	if len(fields) != 2 {
		t.Fatalf("process tree notification = %q, want parent and child PID", started.line)
	}
	parentPID, err := strconv.Atoi(fields[0])
	if err != nil {
		t.Fatalf("parse parent PID %q: %v", fields[0], err)
	}
	childPID, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatalf("parse child PID %q: %v", fields[1], err)
	}
	return parentPID, childPID, outcomes
}

// assertIsolatedProcessGroup verifies that the command leads a group separate from the test process.
func assertIsolatedProcessGroup(t *testing.T, commandPID int) {
	t.Helper()
	commandGroup, err := syscall.Getpgid(commandPID)
	if err != nil {
		t.Fatalf("get command process group: %v", err)
	}
	if commandGroup != commandPID {
		t.Fatalf("command process group = %d, want command PID %d", commandGroup, commandPID)
	}
	if commandGroup == syscall.Getpgrp() {
		t.Fatalf("command process group %d matches test process group", commandGroup)
	}
}

// waitForRunOutcome waits for Run to finish with a bounded test-only deadline.
func waitForRunOutcome(t *testing.T, outcomes <-chan runOutcome) runOutcome {
	t.Helper()
	select {
	case outcome := <-outcomes:
		return outcome
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after process group cancellation")
		return runOutcome{}
	}
}

// waitForProcessExit polls until a process no longer exists or the test deadline expires.
func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if err != nil {
			t.Fatalf("check process %d: %v", pid, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d still exists after cancellation deadline", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
