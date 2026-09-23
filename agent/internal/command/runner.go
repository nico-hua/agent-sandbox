package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// Request describes a program invocation, its optional working directory,
// environment overrides, and maximum running time.
type Request struct {
	Argv    []string
	Cwd     string
	Env     map[string]string
	Timeout time.Duration
}

// Result reports the exit code produced by a completed command.
type Result struct {
	ExitCode int
}

// ErrEmptyArgv indicates that a request does not identify an executable.
var ErrEmptyArgv = errors.New("command argv must contain an executable")

// ErrInvalidTimeout indicates that a request contains a negative timeout.
var ErrInvalidTimeout = errors.New("command timeout must not be negative")

// Run executes request.Argv directly without a shell and writes each output
// stream to its corresponding writer. It reports context cancellation and
// runner failures separately from non-zero exits returned by user programs.
func Run(ctx context.Context, request Request, stdout io.Writer, stderr io.Writer) (Result, error) {
	if len(request.Argv) == 0 || request.Argv[0] == "" {
		return Result{ExitCode: -1}, ErrEmptyArgv
	}
	if request.Timeout < 0 {
		return Result{ExitCode: -1}, ErrInvalidTimeout
	}

	executionContext := ctx
	if request.Timeout > 0 {
		var cancel context.CancelFunc
		executionContext, cancel = context.WithTimeout(ctx, request.Timeout)
		defer cancel()
	}

	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}

	cmd := exec.CommandContext(executionContext, request.Argv[0], request.Argv[1:]...)
	configureProcessGroup(cmd)
	if request.Cwd != "" {
		cmd.Dir = request.Cwd
	}
	if len(request.Env) > 0 {
		cmd.Env = mergeEnvironment(cmd.Environ(), request.Env)
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	if err == nil {
		return Result{ExitCode: 0}, nil
	}

	if contextError := executionContext.Err(); contextError != nil {
		return Result{ExitCode: -1}, fmt.Errorf("run command %q: %w", request.Argv[0], contextError)
	}

	// A non-zero status from a started program is a command result, not a
	// failure of the runner itself.
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return Result{ExitCode: exitError.ExitCode()}, nil
	}

	return Result{ExitCode: -1}, fmt.Errorf("run command %q: %w", request.Argv[0], err)
}

// mergeEnvironment returns one entry per case-sensitive variable name, with
// overrides replacing inherited values and adding new variables.
func mergeEnvironment(base []string, overrides map[string]string) []string {
	merged := make([]string, 0, len(base)+len(overrides))
	positions := make(map[string]int, len(base)+len(overrides))

	for _, entry := range base {
		name, _, _ := strings.Cut(entry, "=")
		if position, exists := positions[name]; exists {
			merged[position] = entry
			continue
		}

		positions[name] = len(merged)
		merged = append(merged, entry)
	}

	for name, value := range overrides {
		entry := name + "=" + value
		if position, exists := positions[name]; exists {
			merged[position] = entry
			continue
		}

		positions[name] = len(merged)
		merged = append(merged, entry)
	}

	return merged
}
