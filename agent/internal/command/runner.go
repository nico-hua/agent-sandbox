package command

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
)

// Request describes a program invocation and its optional working directory.
type Request struct {
	Argv []string
	Cwd  string
}

// Result reports the exit code produced by a completed command.
type Result struct {
	ExitCode int
}

// ErrEmptyArgv indicates that a request does not identify an executable.
var ErrEmptyArgv = errors.New("command argv must contain an executable")

// Run executes request.Argv directly without a shell and writes each output
// stream to its corresponding writer. It reports runner failures separately
// from non-zero exit codes returned by a successfully started program.
func Run(request Request, stdout io.Writer, stderr io.Writer) (Result, error) {
	if len(request.Argv) == 0 || request.Argv[0] == "" {
		return Result{ExitCode: -1}, ErrEmptyArgv
	}

	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}

	cmd := exec.Command(request.Argv[0], request.Argv[1:]...)
	if request.Cwd != "" {
		cmd.Dir = request.Cwd
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	if err == nil {
		return Result{ExitCode: 0}, nil
	}

	// A non-zero status from a started program is a command result, not a
	// failure of the runner itself.
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return Result{ExitCode: exitError.ExitCode()}, nil
	}

	return Result{ExitCode: -1}, fmt.Errorf("run command %q: %w", request.Argv[0], err)
}
