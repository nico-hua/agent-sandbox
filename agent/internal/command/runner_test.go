package command

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunCapturesStdout verifies that successful output reaches stdout.
func TestRunCapturesStdout(t *testing.T) {
	var stdout bytes.Buffer

	result, err := Run(Request{Argv: []string{"printf", "%s", "hello"}}, &stdout, nil)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got, want := stdout.String(), "hello"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
}

// TestRunCapturesStdoutAndStderrSeparately verifies that output streams remain independent.
func TestRunCapturesStdoutAndStderrSeparately(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		Request{Argv: []string{"sh", "-c", "printf 'out'; printf 'err' >&2"}},
		&stdout,
		&stderr,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got, want := stdout.String(), "out"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
	if got, want := stderr.String(), "err"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
}

// TestRunReturnsNonZeroExitCodeWithoutError verifies that a program's failure status is a result rather than a runner error.
func TestRunReturnsNonZeroExitCodeWithoutError(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		Request{Argv: []string{"sh", "-c", "printf 'out'; printf 'err' >&2; exit 7"}},
		&stdout,
		&stderr,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 7 {
		t.Errorf("Run() ExitCode = %d, want 7", result.ExitCode)
	}
	if got, want := stdout.String(), "out"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
	if got, want := stderr.String(), "err"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
}

// TestRunReturnsErrorWhenExecutableDoesNotExist verifies that launch failures return an error and the sentinel exit code.
func TestRunReturnsErrorWhenExecutableDoesNotExist(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		Request{Argv: []string{"/command-runner-test-executable-that-does-not-exist"}},
		&stdout,
		&stderr,
	)
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil")
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
}

// TestRunRejectsEmptyArgv verifies that a request without argv is rejected.
func TestRunRejectsEmptyArgv(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(Request{}, &stdout, &stderr)
	if !errors.Is(err, ErrEmptyArgv) {
		t.Fatalf("Run() error = %v, want errors.Is(error, ErrEmptyArgv)", err)
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
}

// TestRunRejectsEmptyExecutable verifies that an empty executable name is rejected.
func TestRunRejectsEmptyExecutable(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(Request{Argv: []string{""}}, &stdout, &stderr)
	if !errors.Is(err, ErrEmptyArgv) {
		t.Fatalf("Run() error = %v, want errors.Is(error, ErrEmptyArgv)", err)
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
}

// TestRunDiscardsStdoutWhenWriterIsNil verifies that a nil stdout writer does not prevent execution.
func TestRunDiscardsStdoutWhenWriterIsNil(t *testing.T) {
	var stderr bytes.Buffer

	result, err := Run(
		Request{Argv: []string{"sh", "-c", "printf 'discarded'"}},
		nil,
		&stderr,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
}

// TestRunDiscardsStderrWhenWriterIsNil verifies that a nil stderr writer does not prevent execution.
func TestRunDiscardsStderrWhenWriterIsNil(t *testing.T) {
	var stdout bytes.Buffer

	result, err := Run(
		Request{Argv: []string{"sh", "-c", "printf 'discarded' >&2"}},
		&stdout,
		nil,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
}

// TestRunPassesArgumentsWithoutShellExpansionOrSplitting verifies that argv reaches the executable unchanged.
func TestRunPassesArgumentsWithoutShellExpansionOrSplitting(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	const argument = `hello world; $HOME * "quoted" 'single'`

	result, err := Run(
		Request{Argv: []string{"printf", "%s", argument}},
		&stdout,
		&stderr,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got := stdout.String(); got != argument {
		t.Errorf("stdout = %q, want exact argument %q", got, argument)
	}
	if got := stderr.String(); got != "" {
		t.Errorf("stderr = %q, want empty", got)
	}
}

// TestRunUsesSpecifiedWorkingDirectory verifies that a non-empty Cwd becomes the process working directory.
func TestRunUsesSpecifiedWorkingDirectory(t *testing.T) {
	workingDirectory := t.TempDir()
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		Request{Argv: []string{"pwd"}, Cwd: workingDirectory},
		&stdout,
		&stderr,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}

	got, err := filepath.EvalSymlinks(strings.TrimSpace(stdout.String()))
	if err != nil {
		t.Fatalf("evaluate stdout path: %v", err)
	}
	want, err := filepath.EvalSymlinks(workingDirectory)
	if err != nil {
		t.Fatalf("evaluate working directory path: %v", err)
	}
	if got != want {
		t.Errorf("working directory = %q, want %q", got, want)
	}
}

// TestRunResolvesRelativePathsFromWorkingDirectory verifies that relative arguments are resolved from Cwd by the child process.
func TestRunResolvesRelativePathsFromWorkingDirectory(t *testing.T) {
	workingDirectory := t.TempDir()
	const filename = "message.txt"
	const content = "read from working directory"
	if err := os.WriteFile(filepath.Join(workingDirectory, filename), []byte(content), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		Request{Argv: []string{"cat", filename}, Cwd: workingDirectory},
		&stdout,
		&stderr,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got := stdout.String(); got != content {
		t.Errorf("stdout = %q, want %q", got, content)
	}
	if got := stderr.String(); got != "" {
		t.Errorf("stderr = %q, want empty", got)
	}
}

// TestRunInheritsWorkingDirectoryWhenCwdIsEmpty verifies that an empty Cwd preserves the agent's working directory.
func TestRunInheritsWorkingDirectoryWhenCwdIsEmpty(t *testing.T) {
	inheritedDirectory, err := os.Getwd()
	if err != nil {
		t.Fatalf("get current working directory: %v", err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		Request{Argv: []string{"pwd"}, Cwd: ""},
		&stdout,
		&stderr,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}

	got, err := filepath.EvalSymlinks(strings.TrimSpace(stdout.String()))
	if err != nil {
		t.Fatalf("evaluate stdout path: %v", err)
	}
	want, err := filepath.EvalSymlinks(inheritedDirectory)
	if err != nil {
		t.Fatalf("evaluate inherited directory path: %v", err)
	}
	if got != want {
		t.Errorf("working directory = %q, want inherited directory %q", got, want)
	}
}

// TestRunReturnsErrorWhenWorkingDirectoryDoesNotExist verifies that a missing Cwd is reported as a launch failure.
func TestRunReturnsErrorWhenWorkingDirectoryDoesNotExist(t *testing.T) {
	missingDirectory := filepath.Join(t.TempDir(), "missing")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		Request{Argv: []string{"pwd"}, Cwd: missingDirectory},
		&stdout,
		&stderr,
	)
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil")
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
}

// TestRunReturnsErrorWhenWorkingDirectoryIsFile verifies that a non-directory Cwd is reported as a launch failure.
func TestRunReturnsErrorWhenWorkingDirectoryIsFile(t *testing.T) {
	workingDirectory := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(workingDirectory, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		Request{Argv: []string{"pwd"}, Cwd: workingDirectory},
		&stdout,
		&stderr,
	)
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil")
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
}
