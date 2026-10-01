package command

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

type runOutcome struct {
	result Result
	err    error
}

type lineOutcome struct {
	line string
	err  error
}

// TestRunPassesStdinToCommand verifies that finite text input reaches the child unchanged.
func TestRunPassesStdinToCommand(t *testing.T) {
	const input = "command input"
	var stdout bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{Argv: []string{"cat"}},
		strings.NewReader(input),
		&stdout,
		nil,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got := stdout.String(); got != input {
		t.Errorf("stdout = %q, want exact stdin %q", got, input)
	}
}

// TestRunPassesMultilineStdinUnchanged verifies that line boundaries are not converted.
func TestRunPassesMultilineStdinUnchanged(t *testing.T) {
	const input = "first line\nsecond line\nthird line without trailing newline"
	var stdout bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{Argv: []string{"cat"}},
		strings.NewReader(input),
		&stdout,
		nil,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got := stdout.String(); got != input {
		t.Errorf("stdout = %q, want exact multiline stdin %q", got, input)
	}
}

// TestRunPassesBinaryStdinUnchanged verifies that non-text bytes remain intact.
func TestRunPassesBinaryStdinUnchanged(t *testing.T) {
	input := []byte{0x00, 0x01, 0x7f, 0x80, 0xff, '\n'}
	var stdout bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{Argv: []string{"cat"}},
		bytes.NewReader(input),
		&stdout,
		nil,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got := stdout.Bytes(); !bytes.Equal(got, input) {
		t.Errorf("stdout = %v, want exact binary stdin %v", got, input)
	}
}

// TestRunProvidesEOFWhenStdinIsNil verifies that nil input does not inherit a terminal or block the child.
func TestRunProvidesEOFWhenStdinIsNil(t *testing.T) {
	var stdout bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{Argv: []string{"cat"}, Timeout: time.Second},
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
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
}

// TestRunConnectsAllStandardStreams verifies that stdin, stdout, and stderr work together.
func TestRunConnectsAllStandardStreams(t *testing.T) {
	const input = "copied to both streams\n"
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{Argv: []string{"tee", "/dev/stderr"}},
		strings.NewReader(input),
		&stdout,
		&stderr,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("Run() ExitCode = %d, want 0", result.ExitCode)
	}
	if got := stdout.String(); got != input {
		t.Errorf("stdout = %q, want %q", got, input)
	}
	if got := stderr.String(); got != input {
		t.Errorf("stderr = %q, want %q", got, input)
	}
}

// TestRunCapturesStdout verifies that successful output reaches stdout.
func TestRunCapturesStdout(t *testing.T) {
	var stdout bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{Argv: []string{"printf", "%s", "hello"}, Timeout: 0},
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
	if got, want := stdout.String(), "hello"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
}

// TestRunCapturesStdoutAndStderrSeparately verifies that output streams remain independent.
func TestRunCapturesStdoutAndStderrSeparately(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{Argv: []string{"sh", "-c", "printf 'out'; printf 'err' >&2"}},
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
		context.Background(),
		Request{Argv: []string{"sh", "-c", "printf 'out'; printf 'err' >&2; exit 7"}},
		nil,
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
		context.Background(),
		Request{Argv: []string{"/command-runner-test-executable-that-does-not-exist"}},
		nil,
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

	result, err := Run(context.Background(), Request{}, nil, &stdout, &stderr)
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

	result, err := Run(context.Background(), Request{Argv: []string{""}}, nil, &stdout, &stderr)
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
		context.Background(),
		Request{Argv: []string{"sh", "-c", "printf 'discarded'"}},
		nil,
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
		context.Background(),
		Request{Argv: []string{"sh", "-c", "printf 'discarded' >&2"}},
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
}

// TestRunPassesArgumentsWithoutShellExpansionOrSplitting verifies that argv reaches the executable unchanged.
func TestRunPassesArgumentsWithoutShellExpansionOrSplitting(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	const argument = `hello world; $HOME * "quoted" 'single'`

	result, err := Run(
		context.Background(),
		Request{Argv: []string{"printf", "%s", argument}},
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
		context.Background(),
		Request{Argv: []string{"pwd"}, Cwd: workingDirectory},
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
		context.Background(),
		Request{Argv: []string{"cat", filename}, Cwd: workingDirectory},
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
		context.Background(),
		Request{Argv: []string{"pwd"}, Cwd: ""},
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
		context.Background(),
		Request{Argv: []string{"pwd"}, Cwd: missingDirectory},
		nil,
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
		context.Background(),
		Request{Argv: []string{"pwd"}, Cwd: workingDirectory},
		nil,
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

// TestRunInheritsAgentEnvironmentWhenEnvIsEmpty verifies that an empty Env preserves inherited variables.
func TestRunInheritsAgentEnvironmentWhenEnvIsEmpty(t *testing.T) {
	const name = "COMMAND_RUNNER_TEST_INHERITED"
	const value = "inherited value"
	t.Setenv(name, value)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv: []string{"sh", "-c", `printf '%s' "$COMMAND_RUNNER_TEST_INHERITED"`},
			Env:  map[string]string{},
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
	if got := stdout.String(); got != value {
		t.Errorf("stdout = %q, want inherited value %q", got, value)
	}
}

// TestRunAddsEnvironmentVariable verifies that Env can add a variable absent from the agent environment.
func TestRunAddsEnvironmentVariable(t *testing.T) {
	const name = "COMMAND_RUNNER_TEST_ADDED_8E27A4D1"
	const value = "request value"
	if _, exists := os.LookupEnv(name); exists {
		t.Fatalf("test variable %s unexpectedly exists in agent environment", name)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv: []string{"sh", "-c", `printf '%s' "$COMMAND_RUNNER_TEST_ADDED_8E27A4D1"`},
			Env:  map[string]string{name: value},
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
	if got := stdout.String(); got != value {
		t.Errorf("stdout = %q, want added value %q", got, value)
	}
}

// TestRunOverridesInheritedEnvironmentVariable verifies that request values take precedence over inherited values.
func TestRunOverridesInheritedEnvironmentVariable(t *testing.T) {
	const name = "COMMAND_RUNNER_TEST_OVERRIDE"
	const requestValue = "request value"
	t.Setenv(name, "agent value")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv: []string{"sh", "-c", `printf '%s' "$COMMAND_RUNNER_TEST_OVERRIDE"`},
			Env:  map[string]string{name: requestValue},
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
	if got := stdout.String(); got != requestValue {
		t.Errorf("stdout = %q, want override value %q", got, requestValue)
	}
}

// TestRunOverridesEnvironmentVariableWithEmptyValue verifies that an empty request value remains present and empty.
func TestRunOverridesEnvironmentVariableWithEmptyValue(t *testing.T) {
	const name = "COMMAND_RUNNER_TEST_EMPTY_OVERRIDE"
	t.Setenv(name, "agent value")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv: []string{"sh", "-c", `if [ "${COMMAND_RUNNER_TEST_EMPTY_OVERRIDE+x}" = x ]; then printf '%s' "$COMMAND_RUNNER_TEST_EMPTY_OVERRIDE"; else exit 9; fi`},
			Env:  map[string]string{name: ""},
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
	if got := stdout.String(); got != "" {
		t.Errorf("stdout = %q, want empty", got)
	}
}

// TestRunPassesEnvironmentValueWithoutExpansion verifies that variable-like text remains literal.
func TestRunPassesEnvironmentValueWithoutExpansion(t *testing.T) {
	const name = "COMMAND_RUNNER_TEST_LITERAL"
	const value = `$HOME/${USER}/*`
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv: []string{"sh", "-c", `printf '%s' "$COMMAND_RUNNER_TEST_LITERAL"`},
			Env:  map[string]string{name: value},
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
	if got := stdout.String(); got != value {
		t.Errorf("stdout = %q, want literal value %q", got, value)
	}
}

// TestRunDoesNotModifyAgentEnvironment verifies that request overrides remain local to the child process.
func TestRunDoesNotModifyAgentEnvironment(t *testing.T) {
	const name = "COMMAND_RUNNER_TEST_AGENT_UNCHANGED"
	const agentValue = "agent value"
	t.Setenv(name, agentValue)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv: []string{"sh", "-c", ":"},
			Env:  map[string]string{name: "request value"},
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
	if got := os.Getenv(name); got != agentValue {
		t.Errorf("agent environment value = %q, want %q", got, agentValue)
	}
}

// TestRunAppliesEnvironmentAndWorkingDirectory verifies that Env and Cwd take effect together.
func TestRunAppliesEnvironmentAndWorkingDirectory(t *testing.T) {
	workingDirectory := t.TempDir()
	const filename = "message.txt"
	const content = "read using environment and working directory"
	if err := os.WriteFile(filepath.Join(workingDirectory, filename), []byte(content), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv:    []string{"sh", "-c", `cat "$COMMAND_RUNNER_TEST_FILENAME"`},
			Cwd:     workingDirectory,
			Env:     map[string]string{"COMMAND_RUNNER_TEST_FILENAME": filename},
			Timeout: 5 * time.Second,
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
	if got := stdout.String(); got != content {
		t.Errorf("stdout = %q, want %q", got, content)
	}
	if got := stderr.String(); got != "" {
		t.Errorf("stderr = %q, want empty", got)
	}
}

// TestRunReturnsCanceledWhenContextIsCanceledBeforeStart verifies that a pre-canceled context prevents execution.
func TestRunReturnsCanceledWhenContextIsCanceledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		ctx,
		Request{Argv: []string{"printf", "%s", "should not run"}},
		nil,
		&stdout,
		&stderr,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want errors.Is(error, context.Canceled)", err)
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
	if got := stdout.String(); got != "" {
		t.Errorf("stdout = %q, want empty", got)
	}
}

// TestRunCancelsRunningCommand verifies cancellation after startup and bounds the wait for process exit.
func TestRunCancelsRunningCommand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdoutReader, stdoutWriter := io.Pipe()
	defer stdoutReader.Close()
	outcomes := make(chan runOutcome, 1)
	lines := make(chan lineOutcome, 1)

	go func() {
		result, err := Run(
			ctx,
			Request{
				Argv:    []string{"sh", "-c", `printf 'ready\n'; exec sleep 30`},
				Timeout: 30 * time.Second,
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

	select {
	case started := <-lines:
		if started.err != nil {
			t.Fatalf("read startup notification: %v", started.err)
		}
		if started.line != "ready\n" {
			t.Fatalf("startup notification = %q, want %q", started.line, "ready\n")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for command startup notification")
	}

	cancel()
	select {
	case outcome := <-outcomes:
		if !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("Run() error = %v, want errors.Is(error, context.Canceled)", outcome.err)
		}
		if outcome.result.ExitCode != -1 {
			t.Errorf("Run() ExitCode = %d, want -1", outcome.result.ExitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
}

// TestRunReturnsDeadlineExceeded verifies that an expired deadline is reported as a context error.
func TestRunReturnsDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	result, err := Run(
		ctx,
		Request{Argv: []string{"sh", "-c", "exec sleep 30"}},
		nil,
		&stdout,
		&stderr,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want errors.Is(error, context.DeadlineExceeded)", err)
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
}

// TestRunReapsCanceledDirectChild verifies that Run waits for the canceled direct child to disappear.
func TestRunReapsCanceledDirectChild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdoutReader, stdoutWriter := io.Pipe()
	defer stdoutReader.Close()
	outcomes := make(chan runOutcome, 1)
	lines := make(chan lineOutcome, 1)

	go func() {
		result, err := Run(
			ctx,
			Request{Argv: []string{"sh", "-c", `printf '%s\n' "$$"; exec sleep 30`}},
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

	var pid int
	select {
	case started := <-lines:
		if started.err != nil {
			t.Fatalf("read child PID: %v", started.err)
		}
		parsedPID, err := strconv.Atoi(strings.TrimSpace(started.line))
		if err != nil {
			t.Fatalf("parse child PID %q: %v", started.line, err)
		}
		pid = parsedPID
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for child PID")
	}

	cancel()
	select {
	case outcome := <-outcomes:
		if !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("Run() error = %v, want errors.Is(error, context.Canceled)", outcome.err)
		}
		if outcome.result.ExitCode != -1 {
			t.Errorf("Run() ExitCode = %d, want -1", outcome.result.ExitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatalf("find canceled process %d: %v", pid, err)
	}
	defer process.Release()
	if err := process.Signal(syscall.Signal(0)); !errors.Is(err, syscall.ESRCH) && !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("signal canceled process %d error = %v, want process-not-found error", pid, err)
	}
}

// TestRunReturnsDeadlineExceededWhenRequestTimesOut verifies that a positive Timeout terminates a long-running command.
func TestRunReturnsDeadlineExceededWhenRequestTimesOut(t *testing.T) {
	outcomes := make(chan runOutcome, 1)

	go func() {
		result, err := Run(
			context.Background(),
			Request{
				Argv:    []string{"sh", "-c", "exec sleep 30"},
				Timeout: 200 * time.Millisecond,
			},
			nil,
			io.Discard,
			io.Discard,
		)
		outcomes <- runOutcome{result: result, err: err}
	}()

	select {
	case outcome := <-outcomes:
		if !errors.Is(outcome.err, context.DeadlineExceeded) {
			t.Fatalf("Run() error = %v, want errors.Is(error, context.DeadlineExceeded)", outcome.err)
		}
		if outcome.result.ExitCode != -1 {
			t.Errorf("Run() ExitCode = %d, want -1", outcome.result.ExitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after request timeout")
	}
}

// TestRunRejectsNegativeTimeoutBeforeStart verifies that an invalid Timeout cannot start the requested program.
func TestRunRejectsNegativeTimeoutBeforeStart(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")

	result, err := Run(
		context.Background(),
		Request{
			Argv:    []string{"touch", marker},
			Timeout: -time.Second,
		},
		nil,
		io.Discard,
		io.Discard,
	)
	if !errors.Is(err, ErrInvalidTimeout) {
		t.Fatalf("Run() error = %v, want errors.Is(error, ErrInvalidTimeout)", err)
	}
	if result.ExitCode != -1 {
		t.Errorf("Run() ExitCode = %d, want -1", result.ExitCode)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("marker stat error = %v, want errors.Is(error, os.ErrNotExist)", err)
	}
}

// TestRunUsesEarlierParentDeadline verifies that a parent deadline takes precedence over Request.Timeout.
func TestRunUsesEarlierParentDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	outcomes := make(chan runOutcome, 1)

	go func() {
		result, err := Run(
			ctx,
			Request{
				Argv:    []string{"sh", "-c", "exec sleep 30"},
				Timeout: 5 * time.Second,
			},
			nil,
			io.Discard,
			io.Discard,
		)
		outcomes <- runOutcome{result: result, err: err}
	}()

	select {
	case outcome := <-outcomes:
		if !errors.Is(outcome.err, context.DeadlineExceeded) {
			t.Fatalf("Run() error = %v, want errors.Is(error, context.DeadlineExceeded)", outcome.err)
		}
		if outcome.result.ExitCode != -1 {
			t.Errorf("Run() ExitCode = %d, want -1", outcome.result.ExitCode)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not honor the earlier parent deadline")
	}
}

// TestRunCompletesBeforeRequestTimeout verifies that a fast command keeps its normal success result.
func TestRunCompletesBeforeRequestTimeout(t *testing.T) {
	var stdout bytes.Buffer

	result, err := Run(
		context.Background(),
		Request{
			Argv:    []string{"printf", "%s", "finished"},
			Timeout: 5 * time.Second,
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
	if got, want := stdout.String(), "finished"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
}

// TestRunReturnsNonZeroExitBeforeRequestTimeout verifies that user failures remain ordinary exit results.
func TestRunReturnsNonZeroExitBeforeRequestTimeout(t *testing.T) {
	result, err := Run(
		context.Background(),
		Request{
			Argv:    []string{"sh", "-c", "exit 9"},
			Timeout: 5 * time.Second,
		},
		nil,
		io.Discard,
		io.Discard,
	)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 9 {
		t.Errorf("Run() ExitCode = %d, want 9", result.ExitCode)
	}
}
