package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/agent/internal/command"
)

type recordingCommandRunner struct {
	calls      int
	ctx        context.Context
	request    command.Request
	stdin      io.Reader
	stdout     io.Writer
	stderr     io.Writer
	result     command.Result
	err        error
	stdoutText string
	stderrText string
	stdinText  string
}

// Run records the HTTP adapter inputs and returns the configured command outcome.
func (runner *recordingCommandRunner) Run(
	ctx context.Context,
	request command.Request,
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
) (command.Result, error) {
	runner.calls++
	runner.ctx = ctx
	runner.request = request
	runner.stdin = stdin
	runner.stdout = stdout
	runner.stderr = stderr
	if stdin != nil {
		input, err := io.ReadAll(stdin)
		if err != nil {
			return command.Result{ExitCode: -1}, fmt.Errorf("read stdin: %w", err)
		}
		runner.stdinText = string(input)
	}
	_, _ = io.WriteString(stdout, runner.stdoutText)
	_, _ = io.WriteString(stderr, runner.stderrText)
	return runner.result, runner.err
}

type commandRunResponse struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// TestCommandHandlerUsesDefaultExecutionPolicy verifies omitted options use safe service defaults.
func TestCommandHandlerUsesDefaultExecutionPolicy(t *testing.T) {
	runner := &recordingCommandRunner{
		result:     command.Result{ExitCode: 0},
		stdoutText: "hello",
		stderrText: "warning",
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/commands:run",
		strings.NewReader(`{"argv":["printf","%s","hello"]}`),
	)
	requestContext := request.Context()
	response := httptest.NewRecorder()

	NewHandler(runner.Run).ServeHTTP(response, request)

	if runner.calls != 1 {
		t.Fatalf("runner calls = %d, want 1", runner.calls)
	}
	if runner.ctx != requestContext {
		t.Error("runner context does not match request context")
	}
	if want := []string{"printf", "%s", "hello"}; !reflect.DeepEqual(runner.request.Argv, want) {
		t.Errorf("runner argv = %#v, want %#v", runner.request.Argv, want)
	}
	if runner.request.Timeout != 5*time.Second {
		t.Errorf("runner timeout = %v, want %v", runner.request.Timeout, 5*time.Second)
	}
	if runner.request.MaxOutputBytesPerStream != 64*1024 {
		t.Errorf("runner output limit = %d, want %d", runner.request.MaxOutputBytesPerStream, 64*1024)
	}
	if runner.request.Cwd != "" || runner.request.Env != nil {
		t.Errorf("runner optional fields = Cwd %q Env %v, want zero values", runner.request.Cwd, runner.request.Env)
	}
	if runner.stdin == nil || runner.stdinText != "" {
		t.Errorf("runner stdin = %v with content %q, want an empty reader", runner.stdin, runner.stdinText)
	}
	if runner.stdout == nil || runner.stderr == nil || runner.stdout == runner.stderr {
		t.Error("runner did not receive independent stdout and stderr writers")
	}

	got := decodeCommandResponse(t, response)
	if response.Code != http.StatusOK {
		t.Errorf("status code = %d, want %d", response.Code, http.StatusOK)
	}
	if got.ExitCode != 0 || got.Stdout != "hello" || got.Stderr != "warning" {
		t.Errorf("response = %#v, want exit 0 with separate output streams", got)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want %q", contentType, "application/json")
	}
}

// TestCommandHandlerPassesOptionalExecutionSettings verifies all client options map to the runner request.
func TestCommandHandlerPassesOptionalExecutionSettings(t *testing.T) {
	runner := &recordingCommandRunner{result: command.Result{ExitCode: 0}}
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/commands:run",
		strings.NewReader(`{
			"argv":["printf","%s","hello"],
			"cwd":"relative/workspace",
			"env":{"NAME":"sandbox","EMPTY":""},
			"stdin":"hello from stdin",
			"timeout_ms":3000,
			"max_output_bytes_per_stream":12345
		}`),
	)
	requestContext := request.Context()
	response := httptest.NewRecorder()

	NewHandler(runner.Run).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body.String())
	}
	if runner.ctx != requestContext {
		t.Error("runner context does not match request context")
	}
	if runner.request.Cwd != "relative/workspace" {
		t.Errorf("runner cwd = %q, want %q", runner.request.Cwd, "relative/workspace")
	}
	if want := map[string]string{"NAME": "sandbox", "EMPTY": ""}; !reflect.DeepEqual(runner.request.Env, want) {
		t.Errorf("runner env = %#v, want %#v", runner.request.Env, want)
	}
	if runner.stdinText != "hello from stdin" {
		t.Errorf("runner stdin = %q, want %q", runner.stdinText, "hello from stdin")
	}
	if runner.request.Timeout != 3*time.Second {
		t.Errorf("runner timeout = %v, want %v", runner.request.Timeout, 3*time.Second)
	}
	if runner.request.MaxOutputBytesPerStream != 12345 {
		t.Errorf("runner output limit = %d, want %d", runner.request.MaxOutputBytesPerStream, 12345)
	}
}

// TestCommandHandlerRejectsInvalidExecutionSettings verifies invalid options never reach the runner.
func TestCommandHandlerRejectsInvalidExecutionSettings(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "zero timeout", body: `{"argv":["true"],"timeout_ms":0}`},
		{name: "negative timeout", body: `{"argv":["true"],"timeout_ms":-1}`},
		{name: "timeout above maximum", body: `{"argv":["true"],"timeout_ms":30001}`},
		{name: "zero output limit", body: `{"argv":["true"],"max_output_bytes_per_stream":0}`},
		{name: "negative output limit", body: `{"argv":["true"],"max_output_bytes_per_stream":-1}`},
		{name: "output limit above maximum", body: `{"argv":["true"],"max_output_bytes_per_stream":1048577}`},
		{name: "empty environment name", body: `{"argv":["true"],"env":{"":"value"}}`},
		{name: "equals in environment name", body: `{"argv":["true"],"env":{"BAD=NAME":"value"}}`},
		{name: "nul in environment name", body: `{"argv":["true"],"env":{"BAD\u0000NAME":"value"}}`},
		{name: "nul in environment value", body: `{"argv":["true"],"env":{"NAME":"bad\u0000value"}}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &recordingCommandRunner{}
			request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(test.body))
			response := httptest.NewRecorder()

			NewHandler(runner.Run).ServeHTTP(response, request)

			assertErrorResponse(t, response, http.StatusBadRequest, "invalid_request")
			if runner.calls != 0 {
				t.Errorf("runner calls = %d, want 0", runner.calls)
			}
		})
	}
}

// TestCommandHandlerReturnsNonZeroExitAsSuccess verifies user exit codes remain normal results.
func TestCommandHandlerReturnsNonZeroExitAsSuccess(t *testing.T) {
	runner := &recordingCommandRunner{
		result:     command.Result{ExitCode: 7},
		stderrText: "command failed",
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(`{"argv":["false"]}`))
	response := httptest.NewRecorder()

	NewHandler(runner.Run).ServeHTTP(response, request)

	got := decodeCommandResponse(t, response)
	if response.Code != http.StatusOK {
		t.Errorf("status code = %d, want %d", response.Code, http.StatusOK)
	}
	if got.ExitCode != 7 || got.Stdout != "" || got.Stderr != "command failed" {
		t.Errorf("response = %#v, want non-zero command result", got)
	}
}

// TestCommandHandlerRejectsInvalidArgv verifies invalid command identity never reaches the runner.
func TestCommandHandlerRejectsInvalidArgv(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty argv", body: `{"argv":[]}`},
		{name: "empty executable", body: `{"argv":[""]}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &recordingCommandRunner{}
			request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(test.body))
			response := httptest.NewRecorder()

			NewHandler(runner.Run).ServeHTTP(response, request)

			assertErrorResponse(t, response, http.StatusBadRequest, "invalid_request")
			if runner.calls != 0 {
				t.Errorf("runner calls = %d, want 0", runner.calls)
			}
		})
	}
}

// TestCommandHandlerRejectsInvalidJSON verifies strict decoding rejects malformed or ambiguous bodies.
func TestCommandHandlerRejectsInvalidJSON(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed", body: `{"argv":[}`},
		{name: "empty", body: ``},
		{name: "unknown field", body: `{"argv":["true"],"unknown":true}`},
		{name: "multiple values", body: `{"argv":["true"]}{"argv":["false"]}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &recordingCommandRunner{}
			request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(test.body))
			response := httptest.NewRecorder()

			NewHandler(runner.Run).ServeHTTP(response, request)

			assertErrorResponse(t, response, http.StatusBadRequest, "invalid_request")
			if runner.calls != 0 {
				t.Errorf("runner calls = %d, want 0", runner.calls)
			}
		})
	}
}

// TestCommandHandlerAcceptsBodyAboveOldLimit verifies valid bodies may use the expanded request allowance.
func TestCommandHandlerAcceptsBodyAboveOldLimit(t *testing.T) {
	runner := &recordingCommandRunner{result: command.Result{ExitCode: 0}}
	stdin := strings.Repeat("x", 128*1024)
	body := `{"argv":["true"],"stdin":"` + stdin + `"}`
	request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(body))
	response := httptest.NewRecorder()

	NewHandler(runner.Run).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body.String())
	}
	if runner.stdinText != stdin {
		t.Errorf("runner stdin length = %d, want %d", len(runner.stdinText), len(stdin))
	}
}

// TestCommandHandlerRejectsOversizedBody verifies the decoder enforces the one MiB request limit.
func TestCommandHandlerRejectsOversizedBody(t *testing.T) {
	runner := &recordingCommandRunner{}
	body := `{"argv":["true"],"stdin":"` + strings.Repeat("x", 1024*1024) + `"}`
	request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(body))
	response := httptest.NewRecorder()

	NewHandler(runner.Run).ServeHTTP(response, request)

	assertErrorResponse(t, response, http.StatusRequestEntityTooLarge, "request_too_large")
	if runner.calls != 0 {
		t.Errorf("runner calls = %d, want 0", runner.calls)
	}
}

// TestCommandHandlerRunsWithWorkingDirectory verifies cwd is applied by the real runner.
func TestCommandHandlerRunsWithWorkingDirectory(t *testing.T) {
	workingDirectory := t.TempDir()
	body := fmt.Sprintf(`{"argv":["pwd"],"cwd":%q}`, workingDirectory)
	request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(body))
	response := httptest.NewRecorder()

	NewHandler(command.Run).ServeHTTP(response, request)

	got := decodeCommandResponse(t, response)
	actualPath, err := filepath.EvalSymlinks(strings.TrimSpace(got.Stdout))
	if err != nil {
		t.Fatalf("resolve command working directory: %v", err)
	}
	wantPath, err := filepath.EvalSymlinks(workingDirectory)
	if err != nil {
		t.Fatalf("resolve temporary working directory: %v", err)
	}
	if response.Code != http.StatusOK || actualPath != wantPath {
		t.Errorf("status = %d cwd = %q, want status %d cwd %q", response.Code, actualPath, http.StatusOK, wantPath)
	}
}

// TestCommandHandlerRunsWithEnvironmentAndStdin verifies environment overrides and input reach the real process.
func TestCommandHandlerRunsWithEnvironmentAndStdin(t *testing.T) {
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/commands:run",
		strings.NewReader(`{"argv":["sh","-c","printf '%s:' \"$NAME\"; cat"],"env":{"NAME":"sandbox"},"stdin":"hello"}`),
	)
	response := httptest.NewRecorder()

	NewHandler(command.Run).ServeHTTP(response, request)

	got := decodeCommandResponse(t, response)
	if response.Code != http.StatusOK || got.ExitCode != 0 || got.Stdout != "sandbox:hello" || got.Stderr != "" {
		t.Errorf("status = %d response = %#v, want environment and stdin output", response.Code, got)
	}
}

// TestCommandHandlerHonorsCustomTimeout verifies a short client timeout maps to the existing timeout error.
func TestCommandHandlerHonorsCustomTimeout(t *testing.T) {
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/commands:run",
		strings.NewReader(`{"argv":["sleep","30"],"timeout_ms":50}`),
	)
	response := httptest.NewRecorder()

	NewHandler(command.Run).ServeHTTP(response, request)

	assertErrorResponse(t, response, http.StatusGatewayTimeout, "command_timeout")
}

// TestCommandHandlerHonorsCustomOutputLimit verifies a small client limit maps to the existing limit error.
func TestCommandHandlerHonorsCustomOutputLimit(t *testing.T) {
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/commands:run",
		strings.NewReader(`{"argv":["printf","%s","1234567890"],"max_output_bytes_per_stream":5}`),
	)
	response := httptest.NewRecorder()

	NewHandler(command.Run).ServeHTTP(response, request)

	assertErrorResponse(t, response, http.StatusRequestEntityTooLarge, "output_limit_exceeded")
}

// TestCommandHandlerMapsInvalidWorkingDirectory verifies cwd startup failures retain their public mapping.
func TestCommandHandlerMapsInvalidWorkingDirectory(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	body := fmt.Sprintf(`{"argv":["pwd"],"cwd":%q}`, missing)
	request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(body))
	response := httptest.NewRecorder()

	NewHandler(command.Run).ServeHTTP(response, request)

	assertErrorResponse(t, response, http.StatusUnprocessableEntity, "command_start_failed")
}

// TestCommandHandlerReturnsSeparateRealStreams verifies stdout and stderr remain distinct with custom options.
func TestCommandHandlerReturnsSeparateRealStreams(t *testing.T) {
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/commands:run",
		strings.NewReader(`{"argv":["sh","-c","printf out; printf err >&2"],"timeout_ms":1000,"max_output_bytes_per_stream":1024}`),
	)
	response := httptest.NewRecorder()

	NewHandler(command.Run).ServeHTTP(response, request)

	got := decodeCommandResponse(t, response)
	if response.Code != http.StatusOK || got.Stdout != "out" || got.Stderr != "err" {
		t.Errorf("status = %d response = %#v, want separate streams", response.Code, got)
	}
}

// TestCommandHandlerMapsRunnerErrors verifies runner failures receive stable public responses.
func TestCommandHandlerMapsRunnerErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		statusCode int
		code       string
	}{
		{
			name:       "wrapped output limit",
			err:        fmt.Errorf("wrapped: %w", command.ErrOutputLimitExceeded),
			statusCode: http.StatusRequestEntityTooLarge,
			code:       "output_limit_exceeded",
		},
		{
			name:       "deadline",
			err:        fmt.Errorf("wrapped: %w", context.DeadlineExceeded),
			statusCode: http.StatusGatewayTimeout,
			code:       "command_timeout",
		},
		{
			name:       "start failure",
			err:        errors.New("executable not found"),
			statusCode: http.StatusUnprocessableEntity,
			code:       "command_start_failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &recordingCommandRunner{err: test.err}
			request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(`{"argv":["run"]}`))
			response := httptest.NewRecorder()

			NewHandler(runner.Run).ServeHTTP(response, request)

			assertErrorResponse(t, response, test.statusCode, test.code)
		})
	}
}

// TestCommandHandlerEndsWithoutResponseWhenRequestIsCanceled verifies cancellation reaches the runner.
func TestCommandHandlerEndsWithoutResponseWhenRequestIsCanceled(t *testing.T) {
	runner := &recordingCommandRunner{err: context.Canceled}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(`{"argv":["sleep","30"]}`)).WithContext(ctx)
	response := httptest.NewRecorder()

	NewHandler(runner.Run).ServeHTTP(response, request)

	if runner.calls != 1 {
		t.Fatalf("runner calls = %d, want 1", runner.calls)
	}
	if !errors.Is(runner.ctx.Err(), context.Canceled) {
		t.Errorf("runner context error = %v, want context.Canceled", runner.ctx.Err())
	}
	if response.Body.Len() != 0 || response.Header().Get("Content-Type") != "" {
		t.Errorf("canceled response body = %q headers = %v, want no response", response.Body.String(), response.Header())
	}
}

// TestCommandHandlerRejectsUnsupportedMethods verifies the command route only accepts POST.
func TestCommandHandlerRejectsUnsupportedMethods(t *testing.T) {
	runner := &recordingCommandRunner{}
	request := httptest.NewRequest(http.MethodGet, "/v1/commands:run", nil)
	response := httptest.NewRecorder()

	NewHandler(runner.Run).ServeHTTP(response, request)

	assertErrorResponse(t, response, http.StatusMethodNotAllowed, "method_not_allowed")
	if runner.calls != 0 {
		t.Errorf("runner calls = %d, want 0", runner.calls)
	}
}

// TestCommandHandlerRunsRealCommand verifies the HTTP adapter works with the real CommandRunner.
func TestCommandHandlerRunsRealCommand(t *testing.T) {
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/commands:run",
		strings.NewReader(`{"argv":["printf","%s","hello"]}`),
	)
	response := httptest.NewRecorder()

	NewHandler(command.Run).ServeHTTP(response, request)

	got := decodeCommandResponse(t, response)
	if response.Code != http.StatusOK || got.ExitCode != 0 || got.Stdout != "hello" || got.Stderr != "" {
		t.Errorf("status = %d response = %#v, want successful printf result", response.Code, got)
	}
}

// TestCommandHandlerReturnsRealNonZeroExit verifies real user exit statuses stay HTTP successes.
func TestCommandHandlerReturnsRealNonZeroExit(t *testing.T) {
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/commands:run",
		strings.NewReader(`{"argv":["sh","-c","printf error >&2; exit 7"]}`),
	)
	response := httptest.NewRecorder()

	NewHandler(command.Run).ServeHTTP(response, request)

	got := decodeCommandResponse(t, response)
	if response.Code != http.StatusOK || got.ExitCode != 7 || got.Stdout != "" || got.Stderr != "error" {
		t.Errorf("status = %d response = %#v, want exit 7 result", response.Code, got)
	}
}

// TestCommandHandlerPassesShellCharactersLiterally verifies the endpoint never adds an implicit shell.
func TestCommandHandlerPassesShellCharactersLiterally(t *testing.T) {
	const argument = "$HOME; echo injected"
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/commands:run",
		strings.NewReader(`{"argv":["printf","%s","$HOME; echo injected"]}`),
	)
	response := httptest.NewRecorder()

	NewHandler(command.Run).ServeHTTP(response, request)

	got := decodeCommandResponse(t, response)
	if response.Code != http.StatusOK || got.Stdout != argument {
		t.Errorf("status = %d stdout = %q, want literal %q", response.Code, got.Stdout, argument)
	}
}

// decodeCommandResponse decodes a successful command response or fails the test.
func decodeCommandResponse(t *testing.T, response *httptest.ResponseRecorder) commandRunResponse {
	t.Helper()
	var decoded commandRunResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode command response: %v", err)
	}
	return decoded
}

// assertErrorResponse verifies the public HTTP status and stable JSON error code.
func assertErrorResponse(t *testing.T, response *httptest.ResponseRecorder, statusCode int, code string) {
	t.Helper()
	if response.Code != statusCode {
		t.Errorf("status code = %d, want %d", response.Code, statusCode)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want %q", contentType, "application/json")
	}
	var decoded errorResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if decoded.Error.Code != code {
		t.Errorf("error code = %q, want %q", decoded.Error.Code, code)
	}
	if decoded.Error.Message == "" {
		t.Error("error message is empty")
	}
}
