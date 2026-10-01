package server

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/sandbox_agent/command"
)

// TestCommandStreamFramesPreserveSeparateBinaryStreams verifies ordered frames and nonzero completion.
func TestCommandStreamFramesPreserveSeparateBinaryStreams(t *testing.T) {
	runner := func(_ context.Context, _ command.Request, _ io.Reader, stdout io.Writer, stderr io.Writer) (command.Result, error) {
		if _, err := stdout.Write([]byte{0, 'a'}); err != nil {
			return command.Result{}, err
		}
		if _, err := stderr.Write([]byte("bad")); err != nil {
			return command.Result{}, err
		}
		return command.Result{ExitCode: 7}, nil
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/commands:stream", strings.NewReader("{\"argv\":[\"run\"]}"))
	newTestHandler(t, runner, 4).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", got)
	}
	want := "event: stdout\ndata: {\"data_base64\":\"AGE=\"}\n\n" +
		"event: stderr\ndata: {\"data_base64\":\"YmFk\"}\n\n" +
		"event: complete\ndata: {\"exit_code\":7}\n\n"
	if got := response.Body.String(); got != want {
		t.Errorf("SSE frames = %q, want %q", got, want)
	}
}

// readSSEFrame reads one complete event frame from a live HTTP response.
func readSSEFrame(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	var frame strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE frame: %v", err)
		}
		frame.WriteString(line)
		if line == "\n" {
			return frame.String()
		}
	}
}

// TestCommandStreamFlushesBeforeCommandCompletes catches buffered output until process exit.
func TestCommandStreamFlushesBeforeCommandCompletes(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	stop := func() { releaseOnce.Do(func() { close(release) }) }
	runnerDone := make(chan struct{})
	runner := func(_ context.Context, _ command.Request, _ io.Reader, stdout io.Writer, _ io.Writer) (command.Result, error) {
		defer close(runnerDone)
		if _, err := stdout.Write([]byte("first")); err != nil {
			return command.Result{}, err
		}
		<-release
		if _, err := stdout.Write([]byte("second")); err != nil {
			return command.Result{}, err
		}
		return command.Result{ExitCode: 0}, nil
	}
	server := httptest.NewServer(newTestHandler(t, runner, 1))
	defer func() { stop(); server.Close() }()
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Post(server.URL+"/v1/commands:stream", "application/json", strings.NewReader("{\"argv\":[\"run\"]}"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)

	if first := readSSEFrame(t, reader); first != "event: stdout\ndata: {\"data_base64\":\"Zmlyc3Q=\"}\n\n" {
		t.Errorf("first frame = %q", first)
	}
	select {
	case <-runnerDone:
		t.Fatal("command finished before client observed its first output")
	default:
	}
	stop()
	if second := readSSEFrame(t, reader); second != "event: stdout\ndata: {\"data_base64\":\"c2Vjb25k\"}\n\n" {
		t.Errorf("second frame = %q", second)
	}
	if complete := readSSEFrame(t, reader); complete != "event: complete\ndata: {\"exit_code\":0}\n\n" {
		t.Errorf("completion frame = %q", complete)
	}
}

// TestCommandStreamUsesSynchronousRequestPolicy verifies optional settings reach the shared runner.
func TestCommandStreamUsesSynchronousRequestPolicy(t *testing.T) {
	called := false
	runner := func(ctx context.Context, request command.Request, stdin io.Reader, _ io.Writer, _ io.Writer) (command.Result, error) {
		called = true
		if ctx == nil || request.Cwd != "/tmp" || request.Env["NAME"] != "stream" ||
			request.Timeout != 250*time.Millisecond || request.MaxOutputBytesPerStream != 9 {
			t.Errorf("runner settings = %+v, context = %v", request, ctx)
		}
		if deadline, ok := ctx.Deadline(); !ok || deadline.After(time.Now().Add(request.Timeout)) {
			t.Errorf("runner context has no request timeout deadline: %v", ctx)
		}
		input, err := io.ReadAll(stdin)
		if err != nil || string(input) != "hello" {
			t.Errorf("stdin = %q, error = %v", input, err)
		}
		return command.Result{ExitCode: 0}, nil
	}
	body := "{\"argv\":[\"cat\"],\"cwd\":\"/tmp\",\"env\":{\"NAME\":\"stream\"},\"stdin\":\"hello\",\"timeout_ms\":250,\"max_output_bytes_per_stream\":9}"
	response := httptest.NewRecorder()
	newTestHandler(t, runner, 1).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/commands:stream", strings.NewReader(body)))
	if !called || response.Code != http.StatusOK {
		t.Errorf("runner called = %t, status = %d", called, response.Code)
	}
}

// TestCommandStreamRejectsInvalidRequestsBeforeStarting verifies validation remains JSON and calls no runner.
func TestCommandStreamRejectsInvalidRequestsBeforeStarting(t *testing.T) {
	tests := []struct {
		name, method, body, code string
		status                   int
	}{
		{"empty argv", http.MethodPost, "{\"argv\":[]}", "invalid_request", http.StatusBadRequest},
		{"zero timeout", http.MethodPost, "{\"argv\":[\"true\"],\"timeout_ms\":0}", "invalid_request", http.StatusBadRequest},
		{"zero output limit", http.MethodPost, "{\"argv\":[\"true\"],\"max_output_bytes_per_stream\":0}", "invalid_request", http.StatusBadRequest},
		{"unknown field", http.MethodPost, "{\"argv\":[\"true\"],\"shell\":true}", "invalid_request", http.StatusBadRequest},
		{"malformed JSON", http.MethodPost, "{", "invalid_request", http.StatusBadRequest},
		{"wrong method", http.MethodGet, "", "method_not_allowed", http.StatusMethodNotAllowed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := func(context.Context, command.Request, io.Reader, io.Writer, io.Writer) (command.Result, error) {
				t.Error("invalid request called the runner")
				return command.Result{}, nil
			}
			response := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, "/v1/commands:stream", strings.NewReader(test.body))
			newTestHandler(t, runner, 1).ServeHTTP(response, request)
			if response.Code != test.status || response.Header().Get("Content-Type") != "application/json" ||
				!strings.Contains(response.Body.String(), "\"code\":\""+test.code+"\"") ||
				strings.Contains(response.Body.String(), "event:") {
				t.Errorf("response status = %d, body = %q", response.Code, response.Body.String())
			}
		})
	}
}

// TestCommandStreamFailureEvents verifies runner failures use terminal SSE frames after HTTP 200 starts.
func TestCommandStreamFailureEvents(t *testing.T) {
	tests := []struct {
		name, code, message string
		err                 error
	}{
		{"start", "command_start_failed", "command could not be started", errors.New("not found")},
		{"timeout", "command_timeout", "command execution timed out", context.DeadlineExceeded},
		{"limit", "output_limit_exceeded", "command output exceeded the limit", command.ErrOutputLimitExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := func(context.Context, command.Request, io.Reader, io.Writer, io.Writer) (command.Result, error) {
				return command.Result{ExitCode: -1}, test.err
			}
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/commands:stream", strings.NewReader("{\"argv\":[\"run\"]}"))
			newTestHandler(t, runner, 1).ServeHTTP(response, request)
			want := "event: failure\ndata: {\"error\":{\"code\":\"" + test.code + "\",\"message\":\"" + test.message + "\"}}\n\n"
			if response.Code != http.StatusOK || response.Body.String() != want {
				t.Errorf("status = %d, frames = %q, want %q", response.Code, response.Body.String(), want)
			}
		})
	}
}

// TestCommandStreamSharesSynchronousCapacity verifies both routes compete for the same execution slot.
func TestCommandStreamSharesSynchronousCapacity(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	stop := func() { releaseOnce.Do(func() { close(release) }) }
	defer stop()
	var calls atomic.Int32
	runner := func(context.Context, command.Request, io.Reader, io.Writer, io.Writer) (command.Result, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return command.Result{ExitCode: 0}, nil
	}
	handler := newTestHandler(t, runner, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/commands:stream", strings.NewReader("{\"argv\":[\"run\"]}")))
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not start its runner")
	}
	for _, path := range []string{"/v1/commands:run", "/v1/commands:stream"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{\"argv\":[\"run\"]}")))
		if response.Code != http.StatusTooManyRequests || !strings.Contains(response.Body.String(), "command_capacity_exceeded") {
			t.Errorf("%s status = %d, body = %q", path, response.Code, response.Body.String())
		}
	}
	if calls.Load() != 1 {
		t.Errorf("runner calls at capacity = %d, want 1", calls.Load())
	}
	stop()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not finish after release")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader("{\"argv\":[\"run\"]}")))
	if response.Code != http.StatusOK || calls.Load() != 2 {
		t.Errorf("request after release status = %d, runner calls = %d", response.Code, calls.Load())
	}
}

// TestCommandStreamRealFailures verifies runner errors become terminal failure events.
func TestCommandStreamRealFailures(t *testing.T) {
	tests := []struct {
		name, body, code string
	}{
		{"start", "{\"argv\":[\"/command-that-does-not-exist\"]}", "command_start_failed"},
		{"timeout", "{\"argv\":[\"sleep\",\"5\"],\"timeout_ms\":100}", "command_timeout"},
		{"output limit", "{\"argv\":[\"printf\",\"123456\"],\"max_output_bytes_per_stream\":5}", "output_limit_exceeded"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/commands:stream", strings.NewReader(test.body))
			newTestHandler(t, command.Run, 1).ServeHTTP(response, request)
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "event: failure\n") ||
				!strings.Contains(response.Body.String(), "\"code\":\""+test.code+"\"") {
				t.Errorf("status = %d, frames = %q", response.Code, response.Body.String())
			}
		})
	}
}

// TestCommandStreamClientDisconnectCancelsAndReleases verifies no command slot survives a closed stream.
func TestCommandStreamClientDisconnectCancelsAndReleases(t *testing.T) {
	exited := make(chan struct{})
	var calls atomic.Int32
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, stdout io.Writer, _ io.Writer) (command.Result, error) {
		if calls.Add(1) != 1 {
			return command.Result{ExitCode: 0}, nil
		}
		if _, err := stdout.Write([]byte("started")); err != nil {
			close(exited)
			return command.Result{ExitCode: -1}, err
		}
		<-ctx.Done()
		close(exited)
		return command.Result{ExitCode: -1}, ctx.Err()
	}
	server := httptest.NewServer(newTestHandler(t, runner, 1))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/commands:stream", strings.NewReader("{\"argv\":[\"run\"]}"))
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if frame := readSSEFrame(t, bufio.NewReader(response.Body)); frame != "event: stdout\ndata: {\"data_base64\":\"c3RhcnRlZA==\"}\n\n" {
		t.Errorf("first frame = %q", frame)
	}
	cancel()
	_ = response.Body.Close()
	select {
	case <-exited:
	case <-time.After(3 * time.Second):
		t.Fatal("runner was not canceled after client disconnect")
	}

	deadline := time.After(3 * time.Second)
	for {
		probe := httptest.NewRecorder()
		newRequest := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader("{\"argv\":[\"run\"]}"))
		server.Config.Handler.ServeHTTP(probe, newRequest)
		if probe.Code == http.StatusOK {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("command slot not released after disconnect: status %d", probe.Code)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestCommandStreamSerializesConcurrentWriters verifies concurrent streams never corrupt SSE frames.
func TestCommandStreamSerializesConcurrentWriters(t *testing.T) {
	runner := func(_ context.Context, _ command.Request, _ io.Reader, stdout io.Writer, stderr io.Writer) (command.Result, error) {
		var group sync.WaitGroup
		for _, writer := range []io.Writer{stdout, stderr} {
			group.Add(1)
			go func(writer io.Writer) {
				defer group.Done()
				for range 50 {
					if _, err := writer.Write([]byte("x")); err != nil {
						t.Errorf("write stream: %v", err)
						return
					}
				}
			}(writer)
		}
		group.Wait()
		return command.Result{ExitCode: 0}, nil
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/commands:stream", strings.NewReader("{\"argv\":[\"run\"]}"))
	newTestHandler(t, runner, 1).ServeHTTP(response, request)
	reader := bufio.NewReader(strings.NewReader(response.Body.String()))
	counts := map[string]int{"stdout": 0, "stderr": 0}
	for range 100 {
		frame := readSSEFrame(t, reader)
		lines := strings.Split(frame, "\n")
		if len(lines) != 4 || !strings.HasPrefix(lines[0], "event: ") ||
			!strings.HasPrefix(lines[1], "data: ") || lines[2] != "" || lines[3] != "" {
			t.Fatalf("corrupted SSE frame: %q", frame)
		}
		name := strings.TrimPrefix(lines[0], "event: ")
		if _, ok := counts[name]; !ok {
			t.Fatalf("unexpected output event %q", name)
		}
		var data streamOutputData
		if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &data); err != nil {
			t.Fatal(err)
		}
		decoded, err := base64.StdEncoding.DecodeString(data.DataBase64)
		if err != nil || string(decoded) != "x" {
			t.Fatalf("output data = %q, error = %v", decoded, err)
		}
		counts[name]++
	}
	if counts["stdout"] != 50 || counts["stderr"] != 50 {
		t.Errorf("stream event counts = %v, want 50 each", counts)
	}
	if final := readSSEFrame(t, reader); final != "event: complete\ndata: {\"exit_code\":0}\n\n" {
		t.Errorf("final frame = %q", final)
	}
}
