package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/sandbox_agent/command"
)

// taskTestHandler creates an owned manager with a final bounded cleanup and private test policy.
func taskTestHandler(t *testing.T, runner CommandRunner, slots int, policy taskPolicy) *Handler {
	t.Helper()
	handler, err := newHandler(runner, slots, policy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := handler.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return handler
}

// TestBackgroundValidationAndIDs checks request rejection, method errors and opaque ID boundaries.
func TestBackgroundValidationAndIDs(t *testing.T) {
	runner := func(_ context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		t.Error("runner called for invalid request")
		return command.Result{}, nil
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	for _, body := range []string{`{`, `{}`, `null`, `[]`, `{"argv":[]}`, `{"argv":[""]}`, `{"argv":["x"],"unknown":1}`, `{"argv":["x"],"env":{"BAD=NAME":"x"}}`, `{"argv":["x"],"timeout_ms":0}`, `{"argv":["x"],"timeout_ms":30001}`, `{"argv":["x"],"max_output_bytes_per_stream":1048577}`, `{"argv":["x"],"timeout_ms":1.5}`} {
		assertErrorResponse(t, taskRequest(handler, http.MethodPost, "/v1/commands", body), 400, "invalid_request")
	}
	assertErrorResponse(t, taskRequest(handler, http.MethodPost, "/v1/commands", `{"argv":["fake"],"stdin":"`+strings.Repeat("x", commandRequestBodyLimit)+`"}`), 413, "request_too_large")
	assertErrorResponse(t, taskRequest(handler, http.MethodGet, "/v1/commands", ""), 405, "method_not_allowed")
	for _, id := range []string{"123", "cmd_BAD", "cmd_" + strings.Repeat("f", 32), "../etc/passwd", "%2e%2e", ""} {
		for _, path := range []string{"/v1/commands/" + id, "/v1/commands/" + id + "/cancel"} {
			method := http.MethodGet
			if strings.HasSuffix(path, "/cancel") {
				method = http.MethodPost
			}
			assertErrorResponse(t, taskRequest(handler, method, path, ""), 404, "command_not_found")
		}
	}
	valid := "/v1/commands/cmd_" + strings.Repeat("a", 32)
	assertErrorResponse(t, taskRequest(handler, http.MethodDelete, valid, ""), 405, "method_not_allowed")
	assertErrorResponse(t, taskRequest(handler, http.MethodGet, valid+"/cancel", ""), 405, "method_not_allowed")
	if len(handler.tasks.tasks) != 0 || len(handler.tasks.limiter.slots) != 0 {
		t.Fatal("invalid requests consumed capacity")
	}
}

// TestBackgroundCancellationKeepsSharedSlot verifies cancel intent cannot release capacity before Runner cleanup.
func TestBackgroundCancellationKeepsSharedSlot(t *testing.T) {
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return command.Result{}, ctx.Err()
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	<-started
	for range 2 {
		response := taskRequest(handler, http.MethodPost, "/v1/commands/"+id+"/cancel", "")
		if response.Code != 202 || !strings.Contains(response.Body.String(), `"status":"running"`) {
			t.Fatalf("cancel: %s", response.Body.String())
		}
	}
	<-canceled
	for _, path := range []string{"/v1/commands", "/v1/commands:run", "/v1/commands:stream"} {
		assertErrorResponse(t, taskRequest(handler, http.MethodPost, path, `{"argv":["blocked"]}`), 429, "command_capacity_exceeded")
	}
	if response := taskRequest(handler, http.MethodGet, "/v1/commands/"+id, ""); response.Code != 200 {
		t.Fatal("query blocked by capacity")
	}
	release <- struct{}{}
	if snapshot := awaitTask(t, handler, id); snapshot["status"] != "canceled" || snapshot["error"].(map[string]any)["code"] != "command_canceled" {
		t.Fatalf("snapshot: %v", snapshot)
	}
	if response := taskRequest(handler, http.MethodPost, "/v1/commands/"+id+"/cancel", ""); response.Code != 200 {
		t.Fatal("terminal cancel did not return 200")
	}
}

// TestForegroundModesBlockBackground verifies background registration uses the existing sync/SSE slots.
func TestForegroundModesBlockBackground(t *testing.T) {
	for _, foreground := range []string{"/v1/commands:run", "/v1/commands:stream"} {
		t.Run(foreground, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			runner := func(_ context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
				close(started)
				<-release
				return command.Result{}, nil
			}
			handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
			finished := make(chan struct{})
			go func() { defer close(finished); taskRequest(handler, http.MethodPost, foreground, `{"argv":["fake"]}`) }()
			<-started
			assertErrorResponse(t, taskRequest(handler, http.MethodPost, "/v1/commands", `{"argv":["fake"]}`), 429, "command_capacity_exceeded")
			release <- struct{}{}
			<-finished
			if len(handler.tasks.tasks) != 0 {
				t.Fatal("rejected background command was registered")
			}
		})
	}
}

// TestBackgroundNaturalResultWinsCancel verifies final Runner evidence wins a concurrent cancellation request.
func TestBackgroundNaturalResultWinsCancel(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		close(started)
		<-ctx.Done()
		<-release
		return command.Result{ExitCode: 9}, nil
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	<-started
	_ = taskRequest(handler, http.MethodPost, "/v1/commands/"+id+"/cancel", "")
	release <- struct{}{}
	if result := awaitTask(t, handler, id); result["status"] != "exited" || result["exit_code"] != float64(9) {
		t.Fatalf("late cancel rewrote outcome: %v", result)
	}
}

type taskClock struct {
	mu      sync.Mutex
	current time.Time
}

// now returns a thread-safe fixed clock for acceptance, completion and retention tests.
func (clock *taskClock) now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.current
}

// advance moves the test clock without sleeping or racing Runner completion.
func (clock *taskClock) advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.current = clock.current.Add(duration)
}

// TestBackgroundRegistryRetention checks precise expiry, no early eviction, and reusable execution slots.
func TestBackgroundRegistryRetention(t *testing.T) {
	clock := &taskClock{current: time.Date(2026, 10, 21, 10, 0, 0, 123, time.FixedZone("test", 8*3600))}
	policy := taskPolicy{capacity: 2, retention: time.Minute, now: clock.now}
	runner := func(_ context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		return command.Result{}, nil
	}
	handler := taskTestHandler(t, runner, 2, policy)
	id := submittedTask(t, handler, `{"argv":["one"]}`)
	first := awaitTask(t, handler, id)
	if first["created_at"] != "2026-10-21T02:00:00.000000123Z" || first["finished_at"] != first["created_at"] {
		t.Fatalf("UTC times: %v", first)
	}
	second := submittedTask(t, handler, `{"argv":["two"]}`)
	_ = awaitTask(t, handler, second)
	clock.advance(time.Minute - time.Nanosecond)
	assertErrorResponse(t, taskRequest(handler, http.MethodPost, "/v1/commands", `{"argv":["full"]}`), 429, "command_registry_full")
	clock.advance(time.Nanosecond)
	assertErrorResponse(t, taskRequest(handler, http.MethodGet, "/v1/commands/"+id, ""), 404, "command_not_found")
	assertErrorResponse(t, taskRequest(handler, http.MethodPost, "/v1/commands/"+second+"/cancel", ""), 404, "command_not_found")
	third := submittedTask(t, handler, `{"argv":["three"]}`)
	_ = awaitTask(t, handler, third)
	if err := handler.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(handler.tasks.limiter.slots) != 0 {
		t.Fatal("registry rejection leaked an execution slot")
	}
}

// TestBackgroundRunningRecordsNeverExpire verifies retention cannot evict accepted work or hide its cancel handle.
func TestBackgroundRunningRecordsNeverExpire(t *testing.T) {
	clock := &taskClock{current: time.Now()}
	started := make(chan struct{}, 2)
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		started <- struct{}{}
		<-ctx.Done()
		return command.Result{}, ctx.Err()
	}
	handler := taskTestHandler(t, runner, 3, taskPolicy{capacity: 2, retention: time.Second, now: clock.now})
	first := submittedTask(t, handler, `{"argv":["one"]}`)
	second := submittedTask(t, handler, `{"argv":["two"]}`)
	<-started
	<-started
	clock.advance(time.Hour)
	assertErrorResponse(t, taskRequest(handler, http.MethodPost, "/v1/commands", `{"argv":["full"]}`), 429, "command_registry_full")
	if len(handler.tasks.limiter.slots) != 2 {
		t.Fatal("registry capacity attempt took a slot")
	}
	for _, id := range []string{first, second} {
		if response := taskRequest(handler, http.MethodGet, "/v1/commands/"+id, ""); response.Code != 200 || !strings.Contains(response.Body.String(), `"status":"running"`) {
			t.Fatalf("live task was evicted: %s", response.Body.String())
		}
		if response := taskRequest(handler, http.MethodPost, "/v1/commands/"+id+"/cancel", ""); response.Code != 202 {
			t.Fatal("live cancel lost")
		}
	}
}

// TestBackgroundCanceledBeforeRegistration verifies a caller canceled while admission waits cannot create work.
func TestBackgroundCanceledBeforeRegistration(t *testing.T) {
	runner := func(_ context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		t.Error("pre-canceled task started")
		return command.Result{}, nil
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	ctx, cancel := context.WithCancel(context.Background())
	handler.tasks.mu.Lock()
	entered, outcome := make(chan struct{}), make(chan error, 1)
	go func() {
		close(entered)
		_, err := handler.tasks.submit(ctx, command.Request{Argv: []string{"fake"}, MaxOutputBytesPerStream: 32}, "")
		outcome <- err
	}()
	<-entered
	cancel()
	handler.tasks.mu.Unlock()
	if err := <-outcome; !errors.Is(err, context.Canceled) {
		t.Fatalf("admission error: %v", err)
	}
	if len(handler.tasks.tasks) != 0 || len(handler.tasks.limiter.slots) != 0 {
		t.Fatal("pre-canceled admission left state")
	}
}

// TestBackgroundConcurrentShutdownAndSubmissions verifies closing serializes with registration and WG additions.
func TestBackgroundConcurrentShutdownAndSubmissions(t *testing.T) {
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		<-ctx.Done()
		return command.Result{}, ctx.Err()
	}
	handler := taskTestHandler(t, runner, 4, defaultTaskPolicy())
	start := make(chan struct{})
	var submitters sync.WaitGroup
	for range 30 {
		submitters.Add(1)
		go func() {
			defer submitters.Done()
			<-start
			_, err := handler.tasks.submit(context.Background(), command.Request{Argv: []string{"fake"}, MaxOutputBytesPerStream: 32}, "")
			if err != nil && !errors.Is(err, errAgentStopping) && !errors.Is(err, errCommandCapacity) && !errors.Is(err, errCommandRegistryFull) {
				t.Errorf("unexpected admission error: %v", err)
			}
		}()
	}
	close(start)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := handler.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	submitters.Wait()
	if len(handler.tasks.limiter.slots) != 0 {
		t.Fatal("shutdown leaked slots")
	}
	for _, task := range handler.tasks.tasks {
		if task.snapshot.Status != "canceled" {
			t.Fatalf("unsettled state: %s", task.snapshot.Status)
		}
	}
}

// TestBackgroundOwnsInputs verifies submitted argv/env cannot change after registration.
func TestBackgroundOwnsInputs(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	observed := make(chan command.Request, 1)
	runner := func(_ context.Context, request command.Request, input io.Reader, _, _ io.Writer) (command.Result, error) {
		close(entered)
		<-release
		stdin, _ := io.ReadAll(input)
		if string(stdin) != "owned stdin" {
			t.Errorf("stdin: %s", stdin)
		}
		observed <- request
		return command.Result{}, nil
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	request := command.Request{Argv: []string{"original", "argument"}, Env: map[string]string{"KEY": "value"}, Cwd: "/tmp", Timeout: time.Second, MaxOutputBytesPerStream: 32}
	snapshot, err := handler.tasks.submit(context.Background(), request, "owned stdin")
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	request.Argv[0] = "changed"
	request.Env["KEY"] = "changed"
	release <- struct{}{}
	got := <-observed
	if got.Argv[0] != "original" || got.Env["KEY"] != "value" || got.Cwd != "/tmp" || got.Timeout != time.Second || got.MaxOutputBytesPerStream != 32 {
		t.Fatalf("runner input: %#v", got)
	}
	_ = awaitTask(t, handler, snapshot.CommandID)
}

// TestBackgroundCaptureIsBounded verifies even an injected Runner ignoring write errors cannot grow results unbounded.
func TestBackgroundCaptureIsBounded(t *testing.T) {
	runner := func(_ context.Context, _ command.Request, _ io.Reader, stdout, stderr io.Writer) (command.Result, error) {
		_, _ = stdout.Write([]byte("abcdef"))
		_, _ = stderr.Write([]byte{255, 254, 253, 252})
		return command.Result{}, nil
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake"],"max_output_bytes_per_stream":3}`)
	result := awaitTask(t, handler, id)
	if result["status"] != "failed" || result["stdout_base64"] != "YWJj" || result["stderr_base64"] != "//79" || result["error"].(map[string]any)["code"] != "output_limit_exceeded" {
		t.Fatalf("bounded result: %v", result)
	}
}

// TestBackgroundShutdownAndAdmission verifies lifecycle cancellation and explicit unfinished-cleanup errors.
func TestBackgroundShutdownAndAdmission(t *testing.T) {
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return command.Result{}, ctx.Err()
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	<-started
	handler.BeginShutdown()
	<-canceled
	assertErrorResponse(t, taskRequest(handler, http.MethodPost, "/v1/commands", `{"argv":["fake"]}`), 503, "agent_stopping")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := handler.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("unfinished shutdown error = %v", err)
	}
	if got := taskRequest(handler, http.MethodGet, "/v1/commands/"+id, ""); !strings.Contains(got.Body.String(), `"status":"running"`) {
		t.Fatal("shutdown falsely published completion")
	}
	release <- struct{}{}
	if err := handler.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(handler.tasks.limiter.slots) != 0 {
		t.Fatal("shutdown left an execution slot held")
	}
	_ = awaitTask(t, handler, id)
}

// TestRunWaitsForDetachedTask proves http.Server.Shutdown alone cannot finish the Agent lifecycle.
func TestRunWaitsForDetachedTask(t *testing.T) {
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return command.Result{}, ctx.Err()
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- Run(ctx, listener, handler) }()
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	<-started
	cancel()
	<-canceled
	select {
	case err := <-finished:
		t.Fatalf("Run finished before Runner cleanup: %v", err)
	default:
	}
	release <- struct{}{}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not settle")
	}
	if awaitTask(t, handler, id)["status"] != "canceled" {
		t.Fatal("task not canceled on shutdown")
	}
}

// TestServerReportsBackgroundShutdownDeadline verifies a blocked Runner cannot be reported as cleaned.
func TestServerReportsBackgroundShutdownDeadline(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		close(started)
		<-release
		return command.Result{}, ctx.Err()
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	_ = submittedTask(t, handler, `{"argv":["fake"]}`)
	<-started
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runServer(ctx, listener, handler, time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown deadline error: %v", err)
	}
}
