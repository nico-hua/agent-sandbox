package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/agent/internal/command"
)

// TestCommandLimiterReusesReleasedCapacity verifies a released single slot can be acquired again.
func TestCommandLimiterReusesReleasedCapacity(t *testing.T) {
	limiter, err := newCommandLimiter(1)
	if err != nil {
		t.Fatalf("newCommandLimiter() error = %v, want nil", err)
	}
	if !limiter.tryAcquire() {
		t.Fatal("first acquisition failed")
	}
	if limiter.tryAcquire() {
		t.Fatal("second acquisition succeeded before release")
	}
	limiter.release()
	if !limiter.tryAcquire() {
		t.Fatal("acquisition failed after release")
	}
	limiter.release()
}

// TestCommandLimiterAllowsConfiguredCapacity verifies each configured slot can be held concurrently.
func TestCommandLimiterAllowsConfiguredCapacity(t *testing.T) {
	limiter, err := newCommandLimiter(2)
	if err != nil {
		t.Fatalf("newCommandLimiter() error = %v, want nil", err)
	}
	if !limiter.tryAcquire() || !limiter.tryAcquire() {
		t.Fatal("failed to acquire both configured slots")
	}
	if limiter.tryAcquire() {
		t.Fatal("acquisition succeeded beyond configured capacity")
	}
	limiter.release()
	limiter.release()
}

// TestCommandLimiterRejectsInvalidLimits verifies construction enforces the service bounds.
func TestCommandLimiterRejectsInvalidLimits(t *testing.T) {
	for _, limit := range []int{-1, 0, 1025} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			if _, err := newCommandLimiter(limit); err == nil {
				t.Fatalf("newCommandLimiter(%d) error = nil, want non-nil", limit)
			}
		})
	}
}

// TestCommandLimiterAcceptsBoundaryLimits verifies both legal boundary values are accepted.
func TestCommandLimiterAcceptsBoundaryLimits(t *testing.T) {
	for _, limit := range []int{1, 1024} {
		if _, err := newCommandLimiter(limit); err != nil {
			t.Errorf("newCommandLimiter(%d) error = %v, want nil", limit, err)
		}
	}
}

type blockingCommandRunner struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   int
	current int
	maximum int
}

// newBlockingCommandRunner creates a runner controlled by channels for deterministic concurrency tests.
func newBlockingCommandRunner() *blockingCommandRunner {
	return &blockingCommandRunner{
		started: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
}

// Run records concurrent entry, announces startup, and blocks until the test releases it.
func (runner *blockingCommandRunner) Run(
	_ context.Context,
	_ command.Request,
	_ io.Reader,
	_ io.Writer,
	_ io.Writer,
) (command.Result, error) {
	runner.mu.Lock()
	runner.calls++
	runner.current++
	if runner.current > runner.maximum {
		runner.maximum = runner.current
	}
	runner.mu.Unlock()

	runner.started <- struct{}{}
	<-runner.release

	runner.mu.Lock()
	runner.current--
	runner.mu.Unlock()
	return command.Result{ExitCode: 0}, nil
}

// snapshot returns concurrency counters without racing with active runner calls.
func (runner *blockingCommandRunner) snapshot() (calls int, current int, maximum int) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.calls, runner.current, runner.maximum
}

// TestCommandHandlerRejectsExcessConcurrency verifies capacity is shared, non-blocking, and reusable.
func TestCommandHandlerRejectsExcessConcurrency(t *testing.T) {
	runner := newBlockingCommandRunner()
	handler := newTestHandler(t, runner.Run, 1)
	firstResponse := serveCommandRequestAsync(handler, `{"argv":["first"]}`)
	waitForRunnerStart(t, runner.started)

	secondResponse := waitForHTTPResponse(t, serveCommandRequestAsync(handler, `{"argv":["second"]}`))
	assertErrorResponse(t, secondResponse, http.StatusTooManyRequests, "command_capacity_exceeded")
	if calls, _, _ := runner.snapshot(); calls != 1 {
		t.Errorf("runner calls = %d, want 1", calls)
	}

	healthRequest := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthResponse := httptest.NewRecorder()
	handler.ServeHTTP(healthResponse, healthRequest)
	if healthResponse.Code != http.StatusOK {
		t.Errorf("health status = %d, want %d", healthResponse.Code, http.StatusOK)
	}

	close(runner.release)
	if response := waitForHTTPResponse(t, firstResponse); response.Code != http.StatusOK {
		t.Errorf("first status = %d, want %d", response.Code, http.StatusOK)
	}
	afterRelease := waitForHTTPResponse(t, serveCommandRequestAsync(handler, `{"argv":["after-release"]}`))
	if afterRelease.Code != http.StatusOK {
		t.Errorf("status after release = %d, want %d", afterRelease.Code, http.StatusOK)
	}
}

// TestCommandHandlerAllowsConfiguredConcurrency verifies the runner never exceeds a limit greater than one.
func TestCommandHandlerAllowsConfiguredConcurrency(t *testing.T) {
	runner := newBlockingCommandRunner()
	handler := newTestHandler(t, runner.Run, 2)
	firstResponse := serveCommandRequestAsync(handler, `{"argv":["first"]}`)
	secondResponse := serveCommandRequestAsync(handler, `{"argv":["second"]}`)
	waitForRunnerStart(t, runner.started)
	waitForRunnerStart(t, runner.started)

	thirdResponse := waitForHTTPResponse(t, serveCommandRequestAsync(handler, `{"argv":["third"]}`))
	assertErrorResponse(t, thirdResponse, http.StatusTooManyRequests, "command_capacity_exceeded")
	if calls, current, maximum := runner.snapshot(); calls != 2 || current != 2 || maximum != 2 {
		t.Errorf("runner counters = calls %d current %d maximum %d, want 2, 2, 2", calls, current, maximum)
	}

	close(runner.release)
	for _, responseChannel := range []<-chan *httptest.ResponseRecorder{firstResponse, secondResponse} {
		if response := waitForHTTPResponse(t, responseChannel); response.Code != http.StatusOK {
			t.Errorf("admitted status = %d, want %d", response.Code, http.StatusOK)
		}
	}
	if _, current, maximum := runner.snapshot(); current != 0 || maximum > 2 {
		t.Errorf("final concurrency = current %d maximum %d, want current 0 and maximum <= 2", current, maximum)
	}
}

// TestCommandHandlerValidatesBeforeCapacity verifies invalid requests do not compete for execution slots.
func TestCommandHandlerValidatesBeforeCapacity(t *testing.T) {
	runner := newBlockingCommandRunner()
	handler := newTestHandler(t, runner.Run, 1)
	runningResponse := serveCommandRequestAsync(handler, `{"argv":["running"]}`)
	waitForRunnerStart(t, runner.started)

	for _, body := range []string{`{"argv":[}`, `{"argv":[]}`} {
		response := waitForHTTPResponse(t, serveCommandRequestAsync(handler, body))
		assertErrorResponse(t, response, http.StatusBadRequest, "invalid_request")
	}

	close(runner.release)
	waitForHTTPResponse(t, runningResponse)
}

// TestCommandHandlerReleasesCapacityForRunnerOutcomes verifies every normal error path returns its slot.
func TestCommandHandlerReleasesCapacityForRunnerOutcomes(t *testing.T) {
	tests := []struct {
		name       string
		result     command.Result
		err        error
		statusCode int
	}{
		{name: "success", result: command.Result{ExitCode: 0}, statusCode: http.StatusOK},
		{name: "non-zero exit", result: command.Result{ExitCode: 7}, statusCode: http.StatusOK},
		{name: "start failure", err: errors.New("start failed"), statusCode: http.StatusUnprocessableEntity},
		{name: "timeout", err: context.DeadlineExceeded, statusCode: http.StatusGatewayTimeout},
		{name: "output limit", err: command.ErrOutputLimitExceeded, statusCode: http.StatusRequestEntityTooLarge},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			runner := func(
				context.Context,
				command.Request,
				io.Reader,
				io.Writer,
				io.Writer,
			) (command.Result, error) {
				calls.Add(1)
				return test.result, test.err
			}
			handler := newTestHandler(t, runner, 1)

			for range 2 {
				response := serveCommandRequest(handler, `{"argv":["run"]}`)
				if response.Code != test.statusCode {
					t.Errorf("status = %d, want %d", response.Code, test.statusCode)
				}
			}
			if calls.Load() != 2 {
				t.Errorf("runner calls = %d, want 2", calls.Load())
			}
		})
	}
}

// TestCommandHandlerReleasesCapacityAfterCancellation verifies canceled execution returns its slot.
func TestCommandHandlerReleasesCapacityAfterCancellation(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	runner := func(
		context.Context,
		command.Request,
		io.Reader,
		io.Writer,
		io.Writer,
	) (command.Result, error) {
		calls.Add(1)
		cancel()
		return command.Result{ExitCode: -1}, context.Canceled
	}
	handler := newTestHandler(t, runner, 1)
	request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(`{"argv":["run"]}`)).WithContext(ctx)
	handler.ServeHTTP(httptest.NewRecorder(), request)

	serveCommandRequest(handler, `{"argv":["after-cancel"]}`)
	if calls.Load() != 2 {
		t.Errorf("runner calls = %d, want 2", calls.Load())
	}
}

// TestCommandHandlerReleasesCapacityAfterPanic verifies deferred release runs during panic unwinding.
func TestCommandHandlerReleasesCapacityAfterPanic(t *testing.T) {
	var calls atomic.Int32
	runner := func(
		context.Context,
		command.Request,
		io.Reader,
		io.Writer,
		io.Writer,
	) (command.Result, error) {
		if calls.Add(1) == 1 {
			panic("runner panic")
		}
		return command.Result{ExitCode: 0}, nil
	}
	handler := newTestHandler(t, runner, 1)

	func() {
		defer func() {
			_ = recover()
		}()
		serveCommandRequest(handler, `{"argv":["panic"]}`)
	}()

	response := serveCommandRequest(handler, `{"argv":["after-panic"]}`)
	if response.Code != http.StatusOK || calls.Load() != 2 {
		t.Errorf("status = %d calls = %d, want status %d calls 2", response.Code, calls.Load(), http.StatusOK)
	}
}

// serveCommandRequest sends one valid command request through an in-memory handler.
func serveCommandRequest(handler http.Handler, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/commands:run", strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

// serveCommandRequestAsync starts one in-memory request and returns its eventual response channel.
func serveCommandRequestAsync(handler http.Handler, body string) <-chan *httptest.ResponseRecorder {
	responses := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		responses <- serveCommandRequest(handler, body)
	}()
	return responses
}

// waitForRunnerStart waits for deterministic runner entry with a deadlock guard.
func waitForRunnerStart(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not start")
	}
}

// waitForHTTPResponse waits for a handler response with a deadlock guard.
func waitForHTTPResponse(t *testing.T, responses <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case response := <-responses:
		return response
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return")
		return nil
	}
}
