package server

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nico-hua/agent-sandbox/sandbox_agent/command"
)

// containsAll checks independent public snapshot fields without depending on JSON field order.
func containsAll(body string, fields ...string) bool {
	for _, field := range fields {
		if !strings.Contains(body, field) {
			return false
		}
	}
	return true
}

// TestCommandTimeoutByMode keeps foreground limits while allowing explicitly longer background work.
func TestCommandTimeoutByMode(t *testing.T) {
	for _, mode := range []string{"/v1/commands", "/v1/commands:run", "/v1/commands:stream"} {
		for _, test := range []struct {
			name, field  string
			milliseconds int64
		}{
			{"omitted", "", 30000},
			{"null", `,"timeout_ms":null`, 30000},
			{"minimum", `,"timeout_ms":1`, 1},
			{"foreground-maximum", `,"timeout_ms":30000`, 30000},
			{"above-foreground", `,"timeout_ms":30001`, 30001},
			{"long", `,"timeout_ms":600000`, 600000},
			{"one-day", `,"timeout_ms":86400000`, 86400000},
			{"zero", `,"timeout_ms":0`, 0},
			{"negative", `,"timeout_ms":-1`, 0},
			{"above-day", `,"timeout_ms":86400001`, 0},
			{"duration-overflow", `,"timeout_ms":9223372036854775807`, 0},
			{"integer-overflow", `,"timeout_ms":9223372036854775808`, 0},
			{"fraction", `,"timeout_ms":1.5`, 0},
			{"float", `,"timeout_ms":30001.0`, 0},
			{"string", `,"timeout_ms":"30001"`, 0},
			{"boolean", `,"timeout_ms":true`, 0},
			{"array", `,"timeout_ms":[]`, 0},
			{"object", `,"timeout_ms":{}`, 0},
		} {
			t.Run(mode+"/"+test.name, func(t *testing.T) {
				timeoutMS := test.milliseconds
				if mode == "/v1/commands" && (test.name == "omitted" || test.name == "null") {
					timeoutMS = 300000
				}
				var calls atomic.Int64
				observed := make(chan time.Duration, 1)
				runner := func(_ context.Context, request command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
					calls.Add(1)
					observed <- request.Timeout
					return command.Result{}, nil
				}
				handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
				body := `{"argv":["fake"]` + test.field + `}`
				valid := test.milliseconds > 0 && (mode == "/v1/commands" || test.milliseconds <= 30000)
				if !valid {
					assertErrorResponse(t, taskRequest(handler, http.MethodPost, mode, body), 400, "invalid_request")
					if calls.Load() != 0 || len(handler.tasks.tasks) != 0 || len(handler.tasks.limiter.slots) != 0 {
						t.Fatal("invalid timeout registered or executed work")
					}
					return
				}
				if mode == "/v1/commands" {
					id := submittedTask(t, handler, body)
					if result := awaitTask(t, handler, id); result["timeout_ms"] != float64(timeoutMS) {
						t.Fatalf("effective terminal timeout: %v", result)
					}
				} else if response := taskRequest(handler, http.MethodPost, mode, body); response.Code != 200 {
					t.Fatalf("foreground rejected valid timeout: %d", response.Code)
				}
				if got := <-observed; got != time.Duration(timeoutMS)*time.Millisecond {
					t.Fatalf("Runner timeout = %v, want %dms", got, timeoutMS)
				}
			})
		}
	}
}

// TestBackgroundDeadlineStartsAtAcceptance exercises the manager deadline without a real-time wait.
func TestBackgroundDeadlineStartsAtAcceptance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		acceptedAt := time.Now()
		runner := func(ctx context.Context, request command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
			// Simulate setup before a program starts; the accepted budget must already be counting down.
			time.Sleep(2 * time.Second)
			deadline, exists := ctx.Deadline()
			if !exists || !deadline.Equal(acceptedAt.Add(5*time.Minute)) || request.Timeout != 5*time.Minute {
				t.Errorf("missing acceptance deadline: %v, exists=%v, timeout=%v", deadline, exists, request.Timeout)
			}
			<-ctx.Done()
			return command.Result{}, ctx.Err()
		}
		handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
		id := submittedTask(t, handler, `{"argv":["fake"]}`)
		synctest.Wait()
		time.Sleep(5 * time.Minute)
		synctest.Wait()
		response := taskRequest(handler, http.MethodGet, "/v1/commands/"+id, "")
		if body := response.Body.String(); !containsAll(body, `"status":"failed"`, `"code":"command_timeout"`, `"timeout_ms":300000`) {
			t.Fatalf("manager deadline did not settle task: %s", body)
		}
		if len(handler.tasks.limiter.slots) != 0 {
			t.Fatal("timeout did not release execution capacity after cleanup")
		}
	})
}

// TestBackgroundLongDeadlineAndSnapshots preserves one accepted budget through query, cancel and completion.
func TestBackgroundLongDeadlineAndSnapshots(t *testing.T) {
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	defer close(release)
	runner := func(ctx context.Context, request command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		if request.Timeout != 24*time.Hour {
			t.Errorf("Runner timeout: %v", request.Timeout)
		}
		started <- ctx
		<-ctx.Done()
		<-release
		return command.Result{}, ctx.Err()
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	before := time.Now()
	id := submittedTask(t, handler, `{"argv":["fake"],"timeout_ms":86400000}`)
	after := time.Now()
	ctx := <-started
	deadline, exists := ctx.Deadline()
	if !exists || deadline.Before(before.Add(24*time.Hour)) || deadline.After(after.Add(24*time.Hour)) {
		t.Fatalf("deadline not fixed during acceptance: %v, exists=%v", deadline, exists)
	}
	for _, path := range []string{"/v1/commands", "/v1/commands:run", "/v1/commands:stream"} {
		assertErrorResponse(t, taskRequest(handler, http.MethodPost, path, `{"argv":["full"]}`), 429, "command_capacity_exceeded")
	}
	for range 2 {
		query := taskRequest(handler, http.MethodGet, "/v1/commands/"+id, "")
		cancel := taskRequest(handler, http.MethodPost, "/v1/commands/"+id+"/cancel", "")
		if query.Code != 200 || cancel.Code != 202 || !containsAll(query.Body.String(), `"timeout_ms":86400000`, `"status":"running"`) || !containsAll(cancel.Body.String(), `"timeout_ms":86400000`, `"status":"running"`) {
			t.Fatalf("query/cancel snapshots lost timeout: %s / %s", query.Body.String(), cancel.Body.String())
		}
		assertErrorResponse(t, taskRequest(handler, http.MethodPost, "/v1/commands", `{"argv":["still-full"]}`), 429, "command_capacity_exceeded")
	}
	release <- struct{}{}
	terminal := awaitTask(t, handler, id)
	if terminal["timeout_ms"] != float64(86400000) || terminal["status"] != "canceled" {
		t.Fatalf("terminal budget changed: %v", terminal)
	}
	cancel := taskRequest(handler, http.MethodPost, "/v1/commands/"+id+"/cancel", "")
	if cancel.Code != 200 || !containsAll(cancel.Body.String(), `"timeout_ms":86400000`, `"status":"canceled"`) {
		t.Fatalf("terminal cancel snapshot: %s", cancel.Body.String())
	}
}

// TestBackgroundRealTimeoutReleasesCapacity verifies the real Runner finishes before a slot is reused.
func TestBackgroundRealTimeoutReleasesCapacity(t *testing.T) {
	handler := taskTestHandler(t, command.Run, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["sleep","1"],"timeout_ms":20}`)
	result := awaitTask(t, handler, id)
	if result["status"] != "failed" || result["timeout_ms"] != float64(20) || result["error"].(map[string]any)["code"] != "command_timeout" {
		t.Fatalf("real timeout: %v", result)
	}
	handler.tasks.workers.Wait()
	next := submittedTask(t, handler, `{"argv":["true"]}`)
	if result := awaitTask(t, handler, next); result["status"] != "exited" {
		t.Fatalf("execution capacity not reusable: %v", result)
	}
}
