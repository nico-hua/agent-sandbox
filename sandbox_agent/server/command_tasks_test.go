package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/sandbox_agent/command"
)

// taskRequest exercises one background route without requiring an external listener.
func taskRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)))
	return response
}

// submittedTask extracts an accepted command ID, failing before a test can use a bad response.
func submittedTask(t *testing.T, handler http.Handler, body string) string {
	t.Helper()
	response := taskRequest(handler, http.MethodPost, "/v1/commands", body)
	if response.Code != http.StatusAccepted {
		t.Fatalf("submit: %d %s", response.Code, response.Body.String())
	}
	var accepted struct {
		CommandID string `json:"command_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil || !strings.HasPrefix(accepted.CommandID, "cmd_") || len(accepted.CommandID) != 36 {
		t.Fatalf("invalid accepted response: %s (%v)", response.Body.String(), err)
	}
	return accepted.CommandID
}

// awaitTask polls the actual terminal condition with a deadline, without guessing execution order.
func awaitTask(t *testing.T, handler http.Handler, id string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		response := taskRequest(handler, http.MethodGet, "/v1/commands/"+id, "")
		var snapshot map[string]any
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &snapshot) != nil {
			t.Fatalf("query: %d %s", response.Code, response.Body.String())
		}
		if snapshot["status"] != "running" {
			return snapshot
		}
		runtime.Gosched()
	}
	t.Fatal("task did not reach terminal state")
	return nil
}

// TestBackgroundAcceptanceOutlivesRequest verifies returning 202 and canceling its request leaves work alive.
func TestBackgroundAcceptanceOutlivesRequest(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, stdout, _ io.Writer) (command.Result, error) {
		_, _ = stdout.Write([]byte{0})
		close(started)
		select {
		case <-ctx.Done():
			return command.Result{}, ctx.Err()
		case <-release:
			_, _ = stdout.Write([]byte{255, 1})
			return command.Result{ExitCode: 7}, nil
		}
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequest(http.MethodPost, "/v1/commands", strings.NewReader(`{"argv":["fake"]}`)).WithContext(requestCtx)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("submit: %d %s", response.Code, response.Body.String())
	}
	var accepted map[string]string
	_ = json.Unmarshal(response.Body.Bytes(), &accepted)
	cancel()
	<-started
	running := taskRequest(handler, http.MethodGet, "/v1/commands/"+accepted["command_id"], "")
	if strings.Contains(running.Body.String(), "stdout_base64") || !strings.Contains(running.Body.String(), `"status":"running"`) {
		t.Fatalf("running snapshot: %s", running.Body.String())
	}
	// Signal a natural completion even though the submission context is already canceled.
	release <- struct{}{}
	terminal := awaitTask(t, handler, accepted["command_id"])
	if terminal["status"] != "exited" || terminal["exit_code"] != float64(7) || terminal["stdout_base64"] != "AP8B" || terminal["stderr_base64"] != "" {
		t.Fatalf("terminal: %v", terminal)
	}
}

type lostTaskResponse struct {
	header http.Header
	status int
}

// Header exposes response headers while simulating a disconnected submission client.
func (response *lostTaskResponse) Header() http.Header { return response.header }

// WriteHeader records acceptance even though the client will never receive its body.
func (response *lostTaskResponse) WriteHeader(status int) { response.status = status }

// Write fails transport delivery without canceling an already registered command.
func (response *lostTaskResponse) Write(_ []byte) (int, error) {
	return 0, io.ErrClosedPipe
}

// TestBackgroundLostAcceptanceResponse verifies response failure cannot undo accepted work.
func TestBackgroundLostAcceptanceResponse(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, stdout, _ io.Writer) (command.Result, error) {
		close(started)
		select {
		case <-release:
			_, _ = io.WriteString(stdout, "accepted")
			return command.Result{}, nil
		case <-ctx.Done():
			return command.Result{}, ctx.Err()
		}
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequest(http.MethodPost, "/v1/commands", strings.NewReader(`{"argv":["fake"]}`)).WithContext(ctx)
	response := &lostTaskResponse{header: make(http.Header)}
	handler.ServeHTTP(response, request)
	cancel()
	<-started
	handler.tasks.mu.Lock()
	var id string
	for registered := range handler.tasks.tasks {
		id = registered
	}
	handler.tasks.mu.Unlock()
	if response.status != http.StatusAccepted || id == "" {
		t.Fatal("task was not accepted before response loss")
	}
	release <- struct{}{}
	if result := awaitTask(t, handler, id); result["status"] != "exited" || result["stdout_base64"] != "YWNjZXB0ZWQ=" {
		t.Fatalf("response failure changed accepted work: %v", result)
	}
}

// TestBackgroundHTTPPassesExecutionFields verifies the shared decoder forwards policy and input unchanged.
func TestBackgroundHTTPPassesExecutionFields(t *testing.T) {
	runner := &recordingCommandRunner{result: command.Result{ExitCode: 0}}
	handler := taskTestHandler(t, runner.Run, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake","argument"],"cwd":"/tmp","env":{"KEY":"value"},"stdin":"input","timeout_ms":29,"max_output_bytes_per_stream":5}`)
	_ = awaitTask(t, handler, id)
	if runner.stdinText != "input" || runner.request.Cwd != "/tmp" || runner.request.Env["KEY"] != "value" || runner.request.Timeout != 29*time.Millisecond || runner.request.MaxOutputBytesPerStream != 5 || runner.request.Argv[1] != "argument" {
		t.Fatalf("request forwarding: %#v", runner.request)
	}
}

// TestBackgroundRealRunner verifies real binary output, nonzero exit, startup failure, timeout and bounded overflow.
func TestBackgroundRealRunner(t *testing.T) {
	for _, test := range []struct {
		name, body, status, output, code string
		exit                             float64
	}{
		{"binary", `{"argv":["sh","-c","printf '\\377\\000'; exit 7"]}`, "exited", "/wA=", "", 7},
		{"start", `{"argv":["/definitely-missing-background-program"]}`, "failed", "", "command_start_failed", 0},
		{"timeout", `{"argv":["sleep","1"],"timeout_ms":1}`, "failed", "", "command_timeout", 0},
		{"overflow", `{"argv":["sh","-c","printf abcdef"],"max_output_bytes_per_stream":3}`, "failed", "YWJj", "output_limit_exceeded", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := taskTestHandler(t, command.Run, 1, defaultTaskPolicy())
			id := submittedTask(t, handler, test.body)
			result := awaitTask(t, handler, id)
			if result["status"] != test.status || result["stdout_base64"] != test.output {
				t.Fatalf("result: %v", result)
			}
			if test.status == "exited" && result["exit_code"] != test.exit {
				t.Fatalf("exit code: %v", result)
			}
			if test.code != "" && result["error"].(map[string]any)["code"] != test.code {
				t.Fatalf("error: %v", result)
			}
		})
	}
}

// TestBackgroundErrorsAreStable verifies failures retain bounded binary output without leaking diagnostics.
func TestBackgroundErrorsAreStable(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code string
	}{
		{"start", errors.New("private command and credentials"), "command_start_failed"},
		{"timeout", context.DeadlineExceeded, "command_timeout"},
		{"output", command.ErrOutputLimitExceeded, "output_limit_exceeded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := func(_ context.Context, _ command.Request, _ io.Reader, stdout, stderr io.Writer) (command.Result, error) {
				_, _ = stdout.Write([]byte{0xff, 0x00})
				_, _ = stderr.Write([]byte{0xfe})
				return command.Result{}, test.err
			}
			handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
			id := submittedTask(t, handler, `{"argv":["fake"]}`)
			result := awaitTask(t, handler, id)
			public := result["error"].(map[string]any)
			if result["status"] != "failed" || public["code"] != test.code || result["stdout_base64"] != base64.StdEncoding.EncodeToString([]byte{255, 0}) {
				t.Fatalf("result: %v", result)
			}
			if _, ok := result["exit_code"]; ok {
				t.Fatal("failed task contains exit_code")
			}
			if strings.Contains(public["message"].(string), "private") {
				t.Fatal("leaked runner diagnostic")
			}
		})
	}
}

// TestBackgroundConcurrentQueries verifies accepted work exposes race-safe snapshots and immutable terminal results.
func TestBackgroundConcurrentQueries(t *testing.T) {
	runner := func(_ context.Context, _ command.Request, _ io.Reader, stdout, _ io.Writer) (command.Result, error) {
		_, _ = io.WriteString(stdout, "bounded")
		return command.Result{}, nil
	}
	handler := taskTestHandler(t, runner, 4, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	terminal := awaitTask(t, handler, id)
	var workers sync.WaitGroup
	for range 20 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			response := taskRequest(handler, http.MethodPost, "/v1/commands/"+id+"/cancel", "")
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"exited"`) {
				t.Errorf("late cancel: %s", response.Body.String())
			}
		}()
	}
	workers.Wait()
	if got := awaitTask(t, handler, id); got["stdout_base64"] != terminal["stdout_base64"] {
		t.Fatal("terminal output changed")
	}
}
