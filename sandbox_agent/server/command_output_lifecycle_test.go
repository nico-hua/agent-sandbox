package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/sandbox_agent/command"
)

// TestCommandOutputValidation rejects ambiguous integers and malformed routes before any read succeeds.
func TestCommandOutputValidation(t *testing.T) {
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		<-ctx.Done()
		return command.Result{}, ctx.Err()
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	for _, key := range []string{"stdout_offset", "stderr_offset", "limit"} {
		for _, value := range []string{"", "-1", "-0", "+1", "%2B1", "%201", "1%20", "1.0", "1e2", "0x10", "true", "null", "9223372036854775808", "18446744073709551616", "%EF%BC%91"} {
			assertErrorResponse(t, taskRequest(handler, http.MethodGet, "/v1/commands/"+id+"/output?"+key+"="+value, ""), 400, "invalid_request")
		}
		assertErrorResponse(t, taskRequest(handler, http.MethodGet, "/v1/commands/"+id+"/output?"+key+"=1&"+key+"=1", ""), 400, "invalid_request")
	}
	for _, query := range []string{"?stdout_offset=1", "?stderr_offset=1", "?stdout_offset=9223372036854775807", "?limit=0", "?limit=65537", "?extra=1", "?stdout_offset=0;limit=1", "?stdout_offset=%zz"} {
		assertErrorResponse(t, taskRequest(handler, http.MethodGet, "/v1/commands/"+id+"/output"+query, ""), 400, "invalid_request")
	}
	for _, query := range []string{"", "?limit=1", "?limit=65536", "?stdout_offset=00&stderr_offset=0&limit=01"} {
		page := readTaskOutput(t, handler, id, query)
		if page.EOF || page.Status != "running" || page.Stdout != (observedStream{}) || page.Stderr != (observedStream{}) {
			t.Fatalf("empty running observation: %+v", page)
		}
	}
	assertErrorResponse(t, taskRequest(handler, http.MethodPost, "/v1/commands/"+id+"/output", ""), 405, "method_not_allowed")
	for _, path := range []string{"bad/output", "../etc/output", id + "/output/extra", id + "/output/", "cmd_" + strings.Repeat("a", 32) + "/output"} {
		assertErrorResponse(t, taskRequest(handler, http.MethodGet, "/v1/commands/"+path, ""), 404, "command_not_found")
	}
}

// TestCommandOutputSealedWriters rejects late writes so terminal lock-free encoding stays immutable.
func TestCommandOutputSealedWriters(t *testing.T) {
	writers := make(chan [2]io.Writer, 1)
	runner := func(_ context.Context, _ command.Request, _ io.Reader, stdout, stderr io.Writer) (command.Result, error) {
		_, _ = io.WriteString(stdout, "out")
		_, _ = io.WriteString(stderr, "err")
		writers <- [2]io.Writer{stdout, stderr}
		return command.Result{}, nil
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	streams := <-writers
	full := awaitTask(t, handler, id)
	before := readTaskOutput(t, handler, id, "")
	for _, stream := range streams {
		if n, err := stream.Write([]byte("late")); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("late write: %d %v", n, err)
		}
	}
	after := readTaskOutput(t, handler, id, "")
	if after != before || !after.EOF || after.Stdout.Data != full["stdout_base64"] || after.Stderr.Data != full["stderr_base64"] {
		t.Fatalf("terminal bytes mutated: %+v -> %+v", before, after)
	}
	if terminal := awaitTask(t, handler, id); terminal["stdout_base64"] != full["stdout_base64"] || terminal["stderr_base64"] != full["stderr_base64"] {
		t.Fatal("legacy terminal output mutated")
	}
}

// TestCommandOutputEmptyRunning checks querying accepted work before its first output never waits.
func TestCommandOutputEmptyRunning(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	runner := func(_ context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		close(entered)
		<-release
		return command.Result{}, nil
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	page := readTaskOutput(t, handler, id, "")
	if page.Status != "running" || page.EOF || page.Stdout.Available != 0 || page.Stderr.Available != 0 {
		t.Fatalf("pre-output page: %+v", page)
	}
	<-entered
}

// TestCommandOutputTerminalOutcomes preserves partial bytes for every Runner outcome and terminal cancellation.
func TestCommandOutputTerminalOutcomes(t *testing.T) {
	for _, test := range []struct {
		name, status string
		failure      error
	}{
		{"exit", "exited", nil},
		{"start", "failed", errors.New("private start error")},
		{"timeout", "failed", context.DeadlineExceeded},
		{"overflow", "failed", command.ErrOutputLimitExceeded},
		{"cancel", "canceled", context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			written := make(chan struct{})
			runner := func(ctx context.Context, _ command.Request, _ io.Reader, stdout, stderr io.Writer) (command.Result, error) {
				_, _ = stdout.Write([]byte{0xff, 0, 0xfe})
				_, _ = io.WriteString(stderr, "line without newline")
				close(written)
				if test.status == "canceled" {
					<-ctx.Done()
				}
				return command.Result{ExitCode: 9}, test.failure
			}
			handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
			id := submittedTask(t, handler, `{"argv":["fake"]}`)
			<-written
			if test.status == "canceled" {
				_ = taskRequest(handler, http.MethodPost, "/v1/commands/"+id+"/cancel", "")
			}
			full := awaitTask(t, handler, id)
			page := readTaskOutput(t, handler, id, "")
			if page.Status != test.status || !page.EOF || page.Stdout.Data != full["stdout_base64"] || page.Stderr.Data != full["stderr_base64"] {
				t.Fatalf("terminal mismatch: %+v, %v", page, full)
			}
			if end := readTaskOutput(t, handler, id, "?stdout_offset=3&stderr_offset=20"); !end.EOF || end.Stdout.Data != "" || end.Stderr.Data != "" {
				t.Fatalf("terminal end: %+v", end)
			}
		})
	}
}

// TestCommandOutputPaginationBounds catches incorrect defaults, maximum page size and stream cursor coupling.
func TestCommandOutputPaginationBounds(t *testing.T) {
	runner := func(_ context.Context, _ command.Request, _ io.Reader, stdout, _ io.Writer) (command.Result, error) {
		_, _ = io.WriteString(stdout, strings.Repeat("x", 70000))
		return command.Result{}, nil
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake"],"max_output_bytes_per_stream":70000}`)
	_ = awaitTask(t, handler, id)
	for _, test := range []struct {
		query string
		size  int
	}{{"", 16384}, {"?limit=1", 1}, {"?limit=65536", 65536}} {
		page := readTaskOutput(t, handler, id, test.query)
		data, err := base64.StdEncoding.DecodeString(page.Stdout.Data)
		if err != nil || len(data) != test.size || page.Stdout.Next != int64(test.size) || page.Stdout.Available != 70000 || !page.Stdout.More || page.EOF || page.Stderr != (observedStream{}) {
			t.Fatalf("bounded page: %+v (%v)", page, err)
		}
	}
}

// TestCommandOutputReadDoesNotRetainRecord checks polling never extends the original terminal retention deadline.
func TestCommandOutputReadDoesNotRetainRecord(t *testing.T) {
	clock := &taskClock{current: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)}
	runner := func(_ context.Context, _ command.Request, _ io.Reader, _, _ io.Writer) (command.Result, error) {
		return command.Result{}, nil
	}
	handler := taskTestHandler(t, runner, 1, taskPolicy{capacity: 1, retention: time.Minute, now: clock.now})
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	_ = awaitTask(t, handler, id)
	clock.advance(time.Minute - time.Nanosecond)
	if !readTaskOutput(t, handler, id, "").EOF {
		t.Fatal("empty terminal task is not EOF")
	}
	clock.advance(time.Nanosecond)
	assertErrorResponse(t, taskRequest(handler, http.MethodGet, "/v1/commands/"+id+"/output", ""), 404, "command_not_found")
}

// TestCommandOutputDisconnectDoesNotCancel leaves accepted work alive after failed or canceled polling responses.
func TestCommandOutputDisconnectDoesNotCancel(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	defer close(finish)
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, stdout, _ io.Writer) (command.Result, error) {
		_, _ = io.WriteString(stdout, "first")
		close(started)
		select {
		case <-finish:
			return command.Result{}, nil
		case <-ctx.Done():
			return command.Result{}, ctx.Err()
		}
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/v1/commands/"+id+"/output", nil).WithContext(ctx)
	cancel()
	handler.ServeHTTP(&lostTaskResponse{header: make(http.Header)}, request)
	if page := readTaskOutput(t, handler, id, ""); page.Status != "running" || page.Stdout.Data != "Zmlyc3Q=" {
		t.Fatalf("poll disconnected task: %+v", page)
	}
	finish <- struct{}{}
	if awaitTask(t, handler, id)["status"] != "exited" {
		t.Fatal("poll canceled execution")
	}
}

// TestCommandOutputConcurrentPublication coordinates reads in writing, canceling, terminal and expired phases.
func TestCommandOutputConcurrentPublication(t *testing.T) {
	clock := &taskClock{current: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)}
	start, written, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(finish)
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, stdout, stderr io.Writer) (command.Result, error) {
		<-start
		var writers sync.WaitGroup
		for _, stream := range []io.Writer{stdout, stderr} {
			writers.Add(1)
			go func() {
				defer writers.Done()
				for range 500 {
					_, _ = stream.Write([]byte{0xff})
				}
			}()
		}
		writers.Wait()
		close(written)
		<-ctx.Done()
		<-finish
		return command.Result{}, ctx.Err()
	}
	handler := taskTestHandler(t, runner, 1, taskPolicy{capacity: 1, retention: time.Minute, now: clock.now})
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	phases := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})}
	observed := make(chan struct{}, 8)
	readerContext, stopReaders := context.WithCancel(context.Background())
	defer stopReaders()
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for index, phase := range phases {
				select {
				case <-phase:
				case <-readerContext.Done():
					return
				}
				for range 20 {
					response := taskRequest(handler, http.MethodGet, "/v1/commands/"+id+"/output?limit=7", "")
					if index == 3 {
						if response.Code != 404 {
							t.Errorf("expired read: %s", response.Body.String())
						}
						continue
					}
					var page observedOutput
					if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &page) != nil {
						t.Errorf("read: %s", response.Body.String())
						continue
					}
					for _, stream := range []observedStream{page.Stdout, page.Stderr} {
						data, _ := base64.StdEncoding.DecodeString(stream.Data)
						if len(data) > 7 || stream.Next != int64(len(data)) || stream.Available < stream.Next || stream.More != (stream.Next < stream.Available) {
							t.Errorf("incoherent: %+v", page)
						}
					}
					if page.EOF || (index < 2 && page.Status != "running") || (index == 2 && page.Status != "canceled") {
						t.Errorf("phase %d state: %+v", index, page)
					}
					method, path := http.MethodGet, "/v1/commands/"+id
					if index == 2 {
						method, path = http.MethodPost, path+"/cancel"
					}
					legacy := taskRequest(handler, method, path, "")
					if legacy.Code != 200 {
						t.Errorf("legacy read: %s", legacy.Body.String())
					}
				}
				observed <- struct{}{}
			}
		}()
	}
	close(phases[0])
	close(start)
	for range 8 {
		<-observed
	}
	<-written
	_ = taskRequest(handler, http.MethodPost, "/v1/commands/"+id+"/cancel", "")
	close(phases[1])
	for range 8 {
		<-observed
	}
	finish <- struct{}{}
	if full := awaitTask(t, handler, id); full["status"] != "canceled" {
		t.Fatalf("terminal: %v", full)
	}
	close(phases[2])
	for range 8 {
		<-observed
	}
	clock.advance(time.Minute)
	close(phases[3])
	readers.Wait()
}
