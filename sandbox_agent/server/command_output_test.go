package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/nico-hua/agent-sandbox/sandbox_agent/command"
)

type observedStream struct {
	Data      string `json:"data_base64"`
	Next      int64  `json:"next_offset"`
	Available int64  `json:"available_bytes"`
	More      bool   `json:"has_more"`
}

type observedOutput struct {
	ID     string         `json:"command_id"`
	Status string         `json:"status"`
	EOF    bool           `json:"eof"`
	Stdout observedStream `json:"stdout"`
	Stderr observedStream `json:"stderr"`
}

// readTaskOutput checks the public polling response and its cache boundary.
func readTaskOutput(t *testing.T, handler http.Handler, id, query string) observedOutput {
	t.Helper()
	response := taskRequest(handler, http.MethodGet, "/v1/commands/"+id+"/output"+query, "")
	if response.Code != 200 || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("output response: %d %s", response.Code, response.Body.String())
	}
	var result observedOutput
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.ID != id {
		t.Fatalf("output decode: %s (%v)", response.Body.String(), err)
	}
	return result
}

// TestCommandOutputStages catches lost byte offsets, premature EOF and accidental cursor consumption.
func TestCommandOutputStages(t *testing.T) {
	first, proceed := make(chan struct{}), make(chan struct{})
	defer close(proceed)
	runner := func(ctx context.Context, _ command.Request, _ io.Reader, stdout, stderr io.Writer) (command.Result, error) {
		_, _ = stdout.Write([]byte{0xff, 0xe4, 0xb8, 0xad})
		_, _ = io.WriteString(stderr, "err")
		close(first)
		select {
		case <-proceed:
			_, _ = stdout.Write([]byte{0, '\n'})
			return command.Result{ExitCode: 7}, nil
		case <-ctx.Done():
			return command.Result{}, ctx.Err()
		}
	}
	handler := taskTestHandler(t, runner, 1, defaultTaskPolicy())
	id := submittedTask(t, handler, `{"argv":["fake"]}`)
	<-first
	page := readTaskOutput(t, handler, id, "?limit=2")
	if page.Status != "running" || page.EOF || page.Stdout != (observedStream{"/+Q=", 2, 4, true}) || page.Stderr != (observedStream{"ZXI=", 2, 3, true}) {
		t.Fatalf("first page: %+v", page)
	}
	if again := readTaskOutput(t, handler, id, "?limit=2"); again != page {
		t.Fatal("read consumed a shared cursor")
	}
	end := readTaskOutput(t, handler, id, "?stdout_offset=4&stderr_offset=3")
	if end.EOF || end.Stdout.Data != "" || end.Stderr.Data != "" || end.Stdout.More {
		t.Fatalf("running end: %+v", end)
	}
	assertErrorResponse(t, taskRequest(handler, http.MethodPost, "/v1/commands", `{"argv":["blocked"]}`), 429, "command_capacity_exceeded")
	proceed <- struct{}{}
	terminal := awaitTask(t, handler, id)
	partial := readTaskOutput(t, handler, id, "?limit=2")
	if partial.EOF || partial.Status != "exited" || partial.Stdout.Available != 6 {
		t.Fatalf("terminal partial: %+v", partial)
	}
	last := readTaskOutput(t, handler, id, "?stdout_offset=2&stderr_offset=2")
	if !last.EOF || last.Stdout != (observedStream{"uK0ACg==", 6, 6, false}) || last.Stderr != (observedStream{"cg==", 3, 3, false}) {
		t.Fatalf("last page: %+v", last)
	}
	all := readTaskOutput(t, handler, id, "")
	if !all.EOF || all.Stdout.Data != terminal["stdout_base64"] || all.Stderr.Data != terminal["stderr_base64"] {
		t.Fatalf("complete compatibility: %+v, %v", all, terminal)
	}
	lastCancel := taskRequest(handler, http.MethodPost, "/v1/commands/"+id+"/cancel", "")
	if lastCancel.Code != 200 || !strings.Contains(lastCancel.Body.String(), all.Stdout.Data) {
		t.Fatalf("terminal cancellation: %s", lastCancel.Body.String())
	}
}
