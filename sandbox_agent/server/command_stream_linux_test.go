package server

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/sandbox_agent/command"
)

// TestCommandStreamDisconnectKillsProcessTree verifies HTTP cancellation reaches the real process group.
func TestCommandStreamDisconnectKillsProcessTree(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t, command.Run, 1))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	script := "sleep 30 >/dev/null 2>&1 & child=$!; printf '%s %s\\n' \"$$\" \"$child\"; wait"
	payload, err := json.Marshal(map[string]any{"argv": []string{"sh", "-c", script}})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/commands:stream", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	var output strings.Builder
	reader := bufio.NewReader(response.Body)
	for !strings.Contains(output.String(), "\n") {
		frame := readSSEFrame(t, reader)
		if !strings.HasPrefix(frame, "event: stdout\ndata: ") {
			t.Fatalf("expected PID output frame, got %q", frame)
		}
		var data streamOutputData
		if err := json.Unmarshal([]byte(strings.TrimSuffix(strings.TrimPrefix(frame, "event: stdout\ndata: "), "\n\n")), &data); err != nil {
			t.Fatal(err)
		}
		chunk, err := base64.StdEncoding.DecodeString(data.DataBase64)
		if err != nil {
			t.Fatal(err)
		}
		output.Write(chunk)
	}
	fields := strings.Fields(output.String())
	if len(fields) != 2 {
		t.Fatalf("PID notification = %q, want parent and child", output.String())
	}
	parentPID, err := strconv.Atoi(fields[0])
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = syscall.Kill(parentPID, syscall.SIGKILL)
		_ = syscall.Kill(childPID, syscall.SIGKILL)
	}()

	cancel()
	_ = response.Body.Close()
	waitForStreamProcessExit(t, parentPID)
	waitForStreamProcessExit(t, childPID)
}

// waitForStreamProcessExit polls a known child PID until it is reaped or a test deadline expires.
func waitForStreamProcessExit(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if err != nil {
			t.Fatalf("check process %d: %v", pid, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d still exists after stream disconnect", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
