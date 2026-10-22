package server

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"sync"

	"github.com/nico-hua/agent-sandbox/sandbox_agent/command"
)

const (
	defaultOutputPageSize = 16384
	maximumOutputPageSize = 65536
)

var errInvalidOutputRequest = errors.New("invalid output request")

type outputStreamPage struct {
	DataBase64     string `json:"data_base64"`
	NextOffset     int64  `json:"next_offset"`
	AvailableBytes int64  `json:"available_bytes"`
	HasMore        bool   `json:"has_more"`
}

type commandOutputPage struct {
	CommandID string           `json:"command_id"`
	Status    string           `json:"status"`
	EOF       bool             `json:"eof"`
	Stdout    outputStreamPage `json:"stdout"`
	Stderr    outputStreamPage `json:"stderr"`
}

type taskOutput struct {
	mu       sync.Mutex
	data     []byte
	limit    int64
	exceeded bool
	sealed   bool
	cancel   context.CancelFunc
}

// Write appends bounded bytes; sealing at terminal publication prevents later mutation.
func (output *taskOutput) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	if output.sealed {
		return 0, io.ErrClosedPipe
	}
	allowed := min(int64(len(data)), output.limit-int64(len(output.data)))
	output.data = append(output.data, data[:allowed]...)
	if allowed < int64(len(data)) {
		output.exceeded = true
		output.cancel()
		return int(allowed), command.ErrOutputLimitExceeded
	}
	return int(allowed), nil
}

// encodeResult preserves legacy terminal fields without retaining a complete Base64 copy.
func (task *commandTask) encodeResult(snapshot taskSnapshot) taskSnapshot {
	if snapshot.Status != "running" {
		// Publication under manager.mu happens after both streams are sealed.
		// These bytes remain immutable even if the registry expires this task.
		out := base64.StdEncoding.EncodeToString(task.stdout.data)
		errout := base64.StdEncoding.EncodeToString(task.stderr.data)
		snapshot.StdoutBase64, snapshot.StderrBase64 = &out, &errout
	}
	return snapshot
}

// readOutput encodes only the selected bytes after releasing all observation locks.
func (manager *commandManager) readOutput(id string, stdoutOffset, stderrOffset, limit int64) (commandOutputPage, error) {
	page, out, errout, err := manager.captureOutput(id, stdoutOffset, stderrOffset, limit)
	if err != nil {
		return commandOutputPage{}, err
	}
	page.Stdout.DataBase64 = base64.StdEncoding.EncodeToString(out)
	page.Stderr.DataBase64 = base64.StdEncoding.EncodeToString(errout)
	return page, nil
}

// captureOutput observes state and both streams together, copying at most limit bytes per stream.
func (manager *commandManager) captureOutput(id string, stdoutOffset, stderrOffset, limit int64) (commandOutputPage, []byte, []byte, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.prune(manager.policy.now().UTC())
	task, exists := manager.tasks[id]
	if !commandIDPattern.MatchString(id) || !exists {
		return commandOutputPage{}, nil, nil, errCommandNotFound
	}
	// Match terminal publication's lock order; writers never acquire manager.mu.
	task.stdout.mu.Lock()
	defer task.stdout.mu.Unlock()
	task.stderr.mu.Lock()
	defer task.stderr.mu.Unlock()
	if stdoutOffset < 0 || stderrOffset < 0 || limit < 1 || limit > maximumOutputPageSize || stdoutOffset > int64(len(task.stdout.data)) || stderrOffset > int64(len(task.stderr.data)) {
		return commandOutputPage{}, nil, nil, errInvalidOutputRequest
	}
	outPage, out := task.stdout.copyPage(stdoutOffset, limit)
	errPage, errout := task.stderr.copyPage(stderrOffset, limit)
	page := commandOutputPage{CommandID: id, Status: task.snapshot.Status, Stdout: outPage, Stderr: errPage}
	page.EOF = page.Status != "running" && !outPage.HasMore && !errPage.HasMore
	return page, out, errout, nil
}

// copyPage derives byte positions and copies one bounded range while the stream lock is held.
func (output *taskOutput) copyPage(offset, limit int64) (outputStreamPage, []byte) {
	available := int64(len(output.data))
	end := offset + min(limit, available-offset)
	return outputStreamPage{NextOffset: end, AvailableBytes: available, HasMore: end < available}, append([]byte(nil), output.data[offset:end]...)
}
