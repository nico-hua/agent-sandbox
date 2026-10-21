package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nico-hua/agent-sandbox/sandbox_agent/command"
)

const (
	commandRegistryCapacity = 16
	commandResultRetention  = 10 * time.Minute
)

var (
	errAgentStopping       = errors.New("agent stopping")
	errCommandCapacity     = errors.New("command capacity exceeded")
	errCommandRegistryFull = errors.New("command registry full")
	errCommandNotFound     = errors.New("command not found")
	errCommandSubmission   = errors.New("command submission failed")
	commandIDPattern       = regexp.MustCompile(`^cmd_[0-9a-f]{32}$`)
)

type taskPolicy struct {
	capacity  int
	retention time.Duration
	now       func() time.Time
}

// defaultTaskPolicy centralizes fixed registry bounds without adding public configuration knobs.
func defaultTaskPolicy() taskPolicy {
	return taskPolicy{capacity: commandRegistryCapacity, retention: commandResultRetention, now: time.Now}
}

type taskSnapshot struct {
	CommandID    string     `json:"command_id"`
	Status       string     `json:"status"`
	CreatedAt    time.Time  `json:"created_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	ExitCode     *int       `json:"exit_code,omitempty"`
	StdoutBase64 *string    `json:"stdout_base64,omitempty"`
	StderrBase64 *string    `json:"stderr_base64,omitempty"`
	Error        *apiError  `json:"error,omitempty"`
}

type commandTask struct {
	snapshot taskSnapshot
	cancel   context.CancelFunc
}

type commandManager struct {
	mu      sync.Mutex
	tasks   map[string]*commandTask
	runner  CommandRunner
	limiter *commandLimiter
	policy  taskPolicy
	ctx     context.Context
	cancel  context.CancelFunc
	closing bool
	workers sync.WaitGroup
	stopped chan struct{}
}

// newCommandManager owns task contexts until the Agent explicitly begins shutdown.
func newCommandManager(runner CommandRunner, limiter *commandLimiter, policy taskPolicy) *commandManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &commandManager{tasks: make(map[string]*commandTask), runner: runner, limiter: limiter,
		policy: policy, ctx: ctx, cancel: cancel, stopped: make(chan struct{})}
}

// submit atomically registers a validated request; only pre-acceptance cancellation prevents execution.
func (manager *commandManager) submit(caller context.Context, request command.Request, stdin string) (taskSnapshot, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	now := manager.policy.now().UTC()
	manager.prune(now)
	if manager.closing {
		return taskSnapshot{}, errAgentStopping
	}
	if err := caller.Err(); err != nil {
		return taskSnapshot{}, err
	}
	if len(manager.tasks) >= manager.policy.capacity {
		return taskSnapshot{}, errCommandRegistryFull
	}
	if !manager.limiter.tryAcquire() {
		return taskSnapshot{}, errCommandCapacity
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		manager.limiter.release()
		return taskSnapshot{}, errCommandSubmission
	}
	id := "cmd_" + hex.EncodeToString(random[:])
	if _, exists := manager.tasks[id]; exists {
		manager.limiter.release()
		return taskSnapshot{}, errCommandSubmission
	}
	// Own every mutable part of the invocation before acceptance. No handler or
	// request context is retained; the 202 response may be lost after this point.
	request.Argv = append([]string(nil), request.Argv...)
	request.Env = maps.Clone(request.Env)
	if err := caller.Err(); err != nil {
		manager.limiter.release()
		return taskSnapshot{}, err
	}
	ctx, cancel := context.WithCancel(manager.ctx)
	task := &commandTask{snapshot: taskSnapshot{CommandID: id, Status: "running", CreatedAt: now}, cancel: cancel}
	manager.tasks[id] = task
	manager.workers.Add(1)
	go manager.execute(ctx, task, request, stdin)
	return task.snapshot, nil
}

// get returns one consistent observation; terminal snapshots are never modified after publication.
func (manager *commandManager) get(id string) (taskSnapshot, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.prune(manager.policy.now().UTC())
	task, exists := manager.tasks[id]
	if !commandIDPattern.MatchString(id) || !exists {
		return taskSnapshot{}, errCommandNotFound
	}
	return task.snapshot, nil
}

// cancelTask requests cancellation without publishing a terminal state or releasing execution capacity.
func (manager *commandManager) cancelTask(id string) (taskSnapshot, bool, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.prune(manager.policy.now().UTC())
	task, exists := manager.tasks[id]
	if !commandIDPattern.MatchString(id) || !exists {
		return taskSnapshot{}, false, errCommandNotFound
	}
	running := task.snapshot.Status == "running"
	if running {
		task.cancel()
	}
	return task.snapshot, running, nil
}

// prune removes only terminal records retained for the full policy duration; caller holds mu.
func (manager *commandManager) prune(now time.Time) {
	for id, task := range manager.tasks {
		if end := task.snapshot.FinishedAt; end != nil && !now.Before(end.Add(manager.policy.retention)) {
			delete(manager.tasks, id)
		}
	}
}

// beginShutdown serializes against registration, cancels tasks, and starts one bounded-lifetime waiter.
func (manager *commandManager) beginShutdown() {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.closing {
		return
	}
	manager.closing = true
	manager.cancel()
	// Add and the transition to closing share mu, so Wait cannot race a new Add.
	go func() { manager.workers.Wait(); close(manager.stopped) }()
}

// shutdown waits for actual Runner/output/slot cleanup or explicitly reports the caller's deadline.
func (manager *commandManager) shutdown(ctx context.Context) error {
	manager.beginShutdown()
	select {
	case <-manager.stopped:
		return nil
	default:
	}
	select {
	case <-manager.stopped:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("background commands did not finish before shutdown: %w", ctx.Err())
	}
}

// execute collects bounded raw bytes and publishes exactly one Runner-determined outcome.
func (manager *commandManager) execute(ctx context.Context, task *commandTask, request command.Request, stdin string) {
	defer manager.workers.Done()
	defer manager.limiter.release()
	defer task.cancel()
	stdout := &taskOutput{limit: request.MaxOutputBytesPerStream, cancel: task.cancel}
	stderr := &taskOutput{limit: request.MaxOutputBytesPerStream, cancel: task.cancel}
	result, err := manager.runner(ctx, request, strings.NewReader(stdin), stdout, stderr)
	out, outExceeded := stdout.encoded()
	errout, errExceeded := stderr.encoded()
	if outExceeded || errExceeded {
		err = command.ErrOutputLimitExceeded
	}
	snapshot := taskSnapshot{CommandID: task.snapshot.CommandID, CreatedAt: task.snapshot.CreatedAt,
		StdoutBase64: &out, StderrBase64: &errout}
	switch {
	case err == nil:
		snapshot.Status, snapshot.ExitCode = "exited", &result.ExitCode
	case errors.Is(err, context.Canceled) && ctx.Err() != nil:
		snapshot.Status = "canceled"
		snapshot.Error = &apiError{"command_canceled", "command was canceled"}
	default:
		snapshot.Status = "failed"
		_, public := commandFailure(err)
		snapshot.Error = &public
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	finished := manager.policy.now().UTC()
	snapshot.FinishedAt = &finished
	task.snapshot = snapshot
}

type taskOutput struct {
	mu       sync.Mutex
	data     []byte
	limit    int64
	exceeded bool
	cancel   context.CancelFunc
}

// Write stores at most one stream's allowance, canceling even a Runner that ignores write errors.
func (output *taskOutput) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	allowed := min(int64(len(data)), output.limit-int64(len(output.data)))
	output.data = append(output.data, data[:allowed]...)
	if allowed < int64(len(data)) {
		output.exceeded = true
		output.cancel()
		return int(allowed), command.ErrOutputLimitExceeded
	}
	return int(allowed), nil
}

// encoded freezes a bounded stream into an immutable Base64 result after Runner returns.
func (output *taskOutput) encoded() (string, bool) {
	output.mu.Lock()
	defer output.mu.Unlock()
	return base64.StdEncoding.EncodeToString(output.data), output.exceeded
}
