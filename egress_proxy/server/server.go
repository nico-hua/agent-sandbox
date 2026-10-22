// Package server implements bounded HTTP/1.1 forwarding and CONNECT over the guarded network dialer.
package server

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/nico-hua/agent-sandbox/egress_proxy/network"
)

type proxyLimits struct {
	active, connections, headerBytes                           int
	headerTimeout, httpTimeout, tunnelTimeout, shutdownTimeout time.Duration
	bodyBytes, tunnelBytes                                     int64
}

// defaultProxyLimits centralizes finite policy without exposing bypass switches or extra flags.
func defaultProxyLimits() proxyLimits {
	return proxyLimits{16, 32, 32 << 10, 5 * time.Second, 30 * time.Second, 60 * time.Second, 5 * time.Second, 10 << 20, 32 << 20}
}

// Proxy is an HTTP/1.1 explicit proxy with a guarded network.Dialer and an owned shutdown lifecycle.
// Use Run to also enforce inbound connection/header limits and track hijacked sockets.
type Proxy struct {
	dialer  *network.Dialer
	limits  proxyLimits
	mu      sync.Mutex
	closing bool
	jobs    map[*proxyTask]struct{}
	workers sync.WaitGroup
	stopped chan struct{}
}

type clientConnectionKey struct{}

type proxyTask struct {
	owner       *Proxy
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closing     bool
	committed   bool
	client      net.Conn
	connections map[*ownedConn]struct{}
	dials       sync.WaitGroup
	stopWatch   func() bool
	stopRequest func() bool
	aborted     chan struct{}
}

// NewProxy constructs a default-deny proxy when dialer is nil, without network I/O.
func NewProxy(dialer *network.Dialer) *Proxy { return newProxy(dialer, defaultProxyLimits()) }

// newProxy injects smaller finite limits only within package tests.
func newProxy(dialer *network.Dialer, limits proxyLimits) *Proxy {
	if dialer == nil {
		dialer = network.NewDialer(nil, nil, nil)
	}
	return &Proxy{dialer: dialer, limits: limits, jobs: make(map[*proxyTask]struct{}), stopped: make(chan struct{})}
}

// begin serializes registration with shutdown; no dial, close or wait occurs under the global lock.
func (proxy *Proxy) begin(request *http.Request, duration time.Duration) (*proxyTask, int) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if proxy.closing {
		return nil, 503
	}
	if len(proxy.jobs) >= proxy.limits.active {
		return nil, 429
	}
	deadline := time.Now().Add(duration)
	if earlier, ok := request.Context().Deadline(); ok && earlier.Before(deadline) {
		deadline = earlier
	}
	ctx, cancel := context.WithDeadline(context.WithoutCancel(request.Context()), deadline)
	task := &proxyTask{owner: proxy, ctx: ctx, cancel: cancel, connections: make(map[*ownedConn]struct{}), aborted: make(chan struct{})}
	task.client, _ = request.Context().Value(clientConnectionKey{}).(net.Conn)
	task.stopWatch = context.AfterFunc(ctx, task.abort)
	task.stopRequest = func() bool { return true }
	if request.Method != http.MethodConnect {
		task.stopRequest = context.AfterFunc(request.Context(), cancel)
	}
	proxy.jobs[task] = struct{}{}
	proxy.workers.Add(1)
	return task, 0
}

// beginShutdown closes admission before canceling any registered task, preventing missed registrations.
func (proxy *Proxy) beginShutdown() {
	proxy.mu.Lock()
	if proxy.closing {
		proxy.mu.Unlock()
		return
	}
	proxy.closing = true
	tasks := make([]*proxyTask, 0, len(proxy.jobs))
	for task := range proxy.jobs {
		tasks = append(tasks, task)
	}
	proxy.mu.Unlock()
	for _, task := range tasks {
		task.cancel()
	}
	go func() { proxy.workers.Wait(); close(proxy.stopped) }()
}

// Shutdown rejects new work, interrupts active I/O and waits for task and dial cleanup within ctx.
func (proxy *Proxy) Shutdown(ctx context.Context) error {
	proxy.beginShutdown()
	select {
	case <-proxy.stopped:
		return nil
	default:
	}
	select {
	case <-proxy.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// abort closes both sides on cancellation/deadline, without holding locks during network operations.
func (task *proxyTask) abort() {
	task.mu.Lock()
	task.closing = true
	client := task.client
	committed := task.committed
	connections := make([]*ownedConn, 0, len(task.connections))
	for connection := range task.connections {
		connections = append(connections, connection)
	}
	task.mu.Unlock()
	if client != nil {
		if committed {
			_ = client.Close()
		} else if half, ok := client.(interface{ CloseRead() error }); ok {
			_ = half.CloseRead()
		}
	}
	for _, connection := range connections {
		_ = connection.Close()
	}
	close(task.aborted)
}

// commit serializes response/tunnel commitment against deadline and shutdown interruption.
func (task *proxyTask) commit() bool {
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.closing || task.ctx.Err() != nil {
		return false
	}
	task.committed = true
	return true
}

// finish stops the deadline callback, closes owned upstreams and waits for late dial results before releasing capacity.
func (task *proxyTask) finish() {
	task.stopRequest()
	if !task.stopWatch() {
		<-task.aborted
	}
	task.cancel()
	task.mu.Lock()
	task.closing = true
	connections := make([]*ownedConn, 0, len(task.connections))
	for connection := range task.connections {
		connections = append(connections, connection)
	}
	task.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
	task.dials.Wait()
	task.owner.mu.Lock()
	delete(task.owner.jobs, task)
	task.owner.mu.Unlock()
	task.owner.workers.Done()
}

// dial links Transport's potentially detached dial context back to the task's full lifetime.
func (task *proxyTask) dial(ctx context.Context, network, address string) (net.Conn, error) {
	task.mu.Lock()
	if task.closing {
		task.mu.Unlock()
		return nil, context.Canceled
	}
	task.dials.Add(1)
	task.mu.Unlock()
	defer task.dials.Done()
	dialContext, cancel := context.WithCancel(task.ctx)
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	defer cancel()
	connection, err := task.owner.dialer.DialContext(dialContext, network, address)
	if err != nil {
		return nil, err
	}
	owned := &ownedConn{Conn: connection}
	owned.onClose = func() { task.mu.Lock(); delete(task.connections, owned); task.mu.Unlock() }
	task.mu.Lock()
	if task.closing || dialContext.Err() != nil {
		task.mu.Unlock()
		_ = owned.Close()
		return nil, context.Canceled
	}
	task.connections[owned] = struct{}{}
	task.mu.Unlock()
	return owned, nil
}

// Run serves HTTP/1.1 with inbound capacity and shares one shutdown budget across HTTP and proxy tasks.
func Run(ctx context.Context, listener net.Listener, proxy *Proxy) error {
	bounded := &capacityListener{Listener: listener, limit: proxy.limits.connections, connections: make(map[*ownedConn]struct{})}
	server := &http.Server{Handler: proxy, ReadHeaderTimeout: proxy.limits.headerTimeout,
		ReadTimeout: proxy.limits.httpTimeout, WriteTimeout: proxy.limits.httpTimeout, IdleTimeout: proxy.limits.headerTimeout,
		// net/http adds 4 KiB of parser slop to MaxHeaderBytes; reserve that
		// within the configured inbound header budget instead of beyond it.
		MaxHeaderBytes: max(1, proxy.limits.headerBytes-4096), ErrorLog: log.New(io.Discard, "", 0),
		ConnContext: func(ctx context.Context, connection net.Conn) context.Context {
			return context.WithValue(ctx, clientConnectionKey{}, connection)
		}}
	served := make(chan error, 1)
	go func() { served <- server.Serve(bounded) }()
	var serveErr error
	stopped := false
	select {
	case serveErr = <-served:
		stopped = true
	case <-ctx.Done():
	}
	proxy.beginShutdown()
	shutdown, cancel := context.WithTimeout(context.Background(), proxy.limits.shutdownTimeout)
	defer cancel()
	httpStopped := make(chan error, 1)
	go func() { httpStopped <- server.Shutdown(shutdown) }()
	proxyErr := proxy.Shutdown(shutdown)
	httpErr := <-httpStopped
	if httpErr != nil {
		_ = server.Close()
	}
	_ = bounded.Close()
	if !stopped {
		serveErr = <-served
	}
	if errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, net.ErrClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, httpErr, proxyErr)
}
