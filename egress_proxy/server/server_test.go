package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/egress_proxy/network"
)

// TestProxySharedCapacityAndShutdown retains the shared slot through CONNECT I/O and closes blocked tunnels on shutdown.
func TestProxySharedCapacityAndShutdown(t *testing.T) {
	upstream, _ := net.Listen("tcp4", "127.0.0.1:0")
	defer upstream.Close()
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		conn, err := upstream.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()
	var calls atomic.Int32
	limits := defaultProxyLimits()
	limits.active = 1
	proxy := newProxy(mappedDialer(t, upstream.Addr().String(), &calls), limits)
	address := startTestProxy(t, proxy)
	connection, reader, response := openConnect(t, address, "")
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	client := proxyClient(t, address)
	rejected, err := client.Get("http://example.com/")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, rejected.Body)
	_ = rejected.Body.Close()
	if rejected.StatusCode != 429 || calls.Load() != 1 {
		t.Fatalf("shared capacity: %d %d", rejected.StatusCode, calls.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := proxy.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	if data, _ := io.ReadAll(reader); len(data) != 0 {
		t.Fatalf("shutdown inserted tunnel bytes: %q", data)
	}
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("upstream not closed")
	}
	rejected, err = client.Get("http://example.com/")
	if err != nil {
		t.Fatal(err)
	}
	_ = rejected.Body.Close()
	if rejected.StatusCode != 503 {
		t.Fatalf("shutdown admission: %d", rejected.StatusCode)
	}
	proxy.mu.Lock()
	remaining := len(proxy.jobs)
	proxy.mu.Unlock()
	if remaining != 0 {
		t.Fatal("shutdown left active tasks")
	}
}

// TestProxyShutdownWaitsForLateDial does not release a task while a detached Transport dial is still settling.
func TestProxyShutdownWaitsForLateDial(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	late, peer := testPipe(t)
	defer peer.Close()
	resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	})
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return late, nil
	}
	proxy := NewProxy(network.NewDialer(allowedPolicy(t), resolve, dial))
	address := startTestProxy(t, proxy)
	requestEnded := make(chan error, 1)
	client := proxyClient(t, address)
	go func() {
		response, err := client.Get("http://example.com/")
		if response != nil {
			_ = response.Body.Close()
			if err == nil {
				err = errors.New("canceled request completed as an HTTP response")
			}
		}
		requestEnded <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dial not entered")
	}
	shutdownEnded := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { shutdownEnded <- proxy.Shutdown(ctx) }()
	<-canceled
	select {
	case err := <-shutdownEnded:
		t.Fatalf("shutdown returned before late dial: %v", err)
	default:
	}
	release <- struct{}{}
	if err := <-shutdownEnded; err != nil {
		t.Fatal(err)
	}
	if !late.closed.Load() {
		t.Fatal("late socket not closed")
	}
	if err := <-requestEnded; err == nil {
		t.Fatal("canceled request looked successful")
	}
}

// TestInboundConnectionLimitIncludesIdleAndHijacked verifies direct connection rejection and recovery after tunnel cleanup.
func TestInboundConnectionLimitIncludesIdleAndHijacked(t *testing.T) {
	upstream, _ := net.Listen("tcp4", "127.0.0.1:0")
	defer upstream.Close()
	go func() {
		connection, err := upstream.Accept()
		if err == nil {
			defer connection.Close()
			_, _ = io.Copy(io.Discard, connection)
		}
	}()
	var calls atomic.Int32
	limits := defaultProxyLimits()
	limits.connections = 2
	proxy := newProxy(mappedDialer(t, upstream.Addr().String(), &calls), limits)
	address := startTestProxy(t, proxy)
	parsed, _ := url.Parse(address)
	tunnel, _, response := openConnect(t, address, "")
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	idle, err := net.DialTimeout("tcp4", parsed.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	// Prove the idle connection is accepted by finishing just its request line;
	// its remaining headers stay pending and count against the same cap.
	_, _ = io.WriteString(idle, "GET /healthz HTTP/1.1\r\n")
	extra, err := net.DialTimeout("tcp4", parsed.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	_ = extra.SetDeadline(time.Now().Add(time.Second))
	_, _ = io.WriteString(extra, "GET /healthz HTTP/1.1\r\nHost: local\r\n\r\n")
	if line, err := bufio.NewReader(extra).ReadString('\n'); err == nil || strings.Contains(line, "200") {
		t.Fatalf("excess connection accepted: %q %v", line, err)
	}
	_ = idle.Close()
	_ = tunnel.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout("tcp4", parsed.Host, time.Second)
		if err == nil {
			_ = connection.SetDeadline(time.Now().Add(time.Second))
			_, _ = io.WriteString(connection, "GET /healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
			reply, readErr := http.ReadResponse(bufio.NewReader(connection), nil)
			_ = connection.Close()
			if readErr == nil {
				_ = reply.Body.Close()
				if reply.StatusCode == 200 {
					return
				}
			}
		}
		runtime.Gosched()
	}
	t.Fatal("connection capacity not returned")
}

// TestHeaderLimits rejects oversized inbound/upstream headers before any unbounded header buffering.
func TestHeaderLimits(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Huge", strings.Repeat("x", 65536))
		_, _ = io.WriteString(w, "body")
	}))
	defer upstream.Close()
	address := startTestProxy(t, NewProxy(mappedDialer(t, upstream.Listener.Addr().String(), &calls)))
	response, err := proxyClient(t, address).Get("http://example.com/")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 502 {
		t.Fatalf("upstream header limit: %d", response.StatusCode)
	}
	parsed, _ := url.Parse(address)
	for _, size := range []int{33 << 10, 64 << 10} {
		connection, err := net.DialTimeout("tcp4", parsed.Host, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = connection.SetDeadline(time.Now().Add(time.Second))
		_, _ = io.WriteString(connection, "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\nX-Huge: "+strings.Repeat("x", size)+"\r\n\r\n")
		reply, err := http.ReadResponse(bufio.NewReader(connection), nil)
		_ = connection.Close()
		if err != nil {
			t.Fatal(err)
		}
		_ = reply.Body.Close()
		if reply.StatusCode != 431 || calls.Load() != 1 {
			t.Fatalf("inbound header limit %d: %d calls=%d", size, reply.StatusCode, calls.Load())
		}
	}
}

// TestShutdownAdmissionRace settles concurrently accepted and rejected requests without leaking job registrations.
func TestShutdownAdmissionRace(t *testing.T) {
	resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	})
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }
	proxy := NewProxy(network.NewDialer(allowedPolicy(t), resolve, dial))
	start := make(chan struct{})
	var requests sync.WaitGroup
	for range 32 {
		requests.Add(1)
		go func() {
			defer requests.Done()
			<-start
			defer func() {
				if recovered := recover(); recovered != nil && recovered != http.ErrAbortHandler {
					t.Errorf("unexpected panic: %v", recovered)
				}
			}()
			proxy.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "http://example.com/", nil))
		}()
	}
	close(start)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := proxy.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	requests.Wait()
	proxy.mu.Lock()
	remaining := len(proxy.jobs)
	proxy.mu.Unlock()
	if remaining != 0 {
		t.Fatal("shutdown registration leak")
	}
}

// TestRunReportsUnfinishedDialCleanup returns a shared-budget failure rather than claiming a blocked dial has ended.
func TestRunReportsUnfinishedDialCleanup(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	connection, _ := testPipe(t)
	resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	})
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		<-release
		return connection, nil
	}
	limits := defaultProxyLimits()
	limits.shutdownTimeout = 20 * time.Millisecond
	proxy := newProxy(network.NewDialer(allowedPolicy(t), resolve, dial), limits)
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- Run(ctx, listener, proxy) }()
	client := proxyClient(t, "http://"+listener.Addr().String())
	requestEnded := make(chan struct{})
	go func() {
		defer close(requestEnded)
		response, _ := client.Get("http://example.com/")
		if response != nil {
			_ = response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dial not entered")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unfinished shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run exceeded shutdown budget")
	}
	release <- struct{}{}
	settle, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := proxy.Shutdown(settle); err != nil {
		t.Fatal(err)
	}
	if !connection.closed.Load() {
		t.Fatal("late connection leaked after shutdown failure")
	}
	<-requestEnded
}

// TestReadHeaderDeadline closes a partial request within the configured finite header budget.
func TestReadHeaderDeadline(t *testing.T) {
	limits := defaultProxyLimits()
	limits.headerTimeout = 50 * time.Millisecond
	address := startTestProxy(t, newProxy(nil, limits))
	parsed, _ := url.Parse(address)
	connection, err := net.DialTimeout("tcp4", parsed.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(time.Second))
	_, _ = io.WriteString(connection, "GET /healthz HTTP/1.1\r\nHost:")
	if data, err := io.ReadAll(connection); err != nil || strings.Contains(string(data), "200 OK") {
		t.Fatalf("partial headers not closed: %q %v", data, err)
	}
}
