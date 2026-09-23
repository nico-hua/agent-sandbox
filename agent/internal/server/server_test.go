package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestWriteTimeoutExceedsMaximumCommandTimeout verifies HTTP responses outlive allowed commands.
func TestWriteTimeoutExceedsMaximumCommandTimeout(t *testing.T) {
	const maximumAllowedCommandTimeout = 30 * time.Second
	if writeTimeout <= maximumAllowedCommandTimeout {
		t.Errorf("write timeout = %v, want greater than maximum command timeout %v", writeTimeout, maximumAllowedCommandTimeout)
	}
}

// TestRunServesRequestsAndStopsOnCancellation verifies the server accepts traffic and shuts down cleanly.
func TestRunServesRequestsAndStopsOnCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on temporary address: %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outcomes := make(chan error, 1)
	go func() {
		outcomes <- Run(ctx, listener, NewHandler(nil))
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + listener.Addr().String() + "/healthz")
	if err != nil {
		t.Fatalf("request running server: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Errorf("status code = %d, want %d", response.StatusCode, http.StatusOK)
	}

	cancel()
	select {
	case err := <-outcomes:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after context cancellation")
	}
}

// TestRunReturnsListenerError verifies a network serving failure is returned to the caller.
func TestRunReturnsListenerError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on temporary address: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	err = Run(context.Background(), listener, NewHandler(nil))
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil")
	}
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Run() error = %v, want errors.Is(error, net.ErrClosed)", err)
	}
	if errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Run() error = %v, want listener error instead of http.ErrServerClosed", err)
	}
}
