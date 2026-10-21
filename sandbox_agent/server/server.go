package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 35 * time.Second
	idleTimeout       = 60 * time.Second
	shutdownTimeout   = 5 * time.Second
)

// Run serves HTTP and settles accepted background commands within the Agent shutdown budget.
func Run(ctx context.Context, listener net.Listener, handler http.Handler) error {
	return runServer(ctx, listener, handler, shutdownTimeout)
}

// runServer shares one finite shutdown budget across HTTP draining and background Runner cleanup.
func runServer(ctx context.Context, listener net.Listener, handler http.Handler, budget time.Duration) error {
	httpServer := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- httpServer.Serve(listener)
	}()

	var serveErr error
	serveStopped := false
	select {
	case serveErr = <-serveErrors:
		serveStopped = true
	case <-ctx.Done():
	}

	lifecycle, managed := handler.(interface {
		BeginShutdown()
		Shutdown(context.Context) error
	})
	if managed {
		lifecycle.BeginShutdown()
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	// Shutdown closes the listener promptly while detached commands settle in
	// parallel. Neither completion can stand in for the other.
	httpStopped := make(chan error, 1)
	go func() { httpStopped <- httpServer.Shutdown(shutdownContext) }()
	var tasksErr error
	if managed {
		tasksErr = lifecycle.Shutdown(shutdownContext)
	}
	httpErr := <-httpStopped
	if httpErr != nil {
		_ = httpServer.Close()
	}
	if !serveStopped {
		serveErr = <-serveErrors
	}
	var failures []error
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		failures = append(failures, fmt.Errorf("serve HTTP: %w", serveErr))
	}
	if httpErr != nil {
		failures = append(failures, fmt.Errorf("shut down HTTP server: %w", httpErr))
	}
	if tasksErr != nil {
		failures = append(failures, tasksErr)
	}
	return errors.Join(failures...)
}
