package server

import (
	"context"
	"net/http"
	"path"
	"strings"
)

// Handler owns HTTP routing and the Agent-scoped background command lifecycle.
type Handler struct {
	mux   *http.ServeMux
	tasks *commandManager
}

// ServeHTTP dispatches requests without transferring task ownership to a request context.
func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	// Keep malformed routes from being normalized or redirected into live handlers.
	escapedPath := request.URL.EscapedPath()
	if request.URL.Path != path.Clean(request.URL.Path) ||
		strings.Contains(escapedPath, "%2f") || strings.Contains(escapedPath, "%2F") {
		writeAPIError(response, http.StatusNotFound, "route not found")
		return
	}
	if taskPath, found := strings.CutPrefix(request.URL.Path, "/v1/commands/"); found {
		id, suffix, hasSuffix := strings.Cut(taskPath, "/")
		if hasSuffix && suffix != "cancel" && suffix != "output" {
			writeAPIError(response, http.StatusNotFound, "route not found")
			return
		}
		if !commandIDPattern.MatchString(id) {
			writeAPIError(response, http.StatusBadRequest, "invalid command ID")
			return
		}
	}
	handler.mux.ServeHTTP(response, request)
}

// BeginShutdown rejects future background submissions and cancels accepted work immediately.
func (handler *Handler) BeginShutdown() { handler.tasks.beginShutdown() }

// Shutdown waits for actual background Runner cleanup within the supplied lifecycle budget.
func (handler *Handler) Shutdown(ctx context.Context) error { return handler.tasks.shutdown(ctx) }

// NewHandler returns the agent HTTP handler with shared command concurrency control.
func NewHandler(runCommand CommandRunner, maxConcurrentCommands int) (*Handler, error) {
	return newHandler(runCommand, maxConcurrentCommands, defaultTaskPolicy())
}

// newHandler assembles the same shared limiter with injectable internal task bounds and clock.
func newHandler(runCommand CommandRunner, maxConcurrentCommands int, policy taskPolicy) (*Handler, error) {
	limiter, err := newCommandLimiter(maxConcurrentCommands)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	handler := &Handler{mux: mux, tasks: newCommandManager(runCommand, limiter, policy)}
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			response.Header().Set("Allow", http.MethodGet)
			writeAPIError(response, http.StatusMethodNotAllowed, "method must be GET")
			return
		}

		writeSuccess(response, map[string]string{"status": "ok"})
	})
	mux.Handle("/v1/commands:run", newCommandHandler(runCommand, limiter))
	mux.Handle("/v1/commands:stream", newCommandStreamHandler(runCommand, limiter))
	mux.HandleFunc("/v1/commands", handler.submitCommand)
	mux.HandleFunc("/v1/commands/{command_id}", handler.queryCommand)
	mux.HandleFunc("/v1/commands/{command_id}/cancel", handler.cancelCommand)
	mux.HandleFunc("/v1/commands/{command_id}/output", handler.commandOutput)
	mux.Handle("/v1/files", newFileHandler(workspaceDirectory))
	mux.HandleFunc("/", func(response http.ResponseWriter, _ *http.Request) {
		writeAPIError(response, http.StatusNotFound, "route not found")
	})
	return handler, nil
}
