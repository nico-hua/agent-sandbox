package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// Handler owns HTTP routing and the Agent-scoped background command lifecycle.
type Handler struct {
	mux   *http.ServeMux
	tasks *commandManager
}

// ServeHTTP dispatches requests without transferring task ownership to a request context.
func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if path, taskPath := strings.CutPrefix(request.URL.Path, "/v1/commands/"); taskPath {
		id, suffix, hasSuffix := strings.Cut(path, "/")
		// Reject malformed IDs before ServeMux can redirect/canonicalize path
		// components. Task identifiers never become process IDs or file paths.
		if !commandIDPattern.MatchString(id) || (hasSuffix && suffix != "cancel" && suffix != "output") {
			writeTaskError(response, errCommandNotFound)
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
			http.Error(response, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("/v1/commands:run", newCommandHandler(runCommand, limiter))
	mux.Handle("/v1/commands:stream", newCommandStreamHandler(runCommand, limiter))
	mux.HandleFunc("/v1/commands", handler.submitCommand)
	mux.HandleFunc("/v1/commands/{command_id}", handler.queryCommand)
	mux.HandleFunc("/v1/commands/{command_id}/cancel", handler.cancelCommand)
	mux.HandleFunc("/v1/commands/{command_id}/output", handler.commandOutput)
	mux.Handle("/v1/files", newFileHandler(workspaceDirectory))
	return handler, nil
}

// writeJSON sends a JSON response with a stable content type and status code.
func writeJSON(response http.ResponseWriter, statusCode int, body any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(statusCode)
	_ = json.NewEncoder(response).Encode(body)
}
