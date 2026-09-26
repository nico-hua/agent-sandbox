package server

import (
	"encoding/json"
	"net/http"
)

// NewHandler returns the agent HTTP handler with shared command concurrency control.
func NewHandler(runCommand CommandRunner, maxConcurrentCommands int) (http.Handler, error) {
	limiter, err := newCommandLimiter(maxConcurrentCommands)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			response.Header().Set("Allow", http.MethodGet)
			http.Error(response, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("/v1/commands:run", newCommandHandler(runCommand, limiter))
	mux.Handle("/v1/files", newFileHandler(workspaceDirectory))
	return mux, nil
}

// writeJSON sends a JSON response with a stable content type and status code.
func writeJSON(response http.ResponseWriter, statusCode int, body any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(statusCode)
	_ = json.NewEncoder(response).Encode(body)
}
