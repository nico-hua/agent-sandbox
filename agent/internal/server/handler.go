package server

import (
	"encoding/json"
	"net/http"
)

// NewHandler returns the agent HTTP handler with its public routes registered.
func NewHandler(runCommand CommandRunner) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			response.Header().Set("Allow", http.MethodGet)
			http.Error(response, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("/v1/commands:run", newCommandHandler(runCommand))
	return mux
}

// writeJSON sends a JSON response with a stable content type and status code.
func writeJSON(response http.ResponseWriter, statusCode int, body any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(statusCode)
	_ = json.NewEncoder(response).Encode(body)
}
