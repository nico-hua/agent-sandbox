package server

import (
	"io"
	"net/http"
)

// NewHandler returns the agent HTTP handler with its health endpoint registered.
func NewHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			response.Header().Set("Allow", http.MethodGet)
			http.Error(response, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(response, "{\"status\":\"ok\"}\n")
	})
	return mux
}
