package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHealthzReturnsOK verifies the health endpoint reports HTTP success.
func TestHealthzReturnsOK(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()

	newTestHandler(t, nil, 4).ServeHTTP(response, request)

	if got := response.Code; got != http.StatusOK {
		t.Errorf("status code = %d, want %d", got, http.StatusOK)
	}
}

// TestHealthzReturnsJSON verifies the health endpoint declares a JSON response.
func TestHealthzReturnsJSON(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()

	newTestHandler(t, nil, 4).ServeHTTP(response, request)

	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want %q", got, "application/json")
	}
}

// TestHealthzReturnsStatusOK verifies the response body contains the stable health status.
func TestHealthzReturnsStatusOK(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()

	newTestHandler(t, nil, 4).ServeHTTP(response, request)

	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	if body.Status != "ok" {
		t.Errorf("status = %q, want %q", body.Status, "ok")
	}
}

// TestHandlerReturnsNotFound verifies paths outside the registered API remain unavailable.
func TestHandlerReturnsNotFound(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/not-found", nil)
	response := httptest.NewRecorder()

	newTestHandler(t, nil, 4).ServeHTTP(response, request)

	if got := response.Code; got != http.StatusNotFound {
		t.Errorf("status code = %d, want %d", got, http.StatusNotFound)
	}
}

// TestHealthzRejectsUnsupportedMethods verifies only GET is accepted by the health endpoint.
func TestHealthzRejectsUnsupportedMethods(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	response := httptest.NewRecorder()

	newTestHandler(t, nil, 4).ServeHTTP(response, request)

	if got := response.Code; got != http.StatusMethodNotAllowed {
		t.Errorf("status code = %d, want %d", got, http.StatusMethodNotAllowed)
	}
}

// newTestHandler creates a handler for tests or stops immediately on invalid setup.
func newTestHandler(t *testing.T, runCommand CommandRunner, maxConcurrentCommands int) http.Handler {
	t.Helper()
	handler, err := NewHandler(runCommand, maxConcurrentCommands)
	if err != nil {
		t.Fatalf("create handler: %v", err)
	}
	return handler
}
