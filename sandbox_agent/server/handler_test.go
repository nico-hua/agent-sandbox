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
	if err := json.Unmarshal(decodeJSONResponse(t, response, 200, "ok"), &body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	if body.Status != "ok" {
		t.Errorf("status = %q, want %q", body.Status, "ok")
	}
}

// TestHandlerReturnsNotFound verifies paths outside the registered API remain unavailable.
func TestHandlerReturnsNotFound(t *testing.T) {
	handler := newTestHandler(t, nil, 4)
	for _, path := range []string{
		"/not-found", "/healthz/", "/v1//files", "/v1/./files", "/v1/%2e/files",
		"/v1/commands/bad/unknown", "/v1/commands/cmd_0123456789abcdef0123456789abcdef%2fcancel",
	} {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			assertErrorResponse(t, response, http.StatusNotFound, "route not found")
			if location := response.Header().Get("Location"); location != "" {
				t.Errorf("unexpected redirect Location %q", location)
			}
		})
	}
}

// TestHealthzRejectsUnsupportedMethods verifies only GET is accepted by the health endpoint.
func TestHealthzRejectsUnsupportedMethods(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	response := httptest.NewRecorder()

	newTestHandler(t, nil, 4).ServeHTTP(response, request)

	assertErrorResponse(t, response, http.StatusMethodNotAllowed, "method must be GET")
	if got := response.Header().Get("Allow"); got != http.MethodGet {
		t.Errorf("Allow = %q, want GET", got)
	}
}

// decodeJSONResponse verifies the envelope independently of the server's response type.
func decodeJSONResponse(t *testing.T, response *httptest.ResponseRecorder, status int, message string) json.RawMessage {
	t.Helper()
	if response.Code != status || response.Header().Get("Content-Type") != "application/json" {
		t.Errorf("status = %d, content type = %q; want %d and application/json", response.Code, response.Header().Get("Content-Type"), status)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode JSON response: %v", err)
	}
	if len(envelope) != 3 || envelope["code"] == nil || envelope["message"] == nil || envelope["data"] == nil {
		t.Fatalf("expected exactly code, message and data, got %s", response.Body.String())
	}
	var code int
	if err := json.Unmarshal(envelope["code"], &code); err != nil || code != status {
		t.Errorf("code = %s, want integer %d", envelope["code"], status)
	}
	var actualMessage string
	if err := json.Unmarshal(envelope["message"], &actualMessage); err != nil || actualMessage == "" || (message != "" && actualMessage != message) {
		t.Errorf("message = %s, want a nonempty string matching %q when specified", envelope["message"], message)
	}
	return envelope["data"]
}

// TestHandlerRegistersFileEndpoint verifies the main mux exposes the file API.
func TestHandlerRegistersFileEndpoint(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/files?path=../outside", nil)
	response := httptest.NewRecorder()
	newTestHandler(t, nil, 4).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", response.Code, http.StatusBadRequest)
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
