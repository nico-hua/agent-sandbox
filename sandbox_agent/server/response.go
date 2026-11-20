package server

import (
	"encoding/json"
	"net/http"
)

type jsonResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

// writeSuccess sends a successful JSON response with optional business data.
func writeSuccess(response http.ResponseWriter, data any) {
	writeJSON(response, http.StatusOK, "ok", data)
}

// writeAPIError sends a fixed, public failure message without internal details.
func writeAPIError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, message, nil)
}

// writeJSON derives the HTTP status and envelope code from one value.
func writeJSON(response http.ResponseWriter, status int, message string, data any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(jsonResponse{Code: status, Message: message, Data: data})
}
