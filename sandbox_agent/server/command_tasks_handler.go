package server

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
)

// submitCommand validates the existing command contract before accepting detached work.
func (handler *Handler) submitCommand(response http.ResponseWriter, request *http.Request) {
	decoded, execution, ok := validateCommandExecution(response, request)
	if !ok {
		return
	}
	snapshot, err := handler.tasks.submit(request.Context(), execution, decoded.Stdin)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		writeTaskError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, map[string]string{"command_id": snapshot.CommandID})
}

// commandOutput immediately returns one bounded, consistent observation without owning execution.
func (handler *Handler) commandOutput(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		response.Header().Set("Allow", http.MethodGet)
		writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
		return
	}
	stdout, stderr, limit, err := parseOutputQuery(request.URL.RawQuery)
	if err != nil {
		writeTaskError(response, err)
		return
	}
	page, err := handler.tasks.readOutput(request.PathValue("command_id"), stdout, stderr, limit)
	if err != nil {
		writeTaskError(response, err)
		return
	}
	response.Header().Set("Cache-Control", "no-store")
	writeJSON(response, http.StatusOK, page)
}

// parseOutputQuery rejects malformed, repeated or unknown parameters without type coercion.
func parseOutputQuery(raw string) (int64, int64, int64, error) {
	values, err := url.ParseQuery(raw)
	if err != nil {
		return 0, 0, 0, errInvalidOutputRequest
	}
	parsed := map[string]int64{"stdout_offset": 0, "stderr_offset": 0, "limit": defaultOutputPageSize}
	for name, entries := range values {
		if _, known := parsed[name]; !known || len(entries) != 1 || entries[0] == "" {
			return 0, 0, 0, errInvalidOutputRequest
		}
		for _, digit := range entries[0] {
			if digit < '0' || digit > '9' {
				return 0, 0, 0, errInvalidOutputRequest
			}
		}
		value, err := strconv.ParseInt(entries[0], 10, 64)
		if err != nil {
			return 0, 0, 0, errInvalidOutputRequest
		}
		parsed[name] = value
	}
	if parsed["limit"] < 1 || parsed["limit"] > maximumOutputPageSize {
		return 0, 0, 0, errInvalidOutputRequest
	}
	return parsed["stdout_offset"], parsed["stderr_offset"], parsed["limit"], nil
}

// queryCommand returns a consistent task snapshot without acquiring an execution slot.
func (handler *Handler) queryCommand(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		response.Header().Set("Allow", http.MethodGet)
		writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
		return
	}
	snapshot, err := handler.tasks.get(request.PathValue("command_id"))
	if err != nil {
		writeTaskError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, snapshot)
}

// cancelCommand acknowledges intent with 202; immutable terminal outcomes are returned with 200.
func (handler *Handler) cancelCommand(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	snapshot, running, err := handler.tasks.cancelTask(request.PathValue("command_id"))
	if err != nil {
		writeTaskError(response, err)
		return
	}
	status := http.StatusOK
	if running {
		status = http.StatusAccepted
	}
	writeJSON(response, status, snapshot)
}

// writeTaskError exposes fixed admission, lookup and output-query failures without private details.
func writeTaskError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errInvalidOutputRequest):
		writeAPIError(response, http.StatusBadRequest, "invalid_request", "invalid output query")
	case errors.Is(err, errCommandNotFound):
		writeAPIError(response, http.StatusNotFound, "command_not_found", "command not found")
	case errors.Is(err, errAgentStopping):
		writeAPIError(response, http.StatusServiceUnavailable, "agent_stopping", "agent is stopping")
	case errors.Is(err, errCommandCapacity):
		writeAPIError(response, http.StatusTooManyRequests, "command_capacity_exceeded", "too many commands are running")
	case errors.Is(err, errCommandRegistryFull):
		writeAPIError(response, http.StatusTooManyRequests, "command_registry_full", "command registry is full")
	default:
		writeAPIError(response, http.StatusInternalServerError, "command_submission_failed", "command could not be accepted")
	}
}
