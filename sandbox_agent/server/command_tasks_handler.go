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
	decoded, execution, ok := validateCommandExecution(response, request, defaultBackgroundCommandTimeout, maximumBackgroundCommandTimeout)
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
	writeSuccess(response, map[string]string{"command_id": snapshot.CommandID})
}

// commandOutput immediately returns one bounded, consistent observation without owning execution.
func (handler *Handler) commandOutput(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		response.Header().Set("Allow", http.MethodGet)
		writeAPIError(response, http.StatusMethodNotAllowed, "method must be GET")
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
	writeSuccess(response, page)
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
		writeAPIError(response, http.StatusMethodNotAllowed, "method must be GET")
		return
	}
	snapshot, err := handler.tasks.get(request.PathValue("command_id"))
	if err != nil {
		writeTaskError(response, err)
		return
	}
	writeSuccess(response, snapshot)
}

// cancelCommand returns the current task snapshot after recording the cancellation intent.
func (handler *Handler) cancelCommand(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		writeAPIError(response, http.StatusMethodNotAllowed, "method must be POST")
		return
	}
	snapshot, _, err := handler.tasks.cancelTask(request.PathValue("command_id"))
	if err != nil {
		writeTaskError(response, err)
		return
	}
	writeSuccess(response, snapshot)
}

// writeTaskError exposes fixed admission, lookup and output-query failures without private details.
func writeTaskError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errInvalidOutputRequest):
		writeAPIError(response, http.StatusBadRequest, "invalid output query")
	case errors.Is(err, errCommandNotFound):
		writeAPIError(response, http.StatusNotFound, "command not found")
	case errors.Is(err, errAgentStopping):
		writeAPIError(response, http.StatusServiceUnavailable, "agent is stopping")
	case errors.Is(err, errCommandCapacity):
		writeAPIError(response, http.StatusTooManyRequests, "too many commands are running")
	case errors.Is(err, errCommandRegistryFull):
		writeAPIError(response, http.StatusTooManyRequests, "command registry is full")
	default:
		writeAPIError(response, http.StatusInternalServerError, "command could not be accepted")
	}
}
