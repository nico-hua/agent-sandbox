package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nico-hua/agent-sandbox/agent/internal/command"
)

const (
	commandRequestBodyLimit         = 1024 * 1024
	defaultCommandTimeout           = 5 * time.Second
	maximumCommandTimeout           = 30 * time.Second
	defaultCommandOutputLimit int64 = 64 * 1024
	maximumCommandOutputLimit int64 = 1024 * 1024
)

// CommandRunner executes a command with explicit standard streams.
type CommandRunner func(
	context.Context,
	command.Request,
	io.Reader,
	io.Writer,
	io.Writer,
) (command.Result, error)

type runCommandRequest struct {
	Argv                    []string          `json:"argv"`
	Cwd                     string            `json:"cwd,omitempty"`
	Env                     map[string]string `json:"env,omitempty"`
	Stdin                   string            `json:"stdin,omitempty"`
	TimeoutMS               *int64            `json:"timeout_ms,omitempty"`
	MaxOutputBytesPerStream *int64            `json:"max_output_bytes_per_stream,omitempty"`
}

type runCommandResponse struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

type apiErrorResponse struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// newCommandHandler creates the synchronous command endpoint with bounded client options and concurrency.
func newCommandHandler(runCommand CommandRunner, limiter *commandLimiter) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			response.Header().Set("Allow", http.MethodPost)
			writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
			return
		}

		decoded, ok := decodeRunCommandRequest(response, request)
		if !ok {
			return
		}
		commandRequest, err := decoded.commandRequest()
		if err != nil {
			writeAPIError(response, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		if request.Context().Err() != nil {
			return
		}
		if !limiter.tryAcquire() {
			writeAPIError(
				response,
				http.StatusTooManyRequests,
				"command_capacity_exceeded",
				"too many commands are running",
			)
			return
		}
		defer limiter.release()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		result, err := runCommand(
			request.Context(),
			commandRequest,
			strings.NewReader(decoded.Stdin),
			&stdout,
			&stderr,
		)
		if request.Context().Err() != nil || errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			writeCommandError(response, err)
			return
		}

		writeJSON(response, http.StatusOK, runCommandResponse{
			ExitCode: result.ExitCode,
			Stdout:   stdout.String(),
			Stderr:   stderr.String(),
		})
	})
}

// commandRequest validates public execution settings and resolves service defaults.
func (request runCommandRequest) commandRequest() (command.Request, error) {
	if len(request.Argv) == 0 || request.Argv[0] == "" {
		return command.Request{}, errors.New("argv must contain an executable")
	}
	for name, value := range request.Env {
		if name == "" || strings.Contains(name, "=") || strings.ContainsRune(name, '\x00') {
			return command.Request{}, errors.New("environment variable names must be non-empty and contain neither '=' nor NUL")
		}
		if strings.ContainsRune(value, '\x00') {
			return command.Request{}, errors.New("environment variable values must not contain NUL")
		}
	}

	timeout := defaultCommandTimeout
	if request.TimeoutMS != nil {
		if *request.TimeoutMS <= 0 || *request.TimeoutMS > maximumCommandTimeout.Milliseconds() {
			return command.Request{}, errors.New("timeout_ms must be between 1 and 30000")
		}
		timeout = time.Duration(*request.TimeoutMS) * time.Millisecond
	}

	outputLimit := defaultCommandOutputLimit
	if request.MaxOutputBytesPerStream != nil {
		if *request.MaxOutputBytesPerStream <= 0 || *request.MaxOutputBytesPerStream > maximumCommandOutputLimit {
			return command.Request{}, errors.New("max_output_bytes_per_stream must be between 1 and 1048576")
		}
		outputLimit = *request.MaxOutputBytesPerStream
	}

	return command.Request{
		Argv:                    request.Argv,
		Cwd:                     request.Cwd,
		Env:                     request.Env,
		Timeout:                 timeout,
		MaxOutputBytesPerStream: outputLimit,
	}, nil
}

// decodeRunCommandRequest strictly decodes one size-limited JSON request object.
func decodeRunCommandRequest(response http.ResponseWriter, request *http.Request) (runCommandRequest, bool) {
	request.Body = http.MaxBytesReader(response, request.Body, commandRequestBodyLimit)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()

	var decoded runCommandRequest
	if err := decoder.Decode(&decoded); err != nil {
		writeDecodeError(response, err)
		return runCommandRequest{}, false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeDecodeError(response, err)
		return runCommandRequest{}, false
	}
	return decoded, true
}

// writeDecodeError maps oversized bodies separately from other invalid JSON input.
func writeDecodeError(response http.ResponseWriter, err error) {
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		writeAPIError(response, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds 1048576 bytes")
		return
	}
	writeAPIError(response, http.StatusBadRequest, "invalid_request", "request body must contain one valid JSON object")
}

// writeCommandError maps internal runner failures to stable public HTTP errors.
func writeCommandError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, command.ErrOutputLimitExceeded):
		writeAPIError(response, http.StatusRequestEntityTooLarge, "output_limit_exceeded", "command output exceeded the limit")
	case errors.Is(err, context.DeadlineExceeded):
		writeAPIError(response, http.StatusGatewayTimeout, "command_timeout", "command execution timed out")
	default:
		writeAPIError(response, http.StatusUnprocessableEntity, "command_start_failed", "command could not be started")
	}
}

// writeAPIError sends a public error without exposing internal implementation details.
func writeAPIError(response http.ResponseWriter, statusCode int, code string, message string) {
	writeJSON(response, statusCode, apiErrorResponse{
		Error: apiError{
			Code:    code,
			Message: message,
		},
	})
}
