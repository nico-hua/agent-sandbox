package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nico-hua/agent-sandbox/sandbox_agent/command"
)

type streamChunk struct {
	name string
	data []byte
}

type streamOutcome struct {
	result command.Result
	err    error
}

type streamOutputData struct {
	DataBase64 string `json:"data_base64"`
}

type streamCompletion struct {
	ExitCode int `json:"exit_code"`
}

type streamOutputWriter struct {
	ctx    context.Context
	name   string
	chunks chan<- streamChunk
}

// Write copies one output block into a zero-buffer stream channel for backpressure.
func (writer streamOutputWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	chunk := streamChunk{name: writer.name, data: append([]byte(nil), data...)}
	select {
	case writer.chunks <- chunk:
		return len(data), nil
	case <-writer.ctx.Done():
		return 0, writer.ctx.Err()
	}
}

// newCommandStreamHandler streams separate command output frames and one terminal event.
func newCommandStreamHandler(runCommand CommandRunner, limiter *commandLimiter) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		decoded, commandRequest, ok := prepareCommandExecution(response, request, limiter)
		if !ok {
			return
		}
		defer limiter.release()

		controller := http.NewResponseController(response)
		if err := controller.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			writeAPIError(response, http.StatusInternalServerError, "stream_unavailable", "command stream is unavailable")
			return
		}
		response.Header().Set("Content-Type", "text/event-stream")
		response.Header().Set("Cache-Control", "no-cache")
		response.Header().Set("X-Accel-Buffering", "no")
		if err := controller.Flush(); err != nil {
			return
		}

		// Bound blocked output writers as well as the runner when the client reads slowly.
		executionContext, cancel := context.WithTimeout(request.Context(), commandRequest.Timeout)
		chunks := make(chan streamChunk)
		outcomes := make(chan streamOutcome, 1)
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			result, err := runCommand(
				executionContext,
				commandRequest,
				strings.NewReader(decoded.Stdin),
				streamOutputWriter{executionContext, "stdout", chunks},
				streamOutputWriter{executionContext, "stderr", chunks},
			)
			outcomes <- streamOutcome{result: result, err: err}
		}()
		// Keep the shared command slot until the runner has finished cancellation and cleanup.
		defer func() {
			cancel()
			<-finished
		}()

		for {
			select {
			case <-request.Context().Done():
				return
			case chunk := <-chunks:
				data := streamOutputData{DataBase64: base64.StdEncoding.EncodeToString(chunk.data)}
				if err := writeStreamEvent(response, controller, chunk.name, data); err != nil {
					return
				}
			case outcome := <-outcomes:
				if request.Context().Err() != nil {
					return
				}
				if outcome.err != nil {
					_, public := commandFailure(outcome.err)
					_ = writeStreamEvent(response, controller, "failure", apiErrorResponse{Error: public})
					return
				}
				_ = writeStreamEvent(response, controller, "complete", streamCompletion{ExitCode: outcome.result.ExitCode})
				return
			}
		}
	})
}

// writeStreamEvent writes and flushes one JSON-encoded SSE frame from the handler goroutine.
func writeStreamEvent(response http.ResponseWriter, controller *http.ResponseController, name string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(response, "event: %s\ndata: %s\n\n", name, data); err != nil {
		return err
	}
	return controller.Flush()
}
