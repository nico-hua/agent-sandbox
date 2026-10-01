package command

import (
	"errors"
	"io"
	"sync"
)

// ErrInvalidOutputLimit indicates that a request contains a negative output limit.
var ErrInvalidOutputLimit = errors.New("command output limit must not be negative")

// ErrOutputLimitExceeded indicates that stdout or stderr exceeded its allowance.
var ErrOutputLimitExceeded = errors.New("command output limit exceeded")

type outputLimitState struct {
	once     sync.Once
	exceeded chan struct{}
	cancel   func()
}

// markExceeded records the first overflow and cancels command execution once.
func (state *outputLimitState) markExceeded() {
	state.once.Do(func() {
		close(state.exceeded)
		state.cancel()
	})
}

// isExceeded reports whether either output stream has exceeded its allowance.
func (state *outputLimitState) isExceeded() bool {
	select {
	case <-state.exceeded:
		return true
	default:
		return false
	}
}

type limitedWriter struct {
	destination io.Writer
	remaining   int64
	state       *outputLimitState
}

// Write forwards only bytes within the allowance and reports the first excess byte.
func (writer *limitedWriter) Write(data []byte) (int, error) {
	if int64(len(data)) <= writer.remaining {
		written, err := writer.destination.Write(data)
		writer.remaining -= int64(written)
		if written != len(data) && err == nil {
			err = io.ErrShortWrite
		}
		return written, err
	}

	allowed := data[:writer.remaining]
	written, _ := writer.destination.Write(allowed)
	writer.remaining -= int64(written)
	writer.state.markExceeded()
	return written, ErrOutputLimitExceeded
}
