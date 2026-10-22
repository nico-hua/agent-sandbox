package server

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

type boundedReader struct {
	source     io.Reader
	remaining  int64
	limitError error
}

// Read uses one-byte lookahead to reject overflow while forwarding at most the configured raw body allowance.
func (reader *boundedReader) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if int64(len(data)) > reader.remaining+1 {
		data = data[:reader.remaining+1]
	}
	n, err := reader.source.Read(data)
	if int64(n) > reader.remaining {
		allowed := int(reader.remaining)
		reader.remaining = 0
		return allowed, reader.limitError
	}
	reader.remaining -= int64(n)
	return n, err
}

type boundedBody struct {
	source     io.ReadCloser
	remaining  int64
	limitError error
	exceeded   atomic.Bool
	mu         sync.Mutex
	closing    bool
	reads      sync.WaitGroup
	once       sync.Once
}

// Read bounds the sole Transport upload reader and records overflow for pre/post-commit error handling.
func (body *boundedBody) Read(data []byte) (int, error) {
	body.mu.Lock()
	if body.closing {
		body.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	body.reads.Add(1)
	body.mu.Unlock()
	defer body.reads.Done()
	reader := boundedReader{source: body.source, remaining: body.remaining, limitError: body.limitError}
	n, err := reader.Read(data)
	body.remaining = reader.remaining
	if errors.Is(err, body.limitError) {
		body.exceeded.Store(true)
	}
	return n, err
}

// Close settles all started upload reads before the handler can release its active slot.
func (body *boundedBody) Close() error {
	body.once.Do(func() {
		body.mu.Lock()
		body.closing = true
		body.mu.Unlock()
		if body.source != nil {
			_ = body.source.Close()
		}
		body.reads.Wait()
	})
	return nil
}
