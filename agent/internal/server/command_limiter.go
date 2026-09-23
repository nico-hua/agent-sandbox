package server

import "fmt"

const maximumConcurrentCommands = 1024

type commandLimiter struct {
	slots chan struct{}
}

// newCommandLimiter creates a non-blocking limiter within the supported service bounds.
func newCommandLimiter(limit int) (*commandLimiter, error) {
	if limit < 1 || limit > maximumConcurrentCommands {
		return nil, fmt.Errorf("max concurrent commands must be between 1 and %d", maximumConcurrentCommands)
	}
	return &commandLimiter{slots: make(chan struct{}, limit)}, nil
}

// tryAcquire atomically claims an available slot without waiting.
func (limiter *commandLimiter) tryAcquire() bool {
	select {
	case limiter.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// release returns one previously acquired slot to the limiter.
func (limiter *commandLimiter) release() {
	<-limiter.slots
}
