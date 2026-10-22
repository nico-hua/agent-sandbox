package main

import (
	"errors"
	"testing"

	"github.com/nico-hua/agent-sandbox/egress_proxy/network"
)

// TestInvalidPolicyPrecedesListen ensures invalid allowlists fail before any socket operation.
func TestInvalidPolicyPrecedesListen(t *testing.T) {
	for _, arguments := range [][]string{
		{"-listen", "invalid listen address", "-allow-domain", "https://example.com"},
		{"-listen", "invalid listen address", "-allow-domain", "example.com", "-allow-domain", "127.0.0.1"},
	} {
		if err := run(arguments); !errors.Is(err, network.ErrInvalidTarget) {
			t.Fatalf("configuration did not fail before listen: %v", err)
		}
	}
	if err := run([]string{"unexpected"}); err == nil {
		t.Fatal("positional argument accepted")
	}
}
