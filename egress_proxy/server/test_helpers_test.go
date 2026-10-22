package server

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/nico-hua/agent-sandbox/egress_proxy/network"
)

type resolverFunc func(context.Context, string, string) ([]netip.Addr, error)

// LookupNetIP adapts a deterministic resolver without external DNS.
func (resolve resolverFunc) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return resolve(ctx, network, host)
}

type trackedConn struct {
	net.Conn
	closed atomic.Bool
}

// Close records connection ownership cleanup while closing the real in-memory pipe.
func (connection *trackedConn) Close() error {
	connection.closed.Store(true)
	return connection.Conn.Close()
}

// testPipe creates a connection and peer with unconditional test-owned cleanup.
func testPipe(t *testing.T) (*trackedConn, net.Conn) {
	t.Helper()
	local, peer := net.Pipe()
	connection := &trackedConn{Conn: local}
	t.Cleanup(func() { _ = connection.Close(); _ = peer.Close() })
	return connection, peer
}

// allowedPolicy constructs the trusted allowlist for tests of the real guarded chain.
func allowedPolicy(t *testing.T) *network.Policy {
	t.Helper()
	policy, err := network.NewPolicy([]string{"example.com"})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
