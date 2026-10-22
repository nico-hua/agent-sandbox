package server

import (
	"net"
	"sync"
)

type ownedConn struct {
	net.Conn
	once    sync.Once
	onClose func()
	err     error
}

// Close releases socket ownership exactly once, including concurrent shutdown and normal completion.
func (connection *ownedConn) Close() error {
	connection.once.Do(func() {
		connection.err = connection.Conn.Close()
		if connection.onClose != nil {
			connection.onClose()
		}
	})
	return connection.err
}

// CloseWrite preserves TCP half-close through ownership/capacity wrappers; unsupported transports remain open.
func (connection *ownedConn) CloseWrite() error { return closeWrite(connection.Conn) }

// CloseRead interrupts request upload reads without truncating the response write side.
func (connection *ownedConn) CloseRead() error {
	if half, ok := connection.Conn.(interface{ CloseRead() error }); ok {
		return half.CloseRead()
	}
	return nil
}

// closeWrite propagates EOF when the underlying transport supports a write half-close.
func closeWrite(connection net.Conn) error {
	if half, ok := connection.(interface{ CloseWrite() error }); ok {
		return half.CloseWrite()
	}
	return nil
}

type capacityListener struct {
	net.Listener
	mu          sync.Mutex
	closed      bool
	limit       int
	connections map[*ownedConn]struct{}
}

// Accept counts all sockets, including idle and hijacked ones, and directly closes excess connections.
func (listener *capacityListener) Accept() (net.Conn, error) {
	for {
		connection, err := listener.Listener.Accept()
		if err != nil {
			return nil, err
		}
		listener.mu.Lock()
		if listener.closed || len(listener.connections) >= listener.limit {
			closed := listener.closed
			listener.mu.Unlock()
			_ = connection.Close()
			if closed {
				return nil, net.ErrClosed
			}
			continue
		}
		owned := &ownedConn{Conn: connection}
		owned.onClose = func() { listener.mu.Lock(); delete(listener.connections, owned); listener.mu.Unlock() }
		listener.connections[owned] = struct{}{}
		listener.mu.Unlock()
		return owned, nil
	}
}

// Close atomically ends admission and closes all accepted sockets, including idle headers and tunnels.
func (listener *capacityListener) Close() error {
	listener.mu.Lock()
	if listener.closed {
		listener.mu.Unlock()
		return nil
	}
	listener.closed = true
	connections := make([]*ownedConn, 0, len(listener.connections))
	for connection := range listener.connections {
		connections = append(connections, connection)
	}
	listener.mu.Unlock()
	err := listener.Listener.Close()
	for _, connection := range connections {
		_ = connection.Close()
	}
	return err
}
