package server

import (
	"bufio"
	"bytes"
	"net"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync"

	"github.com/nico-hua/agent-sandbox/egress_proxy/network"
)

type responseHeaders struct {
	mu      sync.Mutex
	data    []byte
	limit   int
	stopped bool
}

type observedHeaderConn struct {
	net.Conn
	headers *responseHeaders
}

// Read observes a bounded prefix without altering, reparsing or serializing the transport's byte stream.
func (connection *observedHeaderConn) Read(data []byte) (int, error) {
	n, err := connection.Conn.Read(data)
	connection.headers.mu.Lock()
	if !connection.headers.stopped {
		remaining := connection.headers.limit - len(connection.headers.data)
		connection.headers.data = append(connection.headers.data, data[:min(n, remaining)]...)
	}
	connection.headers.mu.Unlock()
	return n, err
}

// connectionFields recovers hop nominations that Go removes with Connection: close.
// This bounded observation uses textproto only for headers; Transport alone
// handles status validity, transfer framing and bodies. Interim headers count
// against the same observation bound, so a missing final header fails closed.
func (headers *responseHeaders) connectionFields() ([]string, error) {
	headers.mu.Lock()
	headers.stopped = true
	data := headers.data
	headers.mu.Unlock()
	reader := textproto.NewReader(bufio.NewReader(bytes.NewReader(data)))
	for {
		line, err := reader.ReadLine()
		if err != nil {
			return nil, err
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			return nil, network.ErrConnectFailed
		}
		status, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		}
		header, err := reader.ReadMIMEHeader()
		if err != nil {
			return nil, err
		}
		if status < 100 || status >= 200 || status == 101 {
			return header.Values("Connection"), nil
		}
	}
}

// hasToken compares comma-separated header tokens without requiring canonical casing.
func hasToken(values []string, expected string) bool {
	for _, value := range values {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), expected) {
				return true
			}
		}
	}
	return false
}

// stripHopHeaders removes fixed and Connection-nominated hop fields plus proxy credentials.
func stripHopHeaders(header http.Header) {
	for _, value := range header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			header.Del(strings.TrimSpace(token))
		}
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Authentication-Info", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		header.Del(name)
	}
}
