package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/egress_proxy/network"
)

// startTestProxy runs the real server on loopback and requires cleanup to finish.
func startTestProxy(t *testing.T, proxy *Proxy) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- Run(ctx, listener, proxy) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-finished:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			_ = listener.Close()
			t.Error("proxy did not stop")
		}
	})
	return "http://" + listener.Addr().String()
}

// mappedDialer checks the production validation chain before routing approved numbers to a local test server.
func mappedDialer(t *testing.T, address string, count *atomic.Int32) *network.Dialer {
	t.Helper()
	resolve := resolverFunc(func(_ context.Context, network, host string) ([]netip.Addr, error) {
		if network != "ip4" || host != "example.com." {
			t.Errorf("unvalidated DNS: %s %s", network, host)
		}
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	})
	dial := func(ctx context.Context, network, target string) (net.Conn, error) {
		count.Add(1)
		if network != "tcp4" || target != "8.8.8.8:80" && target != "8.8.8.8:443" {
			t.Errorf("unvalidated dial: %s %s", network, target)
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp4", address)
	}
	return network.NewDialer(allowedPolicy(t), resolve, dial)
}

// proxyClient creates a real HTTP/1.1 proxy client without ambient environment configuration.
func proxyClient(t *testing.T, address string) *http.Client {
	t.Helper()
	proxyURL, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{Proxy: http.ProxyURL(proxyURL), DisableKeepAlives: true}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// TestHTTPProxyAndLocalHealth verifies absolute-form routing, framing and both directions of hop-header filtering.
func TestHTTPProxyAndLocalHealth(t *testing.T) {
	observed := make(chan *http.Request, 8)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Host != "example.com:80" || r.URL.RequestURI() != "/healthz?token=not-logged" || string(body) != "input" {
			t.Errorf("forwarded request: %s %s %q", r.Host, r.URL, body)
		}
		if r.Header.Get("X-Remove") != "" || r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("X-Keep") != "keep" {
			t.Errorf("forwarded headers: %v", r.Header)
		}
		observed <- r
		w.Header().Set("Connection", "close, X-Upstream-Hop")
		w.Header().Set("X-Upstream-Hop", "remove")
		w.Header().Set("Proxy-Authenticate", "remove")
		w.Header().Set("X-End-To-End", "keep")
		w.WriteHeader(418)
		_, _ = io.WriteString(w, "reply")
	}))
	defer upstream.Close()
	var calls atomic.Int32
	address := startTestProxy(t, NewProxy(mappedDialer(t, upstream.Listener.Addr().String(), &calls)))
	health, err := http.Get(address + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	healthBody, _ := io.ReadAll(health.Body)
	_ = health.Body.Close()
	if health.StatusCode != 200 || strings.TrimSpace(string(healthBody)) != `{"status":"ok"}` || calls.Load() != 0 {
		t.Fatalf("health: %d %s", health.StatusCode, healthBody)
	}
	client := proxyClient(t, address)
	for _, method := range []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"} {
		request, _ := http.NewRequest(method, "http://EXAMPLE.COM/healthz?token=not-logged", strings.NewReader("input"))
		request.Header.Set("Connection", "X-Remove")
		request.Header.Set("X-Remove", "remove")
		request.Header.Set("Proxy-Authorization", "private")
		request.Header.Set("X-Keep", "keep")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != 418 || response.Header.Get("X-Upstream-Hop") != "" || response.Header.Get("Proxy-Authenticate") != "" || response.Header.Get("X-End-To-End") != "keep" {
			t.Fatalf("response: %v %v", response, err)
		}
		if method == "HEAD" && len(data) != 0 || method != "HEAD" && string(data) != "reply" {
			t.Fatalf("body: %s %q", method, data)
		}
		<-observed
	}
	proxyURL, _ := url.Parse(address)
	connection, err := net.DialTimeout("tcp4", proxyURL.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(time.Second))
	_, _ = io.WriteString(connection, "GET http://example.com/healthz?token=not-logged HTTP/1.1\r\nHost: forged.invalid\r\nX-Keep: keep\r\nContent-Length: 5\r\n\r\ninput")
	forged, err := http.ReadResponse(bufio.NewReader(connection), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, forged.Body)
	_ = forged.Body.Close()
	if forged.StatusCode != 418 {
		t.Fatalf("forged Host changed route: %d", forged.StatusCode)
	}
	<-observed
	if calls.Load() != 8 {
		t.Fatalf("expected one fresh guarded connection per request: %d", calls.Load())
	}
}
