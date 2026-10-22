package egress

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
func allowedPolicy(t *testing.T) *Policy {
	t.Helper()
	policy, err := NewPolicy([]string{"example.com"})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

// TestDialerFreezesPolicyValue prevents caller-side policy replacement from changing an existing dialer.
func TestDialerFreezesPolicyValue(t *testing.T) {
	policy := allowedPolicy(t)
	replacement, err := NewPolicy([]string{"attacker.test"})
	if err != nil {
		t.Fatal(err)
	}
	connection, _ := testPipe(t)
	lookups := 0
	resolve := resolverFunc(func(_ context.Context, _, host string) ([]netip.Addr, error) {
		lookups++
		if host != "example.com." {
			t.Errorf("policy changed target: %s", host)
		}
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	})
	dialer := NewDialer(policy, resolve, func(context.Context, string, string) (net.Conn, error) { return connection, nil })
	*policy = *replacement
	if got, err := dialer.DialContext(context.Background(), "tcp", "example.com:80"); got != connection || err != nil {
		t.Fatalf("original policy changed: %v %v", got, err)
	}
	if _, err := dialer.DialContext(context.Background(), "tcp", "attacker.test:80"); !errors.Is(err, ErrPolicyDenied) || lookups != 1 {
		t.Fatalf("replacement policy applied: %d %v", lookups, err)
	}
}

// TestDialRejectsBeforeDNS catches policy/target checks accidentally occurring after side effects.
func TestDialRejectsBeforeDNS(t *testing.T) {
	resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		t.Error("forbidden target performed DNS")
		return nil, nil
	})
	dial := func(context.Context, string, string) (net.Conn, error) {
		t.Error("forbidden target dialed")
		return nil, nil
	}
	dialer := NewDialer(allowedPolicy(t), resolve, dial)
	for _, test := range []struct {
		network, target string
		want            error
	}{
		{"tcp", "other.test:80", ErrPolicyDenied}, {"tcp", "sub.example.com:443", ErrPolicyDenied},
		{"udp", "example.com:80", ErrInvalidTarget}, {"tcp6", "example.com:443", ErrInvalidTarget},
		{"TCP", "example.com:80", ErrInvalidTarget}, {"", "example.com:80", ErrInvalidTarget},
		{"tcp", "example.com", ErrInvalidTarget}, {"tcp", "example.com:http", ErrInvalidTarget},
		{"tcp", "example.com:81", ErrInvalidTarget}, {"tcp", "example.com:0", ErrInvalidTarget},
		{"tcp", "example.com:080", ErrInvalidTarget}, {"tcp", "example.com:+80", ErrInvalidTarget},
		{"tcp", "example.com:65536", ErrInvalidTarget}, {"tcp", "example.com:", ErrInvalidTarget},
		{"tcp", "[example.com]:80", ErrInvalidTarget}, {"tcp", ":80", ErrInvalidTarget},
		{"tcp", "[::1]:443", ErrInvalidTarget}, {"tcp", "127.1:80", ErrInvalidTarget},
		{"tcp", "https://example.com:443", ErrInvalidTarget}, {"tcp", "user@example.com:80", ErrInvalidTarget},
	} {
		if conn, err := dialer.DialContext(context.Background(), test.network, test.target); conn != nil || !errors.Is(err, test.want) {
			t.Fatalf("%s %s: %v %v", test.network, test.target, conn, err)
		}
	}
	if _, err := NewDialer(nil, resolve, dial).DialContext(context.Background(), "tcp", "example.com:80"); !errors.Is(err, ErrPolicyDenied) {
		t.Fatal("nil policy allowed a target")
	}
}

// TestDialPinsAbsoluteDNSAndFallback prevents a second resolution or an unverified domain fallback.
func TestDialPinsAbsoluteDNSAndFallback(t *testing.T) {
	connection, peer := testPipe(t)
	lookups, calls := 0, []string{}
	var deadline time.Time
	resolve := resolverFunc(func(ctx context.Context, network, host string) ([]netip.Addr, error) {
		lookups++
		if network != "ip4" || host != "example.com." {
			t.Fatalf("DNS input: %s %s", network, host)
		}
		deadline, _ = ctx.Deadline()
		if lookups > 1 {
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		}
		return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("::ffff:1.1.1.1")}, nil
	})
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		if next, _ := ctx.Deadline(); next != deadline {
			t.Fatal("retry changed total budget")
		}
		if network != "tcp4" {
			t.Fatalf("network: %s", network)
		}
		calls = append(calls, address)
		if len(calls) == 1 {
			return nil, errors.New("private refused error")
		}
		return connection, nil
	}
	dialer := NewDialer(allowedPolicy(t), resolve, dial)
	before := time.Now()
	got, err := dialer.DialContext(context.Background(), "tcp", "EXAMPLE.COM.:443")
	if err != nil || got != connection || connection.closed.Load() {
		t.Fatalf("result: %v %v", got, err)
	}
	if lookups != 1 || len(calls) != 2 || calls[0] != "8.8.8.8:443" || calls[1] != "1.1.1.1:443" {
		t.Fatalf("lookup/dial: %d %v", lookups, calls)
	}
	if deadline.Before(before.Add(5*time.Second)) || deadline.After(time.Now().Add(5*time.Second)) {
		t.Fatalf("default budget: %v", deadline)
	}
	_ = got.SetDeadline(time.Now().Add(time.Second))
	_ = peer.SetDeadline(time.Now().Add(time.Second))
	wrote := make(chan error, 1)
	go func() { _, err := got.Write([]byte("ok")); wrote <- err }()
	data := make([]byte, 2)
	if _, err := io.ReadFull(peer, data); err != nil || string(data) != "ok" {
		t.Fatalf("delivered connection unusable: %q %v", data, err)
	}
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
	_ = got.Close()
	if !connection.closed.Load() {
		t.Fatal("caller could not close connection")
	}
	if _, err := dialer.DialContext(context.Background(), "tcp4", "example.com:80"); !errors.Is(err, ErrPolicyDenied) || lookups != 2 || len(calls) != 2 {
		t.Fatalf("new call reused DNS or allowed rebind: %d %v", lookups, err)
	}
}

// TestDialValidatesAllAddresses denies every special range and any poisoned mixed answer before dialing.
func TestDialValidatesAllAddresses(t *testing.T) {
	for _, value := range []string{
		"0.0.0.0", "0.255.255.255", "10.0.0.0", "10.255.255.255", "100.64.0.0", "100.127.255.255",
		"127.0.0.0", "127.255.255.255", "169.254.0.0", "169.254.255.255", "169.254.169.254", "100.100.100.200",
		"172.16.0.0", "172.31.255.255", "192.0.0.0", "192.0.0.255", "192.0.0.9", "192.0.2.0", "192.0.2.255",
		"192.88.99.0", "192.88.99.255", "192.168.0.0", "192.168.255.255", "198.18.0.0", "198.19.255.255",
		"198.51.100.0", "198.51.100.255", "203.0.113.0", "203.0.113.255", "224.0.0.0", "239.255.255.255", "240.0.0.0", "255.255.255.255",
		"::ffff:10.0.0.1", "::ffff:127.0.0.1", "::ffff:169.254.169.254", "::", "::1", "2606:4700:4700::1111", "fe80::1%eth0", "::ffff:8.8.8.8%eth0", "invalid",
	} {
		t.Run(value, func(t *testing.T) {
			address, _ := netip.ParseAddr(value)
			resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
				return []netip.Addr{netip.MustParseAddr("8.8.8.8"), address}, nil
			})
			dial := func(context.Context, string, string) (net.Conn, error) {
				t.Error("mixed forbidden answer dialed")
				return nil, nil
			}
			if conn, err := NewDialer(allowedPolicy(t), resolve, dial).DialContext(context.Background(), "tcp", "example.com:80"); conn != nil || !errors.Is(err, ErrPolicyDenied) {
				t.Fatalf("forbidden %s: %v %v", value, conn, err)
			}
		})
	}
}

// TestDialDNSFailures classifies empty, failed and oversized DNS without attempting connections.
func TestDialDNSFailures(t *testing.T) {
	for _, test := range []struct {
		addresses []netip.Addr
		err       error
	}{
		{nil, nil}, {nil, errors.New("private resolver error")}, {make([]netip.Addr, 17), nil},
		{[]netip.Addr{netip.MustParseAddr("8.8.8.8")}, errors.New("partial DNS failure")},
	} {
		resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) { return test.addresses, test.err })
		dial := func(context.Context, string, string) (net.Conn, error) {
			t.Error("invalid DNS dialed")
			return nil, nil
		}
		if _, err := NewDialer(allowedPolicy(t), resolve, dial).DialContext(context.Background(), "tcp", "example.com:80"); !errors.Is(err, ErrDNSFailed) {
			t.Fatalf("DNS classification: %v", err)
		}
	}
}

// TestDialAnswerBoundaryAndCopiedValues checks the final answer is validated and the dial set cannot change later.
func TestDialAnswerBoundaryAndCopiedValues(t *testing.T) {
	for _, poisoned := range []bool{false, true} {
		connection, _ := testPipe(t)
		answers := make([]netip.Addr, 16)
		for index := range answers {
			answers[index] = netip.MustParseAddr("8.8.8.8")
		}
		if poisoned {
			answers[15] = netip.MustParseAddr("10.0.0.1")
		}
		resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) { return answers, nil })
		calls := 0
		dial := func(context.Context, string, string) (net.Conn, error) { calls++; return connection, nil }
		got, err := NewDialer(allowedPolicy(t), resolve, dial).DialContext(context.Background(), "tcp", "example.com:443")
		if poisoned && (got != nil || calls != 0 || !errors.Is(err, ErrPolicyDenied)) {
			t.Fatalf("last answer bypass: %v %d %v", got, calls, err)
		}
		if !poisoned && (err != nil || got != connection || calls != 1) {
			t.Fatalf("16 valid results: %v %d %v", got, calls, err)
		}
	}
	answers := []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("1.1.1.1")}
	resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) { return answers, nil })
	connection, _ := testPipe(t)
	calls := 0
	dial := func(_ context.Context, _, address string) (net.Conn, error) {
		calls++
		if calls == 1 {
			answers[1] = netip.MustParseAddr("127.0.0.1")
			return nil, errors.New("refused")
		}
		if address != "1.1.1.1:80" {
			t.Fatalf("resolver storage reused after validation: %s", address)
		}
		return connection, nil
	}
	if _, err := NewDialer(allowedPolicy(t), resolve, dial).DialContext(context.Background(), "tcp", "example.com:80"); err != nil {
		t.Fatal(err)
	}
}

// TestDialAllowedPrefixNeighbours catches overbroad masking while keeping all test traffic in memory.
func TestDialAllowedPrefixNeighbours(t *testing.T) {
	for _, value := range []string{"1.1.1.1", "8.8.8.8", "100.63.255.255", "100.128.0.0", "172.15.255.255", "172.32.0.0", "192.0.1.1", "192.88.98.255", "192.88.100.0", "198.17.255.255", "198.20.0.0", "223.255.255.255"} {
		t.Run(value, func(t *testing.T) {
			connection, _ := testPipe(t)
			resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
				return []netip.Addr{netip.MustParseAddr(value)}, nil
			})
			dial := func(_ context.Context, network, target string) (net.Conn, error) {
				if network != "tcp4" || target != value+":80" {
					t.Fatalf("numeric target: %s %s", network, target)
				}
				return connection, nil
			}
			if got, err := NewDialer(allowedPolicy(t), resolve, dial).DialContext(context.Background(), "tcp4", "example.com:80"); err != nil || got != connection {
				t.Fatalf("allowed neighbour: %v %v", got, err)
			}
		})
	}
}

// TestCallerCancellationWhileWaiting uses explicit entry signals to cancel in-flight DNS or dialing.
func TestCallerCancellationWhileWaiting(t *testing.T) {
	for _, phase := range []string{"dns", "dial"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{})
			connection, _ := testPipe(t)
			resolve := resolverFunc(func(ctx context.Context, _, _ string) ([]netip.Addr, error) {
				if phase == "dns" {
					close(entered)
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
			})
			dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
				close(entered)
				<-ctx.Done()
				return connection, ctx.Err()
			}
			finished := make(chan error, 1)
			go func() {
				_, err := NewDialer(allowedPolicy(t), resolve, dial).DialContext(ctx, "tcp", "example.com:80")
				finished <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("dependency not entered")
			}
			cancel()
			select {
			case err := <-finished:
				if !errors.Is(err, ErrCanceled) {
					t.Fatalf("cancel: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("canceled call did not settle")
			}
			if phase == "dial" && !connection.closed.Load() {
				t.Fatal("canceled connection leaked")
			}
		})
	}
}

// TestTimeoutClassificationClosesLateResult distinguishes transport timeouts and closes late successful sockets.
func TestTimeoutClassificationClosesLateResult(t *testing.T) {
	connection, _ := testPipe(t)
	resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	})
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) { <-ctx.Done(); return connection, nil }
	dialer := NewDialer(allowedPolicy(t), resolve, dial)
	dialer.budget = 20 * time.Millisecond
	if got, err := dialer.DialContext(context.Background(), "tcp", "example.com:80"); got != nil || !errors.Is(err, ErrTimeout) || !connection.closed.Load() {
		t.Fatalf("late timeout result: %v %v", got, err)
	}
	resolve = resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return nil, &net.DNSError{Err: "private timeout", IsTimeout: true}
	})
	if _, err := NewDialer(allowedPolicy(t), resolve, nil).DialContext(context.Background(), "tcp", "example.com:80"); !errors.Is(err, ErrTimeout) {
		t.Fatalf("DNS timeout: %v", err)
	}
}

// TestDialCancellationAndLateConnections closes connections that arrive after cancellation or with errors.
func TestDialCancellationAndLateConnections(t *testing.T) {
	for _, phase := range []string{"before", "dns", "dial", "late-success", "late-error"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			connection, _ := testPipe(t)
			resolve := resolverFunc(func(ctx context.Context, _, _ string) ([]netip.Addr, error) {
				if phase == "before" {
					t.Error("pre-canceled lookup")
				}
				if phase == "dns" {
					cancel()
					return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
				}
				return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
			})
			dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
				if phase == "before" || phase == "dns" {
					t.Error("canceled lookup dialed")
				}
				cancel()
				if phase == "dial" {
					return nil, ctx.Err()
				}
				if phase == "late-error" {
					return connection, errors.New("private error with connection")
				}
				return connection, nil
			}
			if phase == "before" {
				cancel()
			}
			if got, err := NewDialer(allowedPolicy(t), resolve, dial).DialContext(ctx, "tcp", "example.com:80"); got != nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel: %v %v", got, err)
			}
			if (phase == "late-success" || phase == "late-error") && !connection.closed.Load() {
				t.Fatal("late connection leaked")
			}
		})
	}
}

// TestDialBudgetAndRetryDeadline verifies DNS and all attempts receive one deadline, not a new budget.
func TestDialBudgetAndRetryDeadline(t *testing.T) {
	for _, phase := range []string{"dns", "retry"} {
		t.Run(phase, func(t *testing.T) {
			var shared time.Time
			attempts := 0
			resolve := resolverFunc(func(ctx context.Context, _, _ string) ([]netip.Addr, error) {
				shared, _ = ctx.Deadline()
				if phase == "dns" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("1.1.1.1")}, nil
			})
			dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
				attempts++
				if deadline, _ := ctx.Deadline(); deadline != shared {
					t.Fatal("budget restarted")
				}
				if attempts == 1 {
					return nil, errors.New("refused")
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			dialer := NewDialer(allowedPolicy(t), resolve, dial)
			dialer.budget = 50 * time.Millisecond
			if _, err := dialer.DialContext(context.Background(), "tcp", "example.com:80"); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("timeout: %v", err)
			}
			if phase == "dns" && attempts != 0 || phase == "retry" && attempts != 2 {
				t.Fatalf("attempts: %d", attempts)
			}
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	parentDeadline, _ := ctx.Deadline()
	resolve := resolverFunc(func(ctx context.Context, _, _ string) ([]netip.Addr, error) {
		if deadline, _ := ctx.Deadline(); deadline != parentDeadline {
			t.Fatal("parent deadline not preserved")
		}
		return nil, errors.New("not found")
	})
	if _, err := NewDialer(allowedPolicy(t), resolve, nil).DialContext(ctx, "tcp", "example.com:80"); !errors.Is(err, ErrDNSFailed) {
		t.Fatalf("parent-bounded DNS: %v", err)
	}
}

// TestDialClosesFailedFallbackAndAnomalousResults ensures no undelivered connection escapes ownership cleanup.
func TestDialClosesFailedFallbackAndAnomalousResults(t *testing.T) {
	first, _ := testPipe(t)
	second, _ := testPipe(t)
	resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("1.1.1.1")}, nil
	})
	attempts := 0
	dial := func(context.Context, string, string) (net.Conn, error) {
		attempts++
		if attempts == 1 {
			return first, errors.New("private connection error")
		}
		if !first.closed.Load() {
			t.Error("fallback began before closing failed connection")
		}
		return second, errors.New("another private error")
	}
	if conn, err := NewDialer(allowedPolicy(t), resolve, dial).DialContext(context.Background(), "tcp", "example.com:443"); conn != nil || !errors.Is(err, ErrConnectFailed) || err.Error() != "egress connection failed" {
		t.Fatalf("failure classification: %v %v", conn, err)
	}
	if !first.closed.Load() || !second.closed.Load() || attempts != 2 {
		t.Fatal("failed connection leaked")
	}
	dial = func(context.Context, string, string) (net.Conn, error) { return nil, nil }
	if conn, err := NewDialer(allowedPolicy(t), resolve, dial).DialContext(context.Background(), "tcp", "example.com:80"); conn != nil || !errors.Is(err, ErrConnectFailed) {
		t.Fatalf("nil-nil result: %v %v", conn, err)
	}
}

// TestConcurrentDialerReuse exercises immutable policy and per-call DNS/dial state with independent pipes.
func TestConcurrentDialerReuse(t *testing.T) {
	var lookups, attempts atomic.Int32
	resolve := resolverFunc(func(_ context.Context, network, host string) ([]netip.Addr, error) {
		lookups.Add(1)
		if network != "ip4" || host != "example.com." {
			t.Errorf("DNS input: %s %s", network, host)
		}
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	})
	dial := func(_ context.Context, network, address string) (net.Conn, error) {
		attempts.Add(1)
		if network != "tcp4" || address != "8.8.8.8:80" {
			t.Errorf("unvalidated dial: %s %s", network, address)
		}
		connection, peer := net.Pipe()
		_ = peer.Close()
		return connection, nil
	}
	dialer := NewDialer(allowedPolicy(t), resolve, dial)
	var workers sync.WaitGroup
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			connection, err := dialer.DialContext(context.Background(), "tcp", "example.com:80")
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
		}()
	}
	workers.Wait()
	if lookups.Load() != 32 || attempts.Load() != 32 {
		t.Fatalf("per-call resolution: %d %d", lookups.Load(), attempts.Load())
	}
}
