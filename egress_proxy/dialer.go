package egress

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"time"
)

// ErrDNSFailed identifies failed, empty or excessive DNS results without resolver diagnostics.
var ErrDNSFailed = errors.New("egress DNS lookup failed")

// ErrConnectFailed identifies unsuccessful TCP attempts without leaking transport diagnostics.
var ErrConnectFailed = errors.New("egress connection failed")

// ErrCanceled is also recognizable with errors.Is(err, context.Canceled).
var ErrCanceled = context.Canceled

// ErrTimeout is also recognizable with errors.Is(err, context.DeadlineExceeded).
var ErrTimeout = context.DeadlineExceeded

// Resolver supplies per-connection IPv4 answers; injected implementations must honor context.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

// DialFunc connects an already validated numeric address, respecting the supplied context.
type DialFunc func(context.Context, string, string) (net.Conn, error)

// Dialer pins each connection to one fully validated DNS answer set. Instances
// may be shared; injected dependencies must support concurrent calls as well.
type Dialer struct {
	policy   Policy
	resolver Resolver
	dial     DialFunc
	budget   time.Duration
}

// NewDialer snapshots immutable policy and assembles trusted injectable dependencies.
// Nil dependencies use net.DefaultResolver and net.Dialer; nil policy denies
// all targets. Construction does not resolve, connect, or consult proxy settings.
func NewDialer(policy *Policy, resolver Resolver, dial DialFunc) *Dialer {
	var fixed Policy
	if policy != nil {
		// Copy the value so replacing the caller's *Policy cannot change this
		// dialer's allowlist. The unexported backing map has no mutation path.
		fixed = *policy
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	return &Dialer{policy: fixed, resolver: resolver, dial: dial, budget: 5 * time.Second}
}

// DialContext accepts tcp/tcp4 and canonical host:80 or host:443, returning a
// caller-owned connection. DNS and every attempt share one five-second budget,
// bounded further by the caller. Only checked numeric addresses reach DialFunc.
func (dialer *Dialer) DialContext(ctx context.Context, network, target string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, dialer.budget)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if network != "tcp" && network != "tcp4" {
		return nil, ErrInvalidTarget
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil || (port != "80" && port != "443") || target != net.JoinHostPort(host, port) {
		return nil, ErrInvalidTarget
	}
	host, err = dialer.policy.CheckHost(host)
	if err != nil {
		return nil, err
	}
	// A root dot prevents search-domain expansion. Request only A records and
	// never pass a domain to the transport, including during fallback attempts.
	answers, err := dialer.resolver.LookupNetIP(ctx, "ip4", host+".")
	if err != nil {
		return nil, operationError(ctx, err, ErrDNSFailed)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(answers) == 0 || len(answers) > 16 {
		return nil, ErrDNSFailed
	}
	validated := make([]netip.Addr, 0, len(answers))
	for _, answer := range answers {
		address, err := publicIPv4(answer)
		if err != nil {
			return nil, err
		}
		// Copy address values: never revisit mutable resolver storage after validation.
		validated = append(validated, address)
	}
	for _, address := range validated {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		connection, err := dialer.dial(ctx, "tcp4", net.JoinHostPort(address.String(), port))
		if connection != nil {
			if err == nil && ctx.Err() == nil {
				return connection, nil
			}
			// Some injected transports return both connection and error, or finish
			// after cancellation. Anything not delivered remains our responsibility.
			_ = connection.Close()
		}
		failure := operationError(ctx, err, ErrConnectFailed)
		if errors.Is(failure, ErrCanceled) || errors.Is(failure, ErrTimeout) {
			return nil, failure
		}
	}
	return nil, ErrConnectFailed
}

// operationError preserves cancellation/timeout identity and otherwise exposes only a stable stage error.
func operationError(ctx context.Context, err, fallback error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return ErrCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrTimeout
	}
	var timed interface{ Timeout() bool }
	if errors.As(err, &timed) && timed.Timeout() {
		return ErrTimeout
	}
	return fallback
}
