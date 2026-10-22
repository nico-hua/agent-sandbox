// Package network validates exact domain policy and pins IPv4 TCP connections
// to checked DNS answers. It does not configure sandbox networking.
package network

import (
	"errors"
	"net/netip"
	"strings"
)

// ErrInvalidTarget identifies malformed hosts, ports, networks or policy entries.
var ErrInvalidTarget = errors.New("invalid egress target")

// ErrPolicyDenied identifies a domain or resolved address forbidden by policy.
var ErrPolicyDenied = errors.New("egress policy denied")

// Policy is an immutable exact-domain allowlist; its zero value denies all hosts.
type Policy struct{ allowed map[string]struct{} }

// NewPolicy validates and copies at most 32 entries; normalized duplicates coalesce.
// The bound counts supplied entries, including duplicates. Invalid entries never
// produce a partial policy; callers must finish modifying input before this call.
func NewPolicy(domains []string) (*Policy, error) {
	if len(domains) > 32 {
		return nil, ErrInvalidTarget
	}
	allowed := make(map[string]struct{}, len(domains))
	for _, domain := range domains {
		host, err := normalizeHost(domain)
		if err != nil {
			return nil, err
		}
		allowed[host] = struct{}{}
	}
	return &Policy{allowed: allowed}, nil
}

// CheckHost returns the canonical allowed DNS host or a stable validation/denial error.
func (policy *Policy) CheckHost(host string) (string, error) {
	normalized, err := normalizeHost(host)
	if err != nil {
		return "", err
	}
	if policy != nil {
		if _, allowed := policy.allowed[normalized]; allowed {
			return normalized, nil
		}
	}
	return "", ErrPolicyDenied
}

// normalizeHost accepts ASCII DNS labels, strips one root dot and rejects numeric address syntax.
func normalizeHost(host string) (string, error) {
	// Reject original Unicode before case folding (e.g. Kelvin sign folds to ASCII k).
	for _, character := range []byte(host) {
		if character >= 128 {
			return "", ErrInvalidTarget
		}
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if len(host) == 0 || len(host) > 253 {
		return "", ErrInvalidTarget
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return "", ErrInvalidTarget
	}
	labels := strings.Split(host, ".")
	allNumeric := true
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", ErrInvalidTarget
		}
		for _, character := range []byte(label) {
			if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-') {
				return "", ErrInvalidTarget
			}
		}
		allNumeric = allNumeric && numericLabel(label)
	}
	// Conservatively reject single integers and dotted decimal/octal/0x forms,
	// including overflowing values. They must never reach an address parser.
	if allNumeric {
		return "", ErrInvalidTarget
	}
	return host, nil
}

// numericLabel recognizes decimal/octal digits and explicit hexadecimal forms without integer overflow.
func numericLabel(label string) bool {
	hexadecimal := strings.HasPrefix(label, "0x")
	if hexadecimal {
		label = label[2:]
	}
	for _, character := range []byte(label) {
		if !(character >= '0' && character <= '9' || hexadecimal && character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

// denyIPv4 covers special-purpose ranges from the IANA IPv4 registry and
// multicast (RFC 1112): https://www.iana.org/assignments/iana-ipv4-special-registry
// Entire prefixes are conservatively denied, even where IANA lists narrower
// global exceptions (such as 192.0.0.9). Passing is neither a safety guarantee
// nor a reachability guarantee, and this is not a complete host firewall.
var denyIPv4 = [...]netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
}

// publicIPv4 rejects invalid, zoned, non-IPv4 and special-use addresses after unmapping.
func publicIPv4(address netip.Addr) (netip.Addr, error) {
	if !address.IsValid() || address.Zone() != "" {
		return netip.Addr{}, ErrPolicyDenied
	}
	address = address.Unmap()
	if !address.Is4() {
		return netip.Addr{}, ErrPolicyDenied
	}
	for _, prefix := range denyIPv4 {
		if prefix.Contains(address) {
			return netip.Addr{}, ErrPolicyDenied
		}
	}
	return address, nil
}
