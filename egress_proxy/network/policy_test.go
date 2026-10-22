package network

import (
	"errors"
	"strings"
	"testing"
)

// TestPolicyExactImmutableDomains catches suffix matching, implicit allow and mutable input configuration.
func TestPolicyExactImmutableDomains(t *testing.T) {
	input := []string{"EXAMPLE.COM.", "example.com", "api.example.com"}
	policy, err := NewPolicy(input)
	if err != nil {
		t.Fatal(err)
	}
	input[0], input[1] = "attacker.test", "other.test"
	for _, host := range []string{"example.com", "EXAMPLE.COM", "Example.Com.", "api.example.com"} {
		if normalized, err := policy.CheckHost(host); err != nil || normalized != strings.TrimSuffix(strings.ToLower(host), ".") {
			t.Fatalf("host %q: %q %v", host, normalized, err)
		}
	}
	for _, host := range []string{"sub.example.com", "example.com.attacker.test", "attacker.test", "other.test"} {
		if _, err := policy.CheckHost(host); !errors.Is(err, ErrPolicyDenied) {
			t.Fatalf("unexpected allow %q: %v", host, err)
		}
	}
	empty, err := NewPolicy(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := empty.CheckHost("example.com"); !errors.Is(err, ErrPolicyDenied) {
		t.Fatal("empty policy allowed a target")
	}
	var zero Policy
	if _, err := zero.CheckHost("example.com"); !errors.Is(err, ErrPolicyDenied) {
		t.Fatal("zero policy allowed a target")
	}
}

// TestPolicyRejectsMalformedAndNumericHosts applies the same strict validation to config and requests.
func TestPolicyRejectsMalformedAndNumericHosts(t *testing.T) {
	policy, _ := NewPolicy([]string{"example.com"})
	for _, host := range []string{
		"", ".", "example.com..", " example.com", "example.com ", "ex ample.com", "example.com\n", "a\x00.test",
		"https://example.com", "user@example.com", "example.com:443", "example.com/path", "*.example.com",
		"example.com?x=1", "example.com#part", "a_b.test", "-a.test", "a-.test", "a..test", "例子.test", "exämple.test", "K.example.com",
		strings.Repeat("a", 64) + ".test", strings.Repeat("a.", 127) + "a",
		"127.0.0.1", "8.8.8.8.", "::1", "[::1]", "::ffff:8.8.8.8", "[fe80::1%eth0]",
		"2130706433", "127.1", "127.0.1", "0177.0.0.1", "0x7f000001", "0x7f.0x0.0.1", "0xffffffff",
		"1.2.3.999", "9999999999999999999999999999999999999", "0xFFFFFFFFFFFFFFFFFFFFFFFF", "00.00.00.01",
	} {
		t.Run(host, func(t *testing.T) {
			if _, err := policy.CheckHost(host); !errors.Is(err, ErrInvalidTarget) {
				t.Fatalf("invalid target accepted: %q (%v)", host, err)
			}
			if _, err := NewPolicy([]string{host}); !errors.Is(err, ErrInvalidTarget) {
				t.Fatalf("invalid config accepted: %q (%v)", host, err)
			}
		})
	}
}

// TestPolicyDomainAndCapacityBoundaries checks DNS label/name limits and finite trusted configuration.
func TestPolicyDomainAndCapacityBoundaries(t *testing.T) {
	long := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	policy, err := NewPolicy([]string{long + ".", "a", "a-b.test", "123.example", "0xff.example"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{long, "a", "a-b.test", "123.example", "0xff.example"} {
		if _, err := policy.CheckHost(name); err != nil {
			t.Fatalf("valid boundary %q: %v", name, err)
		}
	}
	if _, err := NewPolicy(make([]string, 33)); err == nil {
		t.Fatal("oversized allowlist accepted")
	}
	if _, err := NewPolicy(strings.Split(strings.Repeat("example.com,", 31)+"example.com", ",")); err != nil {
		t.Fatal(err)
	}
}
