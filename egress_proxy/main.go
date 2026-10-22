package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nico-hua/agent-sandbox/egress_proxy/network"
	"github.com/nico-hua/agent-sandbox/egress_proxy/server"
)

type domains []string

// String describes repeated trusted domain flags without creating dynamic runtime policy.
func (values *domains) String() string { return strings.Join(*values, ",") }

// Set accumulates trusted configuration for all-or-nothing NewPolicy validation.
func (values *domains) Set(value string) error { *values = append(*values, value); return nil }

// run validates all policy before binding and owns signal-driven HTTP/tunnel shutdown.
func run(arguments []string) error {
	flags := flag.NewFlagSet("egress-proxy", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:18084", "trusted local development listen address")
	var allow domains
	flags.Var(&allow, "allow-domain", "exact allowed ASCII domain (repeatable)")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if len(flags.Args()) != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	policy, err := network.NewPolicy(allow)
	if err != nil {
		return err
	}
	proxy := server.NewProxy(network.NewDialer(policy, nil, nil))
	listener, err := net.Listen("tcp4", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return server.Run(ctx, listener, proxy)
}

// main reports only startup/lifecycle errors; request headers, URLs and tunnel contents are never logged.
func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Printf("egress proxy stopped: %v", err)
		os.Exit(1)
	}
}
