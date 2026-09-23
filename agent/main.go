package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/nico-hua/agent-sandbox/agent/internal/server"
)

const defaultListenAddress = "127.0.0.1:8080"

// main runs the agent and converts startup or serving failures into a non-zero exit status.
func main() {
	if err := run(); err != nil {
		log.Printf("agent failed: %v", err)
		os.Exit(1)
	}
}

// run assembles the listener, signal context, handler, and HTTP server lifecycle.
func run() error {
	listenAddress := flag.String("listen", defaultListenAddress, "HTTP listen address")
	flag.Parse()

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", *listenAddress, err)
	}
	defer listener.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("agent listening on %s", listener.Addr())
	if err := server.Run(ctx, listener, server.NewHandler()); err != nil {
		return err
	}
	log.Printf("agent stopped")
	return nil
}
