package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Nohed/GoTorrent/internal/tracker"
)

const (
	defaultPort      = 8080
	defaultInterval  = 30 * time.Second
	defaultLogLevel  = "info"
	defaultLogFormat = "text"
)

func main() {
	port := flag.Int("port", defaultPort, "Port to listen on")
	interval := flag.Duration("interval", defaultInterval, "Announce interval")
	levelStr := flag.String("log-level", defaultLogLevel, "debug, info, warn or error")
	format := flag.String("log-format", defaultLogFormat, "text or json")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*levelStr)); err != nil {
		log.Fatalf("invalid -log-level %q", *levelStr)
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, opts)
	if *format == "json" {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	logger := slog.New(h)

	srv := tracker.NewServer(*interval, logger)
	logger.Info("tracker starting", "port", *port, "interval", *interval)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), srv.Handler()); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
