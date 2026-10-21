package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Nohed/GoTorrent/internal/engine"
	"github.com/Nohed/GoTorrent/internal/torrent"
)

// TODO: Print progress
func main() {
	var (
		torrentPath = flag.String("torrent", "", "path to the .torrent file")
		outPath     = flag.String("out", "", "output file path")
		port        = flag.Uint("port", 6881, "listening port (0 = random)")
		maxPeers    = flag.Int("peers", 50, "max peer connections")
		verbose     = flag.Bool("v", false, "debug logging")
	)
	flag.Parse()

	if *torrentPath == "" || *outPath == "" {
		fmt.Fprintln(os.Stderr, "usage: gotorrent -torrent file.torrent -out output.bin")
		os.Exit(2)
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	t, err := torrent.ParseFile(*torrentPath)
	if err != nil {
		logger.Error("parse torrent", "error", err)
		os.Exit(1)
	}

	m, err := engine.NewManager(*outPath, t, uint16(*port), *maxPeers, logger)
	if err != nil {
		logger.Error("create manager", "error", err)
		os.Exit(1)
	}

	if err := m.Start(); err != nil {
		logger.Error("start manager", "error", err)
		os.Exit(1)
	}
	defer m.Stop()

	// Ctrl+C / SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	select {
	case <-m.Done():
		logger.Info("download complete")
	case s := <-sig:
		logger.Info("shutting down", "signal", s.String())
	}
}
