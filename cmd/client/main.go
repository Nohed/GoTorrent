package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Nohed/GoTorrent/internal/engine"
	"github.com/Nohed/GoTorrent/internal/torrent"
)

// ANSI color escape sequences
const (
	reset     = "\033[0m"
	bold      = "\033[1m"
	dim       = "\033[2m"
	red       = "\033[31m"
	green     = "\033[32m"
	yellow    = "\033[33m"
	boldBlue  = "\033[1;34m"
	magenta   = "\033[35m"
	white     = "\033[37m"
	boldWhite = "\033[1;37m"
	boldGreen = "\033[1;32m"
)
const (
	defaultOutputNmae = "download"
)

func main() {
	var (
		torrentPath = flag.String("torrent", "", "path to the .torrent file (required)")
		outPath     = flag.String("out", "", "output file path (default: file name from torrent)")
		port        = flag.Uint("port", 6881, "listening port (0 = random)")
		maxPeers    = flag.Int("peers", 50, "max peer connections")
		seed        = flag.Bool("seed", true, "keep running and seed after download completes")
		debug       = flag.Bool("d", false, "enable debug logging")
	)
	flag.Parse()

	if *torrentPath == "" {
		fmt.Fprintf(os.Stderr, "%sUsage:%s gotorrent -torrent <file.torrent> [-out <output_path>] [-port <port>] [-peers <max_peers>] [-seed] [-d]\n", bold, reset)
		os.Exit(2)
	}

	logger := setupLogger(*debug)

	t, err := torrent.ParseFile(*torrentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError parsing torrent file:%s %v\n", red, reset, err)
		os.Exit(1)
	}

	outputPath := *outPath
	if outputPath == "" {
		// Get output file name from torrent if its not empty
		if t.Name != "" {
			outputPath = t.Name
		} else {
			// Default
			outputPath = defaultOutputNmae
		}
	}

	printSetup(t, outputPath, uint16(*port), *maxPeers)

	m, err := engine.NewManager(outputPath, t, uint16(*port), *maxPeers, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError creating engine:%s %v\n", red, reset, err)
		os.Exit(1)
	}

	if err := m.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "%sError starting engine:%s %v\n", red, reset, err)
		os.Exit(1)
	}
	defer m.Stop()

	// Initial piece stats check
	initStats := m.Stats()
	if initStats.HavePieces > 0 {
		fmt.Printf("Existing data verified: %s%d/%d%s pieces completed (%s%.1f%%%s)\n\n",
			green, initStats.HavePieces, initStats.TotalPieces, reset,
			green, float64(initStats.HavePieces)/float64(initStats.TotalPieces)*100, reset,
		)
	}

	fmt.Printf("Connecting to swarm\n")
	// Run main loop until download finishes or OS signal
	runLoop(m, *seed, initStats)
}

func runLoop(m *engine.Manager, seed bool, initStats engine.Stats) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(500 * time.Millisecond) // controls how often to update stats
	defer ticker.Stop()

	startTime := time.Now()
	prevTime := startTime
	prevDownloaded := initStats.Downloaded
	prevUploaded := initStats.Uploaded

	var downloadCompleted bool
	var completedDuration time.Duration

	for {
		select {
		case <-sigChan:
			fmt.Printf("\n\n%sShutting down (closing peer connections)...%s\n", yellow, reset)
			return

		case <-m.Done():
			if !downloadCompleted {
				downloadCompleted = true
				completedDuration = time.Since(startTime)
				finalStats := m.Stats()

				avgSpeed := float64(0)
				if completedDuration.Seconds() > 0 {
					avgSpeed = float64(finalStats.Downloaded) / completedDuration.Seconds()
				}

				fmt.Printf("\n\n%s[DONE]%s Download completed in %s! Total downloaded: %s (avg %s)\n",
					boldGreen, reset,
					formatDuration(completedDuration),
					formatBytes(float64(finalStats.Downloaded)),
					formatRate(avgSpeed),
				)

				if !seed {
					fmt.Printf("%sSeeding disabled by flag. Exiting.%s\n", dim, reset)
					return
				}
				fmt.Printf("%sNow seeding to peers (press Ctrl+C to stop)...%s\n", green, reset)
			}

		case now := <-ticker.C:
			stats := m.Stats()
			elapsed := now.Sub(prevTime).Seconds()

			downSpeed := float64(0)
			upSpeed := float64(0)
			if elapsed > 0 {
				if stats.Downloaded >= prevDownloaded {
					downSpeed = float64(stats.Downloaded-prevDownloaded) / elapsed
				}
				if stats.Uploaded >= prevUploaded {
					upSpeed = float64(stats.Uploaded-prevUploaded) / elapsed
				}
			}

			prevDownloaded = stats.Downloaded
			prevUploaded = stats.Uploaded
			prevTime = now

			renderStats(os.Stdout, stats, downSpeed, upSpeed)
		}
	}
}

func setupLogger(debug bool) *slog.Logger {
	var handler slog.Handler
	if debug {
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	} else {
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})
	}
	return slog.New(handler)
}

func printSetup(t *torrent.Torrent, outPath string, port uint16, maxPeers int) {
	fmt.Println("====================================================")
	fmt.Printf("%s                  GoTorrent Client                  %s\n", bold, reset)
	fmt.Println("====================================================")
	fmt.Printf("  %sName:%s           %s\n", bold, reset, t.Name)
	fmt.Printf("  %sTracker:%s        %s\n\n", bold, reset, t.Announce)
	fmt.Printf("  %sSize:%s           %s\n", bold, reset, formatBytes(float64(t.Length)))
	fmt.Printf("  %sPieces:%s         %d x %s\n", bold, reset, len(t.PieceHashes), formatBytes(float64(t.PieceLength)))
	fmt.Printf("  %sOutput:%s         %s\n", bold, reset, outPath)
	fmt.Printf("  %sListening Port:%s %d\n", bold, reset, port)
	fmt.Printf("  %sMax Peers:%s      %d\n", bold, reset, maxPeers)
	fmt.Println("====================================================")
}

func renderStats(w io.Writer, stats engine.Stats, downSpeed, upSpeed float64) {
	var percent float64

	if stats.TotalLength > 0 {
		completedBytes := stats.TotalLength - stats.Left
		if completedBytes < 0 {
			completedBytes = 0
		}
		percent = (float64(completedBytes) / float64(stats.TotalLength)) * 100
	} else if stats.TotalPieces > 0 {
		percent = (float64(stats.HavePieces) / float64(stats.TotalPieces)) * 100
	}

	stateTag := boldGreen + "[Downloading]" + reset
	if stats.Done || stats.Left == 0 {
		stateTag = boldGreen + "[SEEDING]   " + reset // keep spaces in string to make it as long as "[Downloading]"
	}

	bar := progressBar(percent, 10)

	// UPDate row:
	// \r returns cursor to column 0,
	// \033[K clears from cursor to end of line
	fmt.Fprintf(w, "\r%s %s %5.1f%%%s | %s%s%s/%s | %sDL: %s%s | %sUL: %s%s | %sPeers: %d%s\033[K",
		stateTag,
		bar,
		percent, reset,
		boldBlue, formatBytes(float64(stats.TotalLength-stats.Left)), reset,
		formatBytes(float64(stats.TotalLength)),
		green, formatRate(downSpeed), reset,
		magenta, formatRate(upSpeed), reset,
		yellow, stats.ActivePeers, reset,
	)
}

// renders an ASCII progress bar.
func progressBar(percent float64, width int) string {
	// Clamp percentage between 0 and 100
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	completed := int((percent / 100.0) * float64(width))

	var sb strings.Builder
	sb.WriteString("[")

	// completed portion
	for i := 0; i < completed; i++ {
		sb.WriteByte('#')
	}

	// remaining empty slots
	for i := completed; i < width; i++ {
		sb.WriteByte('.')
	}

	sb.WriteString("]")
	return sb.String()
}

func formatBytes(b float64) string {
	const (
		kB = 1024
		mB = kB * 1024
		gB = mB * 1024
	)
	switch {
	case b >= gB:
		return fmt.Sprintf("%.2f GB", b/float64(gB))
	case b >= mB:
		return fmt.Sprintf("%.2f MB", b/float64(mB))
	case b >= kB:
		return fmt.Sprintf("%.1f KB", b/float64(kB))
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}

func formatRate(bytesPerSec float64) string {
	return formatBytes(bytesPerSec) + "/s"
}

func formatDuration(d time.Duration) string {
	return d.Round(10 * time.Millisecond).String()
}
