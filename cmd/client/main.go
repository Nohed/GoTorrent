package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log/slog"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
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
	blue      = "\033[34m"
	boldBlue  = "\033[1;34m"
	magenta   = "\033[35m"
	boldGreen = "\033[1;32m"
)

func main() {
	var (
		torrentPath = flag.String("torrent", "", "path to the .torrent file (required)")
		outPath     = flag.String("out", "", "output file path (default: file name from torrent)")
		port        = flag.Uint("port", 6881, "listening port (0 = random)")
		maxPeers    = flag.Int("peers", 50, "max peer connections")
		seed        = flag.Bool("seed", true, "keep running and seed after download completes")
		csvPath     = flag.String("csv", "", "path to record time-series transfer metrics (e.g. metrics.csv)")
		debug       = flag.Bool("d", false, "enable debug logging")
		verbose     = flag.Bool("v", false, "enable verbose logging")
	)
	flag.Parse()

	if *torrentPath == "" {
		fmt.Fprintf(os.Stderr, "%sUsage:%s gotorrent -torrent <file.torrent> [-out <output_path>] [-port <port>] [-peers <max_peers>] [-seed] [-csv <metrics.csv>] [-v]\n", bold, reset)
		os.Exit(2)
	}

	var csvWriter *csv.Writer
	if *csvPath != "" {
		f, err := os.Create(*csvPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%sError creating CSV metrics file:%s %v\n", red, reset, err)
			os.Exit(1)
		}
		defer f.Close()
		csvWriter = csv.NewWriter(f)
		defer csvWriter.Flush()

		// Write CSV header
		header := []string{
			"timestamp",
			"elapsed_sec",
			"down_speed_MBps",
			"up_speed_MBps",
			"down_speed_bps",
			"up_speed_bps",
			"downloaded_bytes",
			"uploaded_bytes",
			"have_pieces",
			"total_pieces",
			"peers",
			"progress_pct",
			"state",
		}
		if err := csvWriter.Write(header); err != nil {
			fmt.Fprintf(os.Stderr, "%sError writing CSV header:%s %v\n", red, reset, err)
			os.Exit(1)
		}
		csvWriter.Flush()
	}

	// In non-verbose mode, write only warnings/errors to stderr so they don't corrupt the status line.
	var logHandler slog.Handler
	if *debug {
		logHandler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	} else if *verbose {
		logHandler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	} else {
		logHandler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})
	}
	logger := slog.New(logHandler)

	t, err := torrent.ParseFile(*torrentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError parsing torrent file:%s %v\n", red, reset, err)
		os.Exit(1)
	}

	outputPath := *outPath
	if outputPath == "" {
		if t.Name != "" {
			outputPath = t.Name
		} else {
			outputPath = "download.bin"
		}
	}

	printBanner(t, outputPath, uint16(*port), *maxPeers)

	m, err := engine.NewManager(outputPath, t, uint16(*port), *maxPeers, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError creating engine manager:%s %v\n", red, reset, err)
		os.Exit(1)
	}

	if err := m.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "%sError starting engine manager:%s %v\n", red, reset, err)
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
	} else {
		fmt.Printf("Connecting to swarm...\n")
	}

	// Capture OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	startTime := time.Now()
	prevTime := startTime
	prevDownloaded := initStats.Downloaded
	prevUploaded := initStats.Uploaded

	var (
		downloadCompleted bool
		completedDuration time.Duration
	)

	for {
		select {
		case <-sigChan:
			fmt.Printf("\n\n%sShutting down gracefully (closing peer connections)...%s\n", yellow, reset)
			if *csvPath != "" && csvWriter != nil {
				csvWriter.Flush()
				fmt.Printf("%sMetrics saved to %s%s\n", green, *csvPath, reset)
			}
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

				if !*seed {
					fmt.Printf("%sSeeding disabled by flag. Exiting.%s\n", dim, reset)
					if *csvPath != "" && csvWriter != nil {
						csvWriter.Flush()
						fmt.Printf("%sMetrics saved to %s%s\n", green, *csvPath, reset)
					}
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

			// Calculate ETA
			var eta time.Duration = -1
			if stats.Left > 0 && downSpeed > 0 {
				etaSeconds := float64(stats.Left) / downSpeed
				eta = time.Duration(etaSeconds) * time.Second
			} else if stats.Left == 0 {
				eta = 0
			}

			// Record CSV metrics
			if csvWriter != nil {
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

				state := "downloading"
				if stats.Done || stats.Left == 0 {
					state = "seeding"
				}

				_ = csvWriter.Write([]string{
					now.Format(time.RFC3339),
					fmt.Sprintf("%.2f", now.Sub(startTime).Seconds()),
					fmt.Sprintf("%.3f", downSpeed/(1024*1024)),
					fmt.Sprintf("%.3f", upSpeed/(1024*1024)),
					fmt.Sprintf("%.0f", downSpeed),
					fmt.Sprintf("%.0f", upSpeed),
					strconv.FormatUint(stats.Downloaded, 10),
					strconv.FormatUint(stats.Uploaded, 10),
					strconv.Itoa(stats.HavePieces),
					strconv.Itoa(stats.TotalPieces),
					strconv.Itoa(stats.ActivePeers),
					fmt.Sprintf("%.2f", percent),
					state,
				})
				csvWriter.Flush()
			}

			renderStats(os.Stdout, stats, downSpeed, upSpeed, eta, *verbose, logger)
		}
	}
}

func printBanner(t *torrent.Torrent, outPath string, port uint16, maxPeers int) {
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

func renderStats(w io.Writer, stats engine.Stats, downSpeed, upSpeed float64, eta time.Duration, verbose bool, logger *slog.Logger) {
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

	stateTag := boldGreen + "[DL]" + reset
	if stats.Done || stats.Left == 0 {
		stateTag = boldGreen + "[SEED]" + reset
	}

	if verbose {
		stateName := "Downloading"
		if stats.Done || stats.Left == 0 {
			stateName = "Seeding"
		}
		logger.Info("stats",
			"state", stateName,
			"percent", fmt.Sprintf("%.1f%%", percent),
			"pieces", fmt.Sprintf("%d/%d", stats.HavePieces, stats.TotalPieces),
			"downloaded", formatBytes(float64(stats.Downloaded)),
			"downSpeed", formatRate(downSpeed),
			"upSpeed", formatRate(upSpeed),
			"peers", stats.ActivePeers,
			"eta", formatDuration(eta),
		)
		return
	}

	bar := progressBar(percent, 10)

	// Single-line in-place terminal update:
	// \r returns cursor to column 0, \033[K clears from cursor to end of line
	fmt.Fprintf(w, "\r%s %s %s%5.1f%%%s | %s%s%s/%s | %sDL: %s%s | %sUL: %s%s | %sPeers: %d%s\033[K",
		stateTag,
		bar,
		boldGreen, percent, reset,
		boldBlue, formatBytes(float64(stats.TotalLength-stats.Left)), reset,
		formatBytes(float64(stats.TotalLength)),
		green, formatRate(downSpeed), reset,
		magenta, formatRate(upSpeed), reset,
		yellow, stats.ActivePeers, reset,
	)
}

func progressBar(percent float64, width int) string {
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	completed := int((percent / 100.0) * float64(width))

	var sb strings.Builder
	sb.WriteString("[")
	sb.WriteString(green)
	for i := 0; i < completed; i++ {
		sb.WriteByte('=')
	}
	if completed < width {
		sb.WriteString(boldGreen)
		sb.WriteByte('>')
		sb.WriteString(reset)
		sb.WriteString(dim)
		for i := completed + 1; i < width; i++ {
			sb.WriteByte('-')
		}
		sb.WriteString(reset)
	} else {
		sb.WriteString(reset)
	}
	sb.WriteString("]")
	return sb.String()
}

func formatBytes(b float64) string {
	if b < 0 {
		b = 0
	}
	const (
		kB = 1024
		mB = 1024 * kB
		gB = 1024 * mB
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
	if d < 0 {
		return "--"
	}
	if d == 0 {
		return "0.00s"
	}
	d = d.Round(10 * time.Millisecond) // Round t 2 decimaler
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d.Seconds()

	if h > 0 {
		return fmt.Sprintf("%dh%02dm%05.2fs", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%05.2fs", m, s)
	}
	return fmt.Sprintf("%.2fs", s)
}
