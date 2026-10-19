package engine

/*
RESPSIBILITY:
	Manage the worker's lifecycle and overall resources.
*/

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/Nohed/GoTorrent/internal/peer"
	"github.com/Nohed/GoTorrent/internal/storage"
	"github.com/Nohed/GoTorrent/internal/torrent"
	"github.com/Nohed/GoTorrent/internal/tracker"
)

type Manager struct {
	torrent       *torrent.Torrent
	trackerClient *tracker.Client
	scheduler     *scheduler
	listener      net.Listener
	pieceIO       *storage.PieceIO

	log *slog.Logger

	peerID   [20]byte // own id
	port     uint16   // own listening port
	maxPeers int      // MAx allowed connectiosn

	mu          sync.Mutex
	connections map[[20]byte]*peer.Connection // by remote PeerID
	workers     map[[20]byte]*Worker
	knownPeers  map[string]struct{} // dialed addresses only (outbound dedup)
	pending     int                 // Handshakes in flight

	events chan Event // For workers to pass data back to manager

	ctx    context.Context    // When the gorotuines should to stop
	cancel context.CancelFunc // send cancel
	wg     sync.WaitGroup     // Wait until routines are finnished

	downloaded, uploaded atomic.Uint64 // Curernt staet

	started, stopped bool
}

func NewManager(path string, newTorrent *torrent.Torrent, port uint16, maxConnections int, logger *slog.Logger) (*Manager, error) {

	if newTorrent == nil {
		return nil, fmt.Errorf("torrent is nil")
	}
	if maxConnections <= 0 {
		return nil, fmt.Errorf("max connections must be positive int")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	pieceIO, err := storage.NewPieceIO(path, newTorrent.Length, newTorrent.PieceLength)
	if err != nil {
		return nil, err
	}

	// Create peerID
	m := &Manager{
		torrent:       newTorrent,
		trackerClient: tracker.NewClient(newTorrent.Announce),
		pieceIO:       pieceIO,
		log:           logger,
		port:          port,
		maxPeers:      maxConnections,
		workers:       make(map[[20]byte]*Worker),
		connections:   make(map[[20]byte]*peer.Connection),
		knownPeers:    make(map[string]struct{}),
		events:        make(chan Event, 256),
	}

	// Generate random 20 byte peer id for ourself
	if _, err := rand.Read(m.peerID[:]); err != nil {
		// CLoSE IO FILE?
		return nil, fmt.Errorf("generate peer ID: %w", err)
	}

	return m, nil
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return fmt.Errorf("manager already started")
	}
	if m.stopped {
		return fmt.Errorf("manager already stopped")
	}

	// Accept new connections from peers
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(int(m.port)))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	m.listener = ln
	m.port = uint16(ln.Addr().(*net.TCPAddr).Port) // Update with real port on listenr, not just int

	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.started = true

	m.wg.Add(1)
	go func() { defer m.wg.Done(); m.runScheduler() }()

	m.wg.Add(1)
	go func() { defer m.wg.Done(); m.runTracker() }()

	m.wg.Add(1)
	go func() { defer m.wg.Done(); m.runListener(ln) }()

	m.wg.Add(1)
	go func() { defer m.wg.Done(); m.runIO() }()
	return nil
}

func (m *Manager) runIO() {
	m.pieceIO.Run()
}

// Stop cancels background work, closes peer connections, and waits for manager goroutines to exit.
func (m *Manager) Stop() {
	m.mu.Lock()
	if !m.started || m.stopped {
		m.mu.Unlock()
		return
	}

	m.stopped = true
	m.cancel()

	// Copy peer connections under mutex
	conns := make([]*peer.Connection, 0, len(m.connections))
	for _, c := range m.connections {
		conns = append(conns, c)
	}
	m.mu.Unlock()

	// Close all connections
	for _, c := range conns {
		_ = c.Close() // unblocks workers stuck on reads
	}
	m.wg.Wait() // Wait for routines (the workers) to exit
}
