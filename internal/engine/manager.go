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
	"github.com/Nohed/GoTorrent/internal/types"
)

type Manager struct {
	torrent *torrent.Torrent

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

	events chan types.Event // For workers to pass data back to manager

	done     chan struct{} // closed once every piece is verified
	doneOnce sync.Once

	ctx    context.Context    // When the gorotuines should to stop
	cancel context.CancelFunc // send cancel
	wg     sync.WaitGroup     // Wait until routines are finnished

	downloaded, uploaded atomic.Uint64 // Current state
	havePieces           atomic.Int32
	leftBytes            atomic.Int64

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

	// Create peerID
	m := &Manager{
		torrent:       newTorrent,
		trackerClient: tracker.NewClient(newTorrent.Announce),
		log:           logger,
		port:          port,
		maxPeers:      maxConnections,
		workers:       make(map[[20]byte]*Worker),
		connections:   make(map[[20]byte]*peer.Connection),
		knownPeers:    make(map[string]struct{}),
		events:        make(chan types.Event, 256),
		done:          make(chan struct{}),
	}

	// Generate random 20 byte peer id for ourself
	if _, err := rand.Read(m.peerID[:]); err != nil {
		// CLoSE IO FILE?
		return nil, fmt.Errorf("generate peer ID: %w", err)
	}
	pio, err := storage.NewPieceIO(path, newTorrent.Length, newTorrent.PieceLength, m.events)
	if err != nil {
		return nil, err
	}

	have, err := pio.VerifyExisting(newTorrent.PieceHashes) // use your real field name
	if err != nil {
		pio.Close()
		return nil, fmt.Errorf("verify existing data: %w", err)
	}
	m.pieceIO = pio //is this used?

	m.scheduler = newScheduler(newTorrent, m.pieceIO)
	m.markHave(have)

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
	ln, err := net.Listen("tcp4", ":"+strconv.Itoa(int(m.port)))
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
	m.pieceIO.Run(m.ctx)
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
	err := m.pieceIO.Close()
	if err != nil {
		m.log.Error("Closing file failed", "error", err)
	}
}

// Done returns a channel that is closed when all pieces have been
// downloaded and verified. Use this to wait for completion.
func (m *Manager) Done() <-chan struct{} {
	return m.done
}

// Wait blocks until the download is complete or the manager is stopped.
func (m *Manager) Wait() {
	select {
	case <-m.done:
	case <-m.ctx.Done():
	}
}

// Stats represents a snapshot of the manager's live transfer and swarm statistics.
type Stats struct {
	Downloaded  uint64
	Uploaded    uint64
	TotalLength int64
	Left        int64
	HavePieces  int
	TotalPieces int
	ActivePeers int
	Done        bool
}

// returns a snapshot of download progress, speed and connected peers.
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	peers := len(m.connections)
	m.mu.Unlock()

	isDone := false
	select {
	case <-m.done:
		isDone = true
	default:
	}

	totalPieces := 0
	if m.torrent != nil {
		totalPieces = len(m.torrent.PieceHashes)
	}

	totalLen := int64(0)
	if m.torrent != nil {
		totalLen = int64(m.torrent.Length)
	}

	return Stats{
		Downloaded:  m.downloaded.Load(),
		Uploaded:    m.uploaded.Load(),
		TotalLength: totalLen,
		Left:        m.leftBytes.Load(),
		HavePieces:  int(m.havePieces.Load()),
		TotalPieces: totalPieces,
		ActivePeers: peers,
		Done:        isDone,
	}
}

func (m *Manager) Torrent() *torrent.Torrent {
	return m.torrent
}
