package manager

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
	"time"

	"github.com/Nohed/GoTorrent/internal/engine"
	"github.com/Nohed/GoTorrent/internal/peer"
	"github.com/Nohed/GoTorrent/internal/torrent"
	"github.com/Nohed/GoTorrent/internal/tracker"
)

type Manager struct {
	torrent       *torrent.Torrent
	trackerClient *tracker.Client

	log *slog.Logger

	peerID   [20]byte
	port     uint16
	maxPeers int

	mu          sync.Mutex
	workers     map[string]*engine.Worker
	connections map[string]*peer.Connection
	knownPeers  map[string]struct{}
	pending     int // Handshakes in flight

	events chan Event // For workers to pass data back to manager

	ctx    context.Context    // When the gorotuines should to stop
	cancel context.CancelFunc // send cancel
	wg     sync.WaitGroup     // Wait until routines are finnished

	downloaded atomic.Uint64
	uploaded   atomic.Uint64

	started bool
	stopped bool
}

func NewManager(newTorrent *torrent.Torrent, port uint16, maxConnections int, logger *slog.Logger) (*Manager, error) {

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
		workers:       make(map[string]*engine.Worker),
		connections:   make(map[string]*peer.Connection),
		knownPeers:    make(map[string]struct{}),
		events:        make(chan Event, 256),
	}

	// Generate random 20 byte peer id for ourself
	if _, err := rand.Read(m.peerID[:]); err != nil {
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

	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.started = true

	m.wg.Add(1) // adds to waitgroup
	go func() {
		defer m.wg.Done() // On function exit, tell wg were done
		m.runScheduler()
	}()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runTracker()
	}()

	return nil
}

func (m *Manager) runScheduler() {
	rechokeTicker := time.NewTicker(10 * time.Second)
	optimisticTicker := time.NewTicker(30 * time.Second)
	defer rechokeTicker.Stop()
	defer optimisticTicker.Stop()

	for {
		select {
		case event := <-m.events:
			if event.Err != nil {
				m.log.Warn("peer event error",
					"peer", event.PeerID,
					"error", event.Err)
			}

			// TOrrent events here
			// For example: m.handleEvent(event)

		case <-rechokeTicker.C:
			// m.recalculateChoking()

		case <-optimisticTicker.C:
			// m.rotateOptimisticUnchoke()

		case <-m.ctx.Done(): // cancel signal
			return
		}
	}
}

func (m *Manager) runTracker() {
	for {
		response, err := m.trackerClient.Announce(m.ctx, tracker.AnnounceRequest{
			InfoHash:   m.torrent.InfoHash,
			PeerID:     m.peerID,
			Port:       m.port,
			Uploaded:   int64(m.uploaded.Load()),
			Downloaded: int64(m.downloaded.Load()),
			Left:       int64(m.torrent.Length) - int64(m.downloaded.Load()),
		})

		if err != nil {
			if m.ctx.Err() != nil {
				return
			}
			m.log.Warn("tracker announce failed", "error", err)
			if !m.waitFor(time.Minute) {
				return
			}
			continue
		}

		for _, peer := range response.Peers {
			if m.ctx.Err() != nil {
				return
			}

			addr := net.JoinHostPort(
				peer.IP.String(),
				strconv.Itoa(int(peer.Port)),
			)
			m.connectToPeer(addr)
		}

		interval := time.Duration(response.Interval) * time.Second
		if interval <= 0 {
			interval = time.Minute
		}
		if !m.waitFor(interval) {
			return
		}
	}
}

func (m *Manager) waitFor(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-m.ctx.Done():
		return false
	}
}

func (m *Manager) connectToPeer(addr string) {
	m.mu.Lock()

	if m.stopped || len(m.connections)+m.pending >= m.maxPeers {
		m.mu.Unlock()
		return
	}
	if _, exists := m.knownPeers[addr]; exists {
		m.mu.Unlock()
		return
	}

	m.knownPeers[addr] = struct{}{}
	m.pending++ // reserve the slot

	m.wg.Add(1)
	m.mu.Unlock()

	go func() {
		defer m.wg.Done()

		// Connects and handshakes
		peer, err := peer.Connect(addr, m.torrent.InfoHash, m.peerID)

		m.mu.Lock()
		m.pending-- // Peer is either connected or release below
		if err != nil {
			delete(m.knownPeers, addr)
			m.mu.Unlock()

			if m.ctx.Err() == nil {
				m.log.Debug("peer connection failed", "address", addr, "error", err)
			}
			return
		}

		if m.stopped {
			delete(m.knownPeers, addr)
			m.mu.Unlock()
			_ = peer.Close()
			return
		}

		m.connections[addr] = peer
		m.mu.Unlock()

		// Runs on every exit path: worker finished, errored, or ctx cancelled.
		defer m.removePeer(addr)

		m.runPeer(addr, conn)

	}()
}

// removePeer frees the slot and lets the address be dialed again on a later announce.
func (m *Manager) removePeer(addr string) {
	m.mu.Lock()
	conn := m.connections[addr]
	delete(m.connections, addr)
	delete(m.workers, addr)
	delete(m.knownPeers, addr)
	m.mu.Unlock()

	if conn != nil {
		_ = conn.Close() // safe if the worker already closed it
	}
}

// runPeer blocks until the peer session ends.
func (m *Manager) runPeer(addr string, conn *peer.Connection) {
	// TODO: build the worker, register it under m.mu in m.workers[addr],
	// and run it with m.ctx. Return when it exits.

	// This is called from inside the workers gorotuine
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
