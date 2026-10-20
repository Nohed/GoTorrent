package engine

import (
	"context"
	"encoding/hex"
	"net"
	"time"

	"github.com/Nohed/GoTorrent/internal/peer"
)

// runPeer blocks until the peer session ends.
func (m *Manager) runPeer(peerID [20]byte, conn *peer.Connection) {
	// TODO: build the worker, register it under m.mu in m.workers[addr],
	// and run it with m.ctx. Return when it exits.

	// This is called from inside the workers gorotuine

	// "m.workers" MAP CONTIANING WORKERS

	// m.events = event channel to pass messages up to worker
	worker := NewWorker(m.ctx, conn, m.events, len(m.torrent.PieceHashes))

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.workers[peerID] = worker
	m.mu.Unlock()

	worker.Run()
}

// removePeer frees the slot and lets the address be dialed again on a later announce.
func (m *Manager) removePeer(conn *peer.Connection, dialAddr string) {
	m.mu.Lock()
	// Only evict our own entry, never a newer connection with the same PeerID.
	if m.connections[conn.PeerID] == conn {
		delete(m.connections, conn.PeerID)
		delete(m.workers, conn.PeerID)
	}
	if dialAddr != "" {
		delete(m.knownPeers, dialAddr)
	}
	m.mu.Unlock()

	_ = conn.Close() // safe if the worker already closed it
}

func (m *Manager) acceptPeer(connection net.Conn) {
	m.mu.Lock()
	if m.stopped || len(m.connections)+m.pending >= m.maxPeers {
		m.mu.Unlock()
		_ = connection.Close()
		return
	}

	m.pending++ // reserve the slot
	m.wg.Add(1)
	m.mu.Unlock()

	go func() {
		defer m.wg.Done()

		stop := context.AfterFunc(m.ctx, func() { _ = connection.Close() })
		peerConn, err := peer.Accept(connection, m.torrent.InfoHash, m.peerID)
		stop()

		m.mu.Lock()
		m.pending--
		if err != nil {
			m.mu.Unlock()
			m.log.Debug("inbound handshake failed", "remote", connection.RemoteAddr(), "error", err)
			return
		}
		if m.stopped {
			m.mu.Unlock()
			_ = peerConn.Close()
			return
		}

		if _, dup := m.connections[peerConn.PeerID]; dup {
			m.mu.Unlock()
			_ = peerConn.Close()
			return
		}

		m.connections[peerConn.PeerID] = peerConn
		m.mu.Unlock()

		defer m.removePeer(peerConn, "")
		m.log.Info("Accepted connection from peer", "ID", hex.EncodeToString(peerConn.PeerID[:]), "Current connections", len(m.connections))
		m.runPeer(peerConn.PeerID, peerConn)
	}()
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
		peerConn, err := peer.Connect(addr, m.torrent.InfoHash, m.peerID)

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
			_ = peerConn.Close()
			return
		}
		// Can only check peerID after handshake
		if _, dup := m.connections[peerConn.PeerID]; dup {
			// Already connected to this peer
			delete(m.knownPeers, addr)
			m.mu.Unlock()
			_ = peerConn.Close()
			return
		}
		m.connections[peerConn.PeerID] = peerConn
		m.mu.Unlock()

		// Runs on every exit path: worker finished, errored, or ctx cancelled.
		defer m.removePeer(peerConn, addr)

		m.log.Info("connected to peer", "peer", hex.EncodeToString(peerConn.PeerID[:]), "addr", addr)
		m.runPeer(peerConn.PeerID, peerConn)

	}()
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
