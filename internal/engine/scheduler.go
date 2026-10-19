package engine

import (
	"encoding/hex"
	"time"

	"github.com/Nohed/GoTorrent/internal/peer"
)

type scheduler struct {
	// Has (bool) peerhocking, perinterested, amInterest, AmChoking
	// Bitfield []byte
	// Inflight map[BlockKey]struct{}
	peers        map[[20]byte]*peer.Peer
	inProgress   map[int]*pieceAssembly
	availability []int
}

type pieceAssembly struct {
	buf       []byte
	received  []bool // per block
	requested []bool // per block: currently asked of some peer
	nReceived int
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
				m.log.Warn("peer event error", "peer", hex.EncodeToString(event.SenderID[:]), "error", event.Err)
			}
			// TOrrent events here
			m.handleEvent(event)

		case <-rechokeTicker.C:
			// m.recalculateChoking()

		case <-optimisticTicker.C:
			// m.rotateOptimisticUnchoke()

		case <-m.ctx.Done(): // cancel signal
			return
		}
	}
}

// Boundend left to 0
func (m *Manager) left() int64 {
	left := int64(m.torrent.Length) - int64(m.downloaded.Load())
	if left < 0 {
		return 0
	}
	return left
}

func (m *Manager) handleEvent(ev Event) {
	s := &m.scheduler

	switch ev.Eve {
	case EventPeerReady:
		m.mu.Lock()
		w := m.wo
	case EventPeerClosed:
	case EventPeerInterested:
	case EventPeerNotInterested:
	case EventPeerChoked:
	case EventPeerUnchoked:
	case EventHave:
	case EventBlockReceived:
	case EventBlockUploaded:
	case EventReadDone:
	case EventBlockRequested:
	case EventBitfield:
	}
}
