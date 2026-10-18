package engine

import (
	"math/rand"
	"sort"

	"github.com/Nohed/GoTorrent/internal/peer"
)

const (
	regularSlots     = 3
	optimisticSlots  = 1
	optimisticRotate = 3
)

// Types

// PeerStats holds per-round traffic counters only. Choke/interest state
// lives on peer.Peer, which is the single source of truth for it.
type PeerStats struct {
	BytesUploaded   int
	BytesDownloaded int
}

type Decision struct {
	Unchoke []*peer.Peer
	Choke   []*peer.Peer
}

type Choker struct {
	peers      map[*peer.Peer]*PeerStats
	optimistic *peer.Peer
	round      int
	seeding    bool
}

// Constructor
func NewChoker() *Choker {
	return &Choker{
		peers: make(map[*peer.Peer]*PeerStats),
	}
}

// Public peer management
func (c *Choker) AddPeer(p *peer.Peer) {
	if _, ok := c.peers[p]; !ok {
		c.peers[p] = &PeerStats{}
	}
}

func (c *Choker) RemovePeer(p *peer.Peer) {
	delete(c.peers, p)
	if c.optimistic == p {
		c.optimistic = nil
	}
}

func (c *Choker) SetSeeding(seeding bool) {
	c.seeding = seeding
}

func (c *Choker) IncrementUpload(p *peer.Peer, bytes int) {
	if s, ok := c.peers[p]; ok {
		s.BytesUploaded += bytes
	}
}

func (c *Choker) IncrementDownload(p *peer.Peer, bytes int) {
	if s, ok := c.peers[p]; ok {
		s.BytesDownloaded += bytes
	}
}

// Main operation/purpose
func (c *Choker) RecalculateChoking() *Decision {
	c.round++

	candidates := c.interestedPeers()
	c.rankByRate(candidates)

	selected := c.selectRegular(candidates)
	c.updateOptimistic(candidates, selected)

	d := c.diffAgainstCurrent(selected)
	c.resetCounters()

	return d
}

// interestedPeers returns peers that are interested in US (PeerInterested),
// since only they can benefit from being unchoked.
func (c *Choker) interestedPeers() []*peer.Peer {
	candidates := make([]*peer.Peer, 0, len(c.peers))
	for p := range c.peers {
		if p.PeerInterested {
			candidates = append(candidates, p)
		}
	}
	return candidates
}

func (c *Choker) rankByRate(candidates []*peer.Peer) {
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	sort.SliceStable(candidates, func(i, j int) bool {
		return c.rate(candidates[i]) > c.rate(candidates[j])
	})
}

func (c *Choker) rate(p *peer.Peer) int {
	if c.seeding {
		return c.peers[p].BytesUploaded
	}
	return c.peers[p].BytesDownloaded
}

func (c *Choker) selectRegular(candidates []*peer.Peer) map[*peer.Peer]bool {
	n := regularSlots
	if len(candidates) < n {
		n = len(candidates)
	}

	selected := make(map[*peer.Peer]bool, regularSlots+optimisticSlots)
	for _, p := range candidates[:n] {
		selected[p] = true
	}
	return selected
}

// 6. Optimistic unchoking
func (c *Choker) updateOptimistic(candidates []*peer.Peer, selected map[*peer.Peer]bool) {
	if c.round%optimisticRotate == 1 || !c.optimisticStillValid(selected) {
		c.optimistic = c.pickOptimistic(candidates, selected)
	}

	if c.optimistic != nil {
		selected[c.optimistic] = true
	}
}

func (c *Choker) optimisticStillValid(selected map[*peer.Peer]bool) bool {
	if c.optimistic == nil {
		return false
	}
	if _, ok := c.peers[c.optimistic]; !ok {
		return false
	}
	return c.optimistic.PeerInterested && !selected[c.optimistic]
}

func (c *Choker) pickOptimistic(candidates []*peer.Peer, selected map[*peer.Peer]bool) *peer.Peer {
	var choked, any []*peer.Peer

	for _, p := range candidates {
		if selected[p] {
			continue
		}

		any = append(any, p)

		if p.AmChoking {
			choked = append(choked, p)
		}
	}

	pool := choked
	if len(pool) == 0 {
		pool = any
	}

	if len(pool) == 0 {
		return nil
	}

	return pool[rand.Intn(len(pool))]
}

// diffAgainstCurrent compares the desired unchoked set with each peer's
// actual AmChoking state, updates it, and returns only the changes.
func (c *Choker) diffAgainstCurrent(selected map[*peer.Peer]bool) *Decision {
	d := &Decision{}

	for p := range c.peers {
		choking := p.AmChoking

		switch {
		case selected[p] && choking:
			p.AmChoking = false
			d.Unchoke = append(d.Unchoke, p)

		case !selected[p] && !choking:
			p.AmChoking = true
			d.Choke = append(d.Choke, p)
		}
	}

	return d
}

func (c *Choker) resetCounters() {
	for _, s := range c.peers {
		s.BytesUploaded = 0
		s.BytesDownloaded = 0
	}
}
