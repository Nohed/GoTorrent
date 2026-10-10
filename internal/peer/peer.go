package peer

import (
	"net"
	"sync/atomic"

	"github.com/Nohed/GoTorrent/internal/manager"
)

type BlockKey struct {
	Index, Begin int
}

const (
	MaxPipeline = 8  //Max blocks requested at once
	OutQueueLen = 64 //Sending queue
)

//THIS STRUCT IS NOT SHARED BETWEEN ALL WORKERS AND IS UNIQUE FOR EACH ONE

type Peer struct {
	Conn net.Conn
	Mgr  *manager.Manager // shared
	Out  chan *Message    // private outbound queue

	NumPieces int

	// Only the read loop touches these
	PeerChoking    bool
	PeerInterested bool
	AmInterested   bool
	Bitfield       []byte
	Inflight       map[BlockKey]struct{}

	// Touched by the mutliple things, need automic i think
	AmChoking atomic.Bool

	done chan struct{} // closed when the peer shuts down
}

func NewPeer(conn net.Conn, manager *manager.Manager) *Peer {
	count := manager.NumPieces()
	p := &Peer{
		Conn:        conn,
		Mgr:         manager,
		Out:         make(chan *Message, OutQueueLen),
		NumPieces:   count,
		PeerChoking: true,
		Bitfield:    make([]byte, (count+7)/8),
		Inflight:    make(map[BlockKey]struct{}),
		done:        make(chan struct{}),
	}
	p.AmChoking.Store(true)
	return p
}

// Has reports whether the peer has the piece at the given index.
func (p *Peer) Has(index int) bool {
	if index < 0 || index >= p.NumPieces {
		return false
	}
	byteIndex := index / 8
	bitIndex := uint(7 - (index % 8))

	return (p.Bitfield[byteIndex] & (1 << bitIndex)) != 0
}

// SetPiece marks a specific piece as available in the peer's bitfield.
func (p *Peer) SetPiece(index int) {
	if index < 0 || index >= p.NumPieces {
		return
	}
	byteIndex := index / 8
	bitIndex := uint(7 - (index % 8))

	p.Bitfield[byteIndex] |= (1 << bitIndex)
}

// ClearPiece marks a specific piece as missing (if needed).
func (p *Peer) ClearPiece(index int) {
	if index < 0 || index >= p.NumPieces {
		return
	}
	byteIndex := index / 8
	bitIndex := uint(7 - (index % 8))

	p.Bitfield[byteIndex] &^= (1 << bitIndex)
}

// SetBitfield copies an entire raw bitfield message payload into the peer's bitfield.
func (p *Peer) SetBitfield(data []byte) {
	// Copy as much as fits to prevent out-of-bounds panics if payloads vary slightly
	copy(p.Bitfield, data)
}
