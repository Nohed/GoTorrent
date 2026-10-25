package peer

import (
	"sync/atomic"

	"github.com/Nohed/GoTorrent/internal/storage"
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
	Conn      *Connection
	NumPieces int

	// Only the read loop touches these
	PeerChoking    atomic.Bool
	PeerInterested atomic.Bool
	AmInterested   atomic.Bool
	AmChoking      atomic.Bool

	Bfield *storage.Bitfield
}

// Count is number of pieces that we can fetch
func NewPeer(conn *Connection, count int) *Peer {
	p := &Peer{
		Conn:      conn,
		NumPieces: count,
		Bfield:    storage.NewBitfield(count),
	}
	p.PeerChoking.Store(true)
	p.AmChoking.Store(true)
	p.PeerInterested.Store(false)
	p.AmInterested.Store(false)
	return p
}
