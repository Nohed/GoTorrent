package peer

import (
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
	Conn      Connection
	NumPieces int

	// Only the read loop touches these
	PeerChoking    bool
	PeerInterested bool
	AmInterested   bool
	AmChoking      bool

	Bfield *storage.Bitfield
}

// Count is number of pieces that we can fetch
func NewPeer(conn Connection, count int) *Peer {
	p := &Peer{
		Conn:        conn,
		NumPieces:   count,
		PeerChoking: true,
		AmChoking:   true,
		Bfield:      storage.NewBitfield(count),
	}
	return p
}

// Has reports whether the peer has the piece at the given index.
