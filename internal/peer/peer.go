package peer

import (
	"fmt"
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
	Conn Connection
	Out  chan *Message // private outbound queue

	NumPieces int

	// Only the read loop touches these
	PeerChoking    bool
	PeerInterested bool
	AmInterested   bool
	AmChoking      bool

	Bitfield []byte
	Inflight map[BlockKey]struct{}
}

// Count is number of pieces that we can fetch
func NewPeer(conn Connection, count int) *Peer {
	p := &Peer{
		Conn:        conn,
		Out:         make(chan *Message, OutQueueLen),
		NumPieces:   count,
		PeerChoking: true,
		AmChoking:   true,
		Bitfield:    make([]byte, (count+7)/8),
		Inflight:    make(map[BlockKey]struct{}),
	}
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
func (p *Peer) SetPiece(index int) error {
	if !p.setBit(index, true) {
		return fmt.Errorf("have index %d out of range", index)
	}
	return nil
}

func (p *Peer) ClearPiece(index int) {
	p.setBit(index, false)
}

// SetBitfield copies an entire raw bitfield message payload into the peer's bitfield.
func (p *Peer) SetBitfield(data []byte) error {
	if len(data) != len(p.Bitfield) {
		return fmt.Errorf("bitfield length %d, want %d", len(data), len(p.Bitfield))
	}
	copy(p.Bitfield, data)
	return nil
}

//helper

// It reports false if index is out of range.
func (p *Peer) setBit(index int, on bool) bool {
	if index < 0 || index >= p.NumPieces {
		return false
	}
	byteIndex := index / 8
	mask := byte(0x80) >> (index % 8)

	if on {
		p.Bitfield[byteIndex] |= mask // force the bit to 1
	} else {
		p.Bitfield[byteIndex] &^= mask // force the bit to 0
	}
	return true
}
