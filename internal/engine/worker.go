package engine

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/Nohed/GoTorrent/internal/peer"
)

func run(p *peer.Peer) {

}
func readLoop(p *peer.Peer) error {
	for {
		// A dead peer sends nothing, not even keep-alives
		p.Conn.SetReadDeadline(time.Now().Add(3 * time.Minute))

		msg, err := peer.ReadMessage(p.Conn)
		if err != nil {
			return err
		}
		if msg == nil {
			continue // keep-alive
		}
		if err := handle(p, msg); err != nil {
			return err // protocol violation: drop the peer
		}
	}
}

func writeLoop(p *peer.Peer) {
}

func releaseAll(p *peer.Peer) {
	for k := range p.Inflight {
		p.Mgr.Release(k.Index, k.Begin)
	}
	clear(p.Inflight)
}

// Fills the pipeline to peer with relevant data so we can continue sending asap
func fillPipeline(p *peer.Peer) {
	for !p.PeerChoking && len(p.Inflight) < peer.MaxPipeline {
		idx, begin, length, ok := p.Mgr.NextRequest(p.Has)
		if !ok {
			return
		}
		p.Inflight[peer.BlockKey{Index: idx, Begin: begin}] = struct{}{}
		p.Out <- peer.NewRequest(idx, begin, length)
	}
}

func handle(p *peer.Peer, m *peer.Message) error {
	switch m.ID {
	case peer.MsgChoke:
		p.PeerChoking = true
		releaseAll(p) //We are choked, drop our work so others can do it
	case peer.MsgUnchoke:
		p.PeerChoking = false

	case peer.MsgInterested:
		p.PeerInterested = true

	case peer.MsgNotInterested:
		p.PeerInterested = false

	case peer.MsgHave:
		if len(m.Payload) < 4 {
			return fmt.Errorf("Too short for a have message")
		}
		pieceIndex := int(binary.BigEndian.Uint32(m.Payload[0:4])) //göra detta till funktioner istället, kommer behövas en del
		p.SetPiece(pieceIndex)

	case peer.MsgBitfield:

	case peer.MsgRequest:

	case peer.MsgPiece:

	case peer.MsgCancel:
		// optional: drop a queued upload for this block
	}
	return nil // unknown IDs are ignored
}
