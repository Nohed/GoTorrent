package engine

import (
	"log"
	"net"
	"time"

	"github.com/Nohed/GoTorrent/internal/manager"
	"github.com/Nohed/GoTorrent/internal/peer"
)

// TODO: should add tracking ( bad data, not sending and so on)
type Worker struct {
	Peer *peer.Peer
	Mgr  *manager.Manager
}

func NewWorker(conn net.Conn, mgr *manager.Manager) *Worker {
	return &Worker{
		Peer: peer.NewPeer(conn, mgr.NumPieces()),
		Mgr:  mgr,
	}
}

func (w *Worker) Run() error {

}
func (w *Worker) readLoop() error {
	p := w.Peer
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
		if err := w.handle(msg); err != nil {
			return err // protocol violation: drop the peer
		}
	}
}

func (w *Worker) writeLoop() {

}

func (w *Worker) releaseAll() {
	p := w.Peer
	for k := range p.Inflight {
		w.Mgr.Release(k.Index, k.Begin)
	}
	clear(p.Inflight)
}

// fillPipeline keeps up to MaxPipeline block requests outstanding.
func (w *Worker) fillPipeline() {
	p := w.Peer
	for !p.PeerChoking && len(p.Inflight) < peer.MaxPipeline {
		idx, begin, length, ok := w.Mgr.NextRequest(p.Has)
		if !ok {
			return
		}
		p.Inflight[peer.BlockKey{Index: idx, Begin: begin}] = struct{}{}
		p.Out <- peer.NewRequest(idx, begin, length)
	}
}

func (w *Worker) updateInterest() {
	p := w.Peer
	want := w.Mgr.Needs(p.Has)

	if want && !p.AmInterested {
		p.AmInterested = true
		p.Out <- peer.NewInterested()
	} else if !want && p.AmInterested {
		p.AmInterested = false
		p.Out <- peer.NewNotInterested()
	}
}
func (w *Worker) handleBlock(index, begin int, data []byte) {
	p := w.Peer
	key := peer.BlockKey{Index: index, Begin: begin}
	if _, asked := p.Inflight[key]; !asked {
		return // we never requested this block, ignore it
	}
	delete(p.Inflight, key)

	if _, err := w.Mgr.AddBlock(index, begin, data); err != nil {
		log.Printf("piece %d: %v", index, err)
	}
	w.fillPipeline() // a slot freed up, request another block
}

func (w *Worker) handleRequest(index, begin, length int) {
	p := w.Peer
	if p.AmChoking.Load() || !p.PeerInterested || !w.Mgr.Have(index) {
		return
	}
	go func() {
		data, err := w.Mgr.ReadBlock(index, begin, length)
		if err != nil {
			return // bad range from a hostile peer
		}
		select {
		case p.Out <- peer.NewPiece(index, begin, data):
		case <-p.Done:
		default: // queue full, drop it
		}
	}()
}

func (w *Worker) handle(m *peer.Message) error {
	p := w.Peer
	switch m.ID {
	case peer.MsgChoke:
		p.PeerChoking = true
		w.releaseAll() // our requests are dead, give blocks back

	case peer.MsgUnchoke:
		p.PeerChoking = false
		w.fillPipeline()

	case peer.MsgInterested:
		p.PeerInterested = true

	case peer.MsgNotInterested:
		p.PeerInterested = false

	case peer.MsgHave:
		index, err := m.ParseHave()
		if err != nil {
			return err
		}
		if err := p.SetPiece(index); err != nil {
			return err
		}
		w.updateInterest()
		w.fillPipeline()

	case peer.MsgBitfield:
		if err := p.SetBitfield(m.Payload); err != nil {
			return err
		}
		w.updateInterest()

	case peer.MsgRequest:
		index, begin, length, err := m.ParseRequest()
		if err != nil {
			return err
		}
		w.handleRequest(index, begin, length)

	case peer.MsgPiece:
		index, begin, data, err := m.ParsePiece()
		if err != nil {
			return err
		}
		w.handleBlock(index, begin, data)

	case peer.MsgCancel:
	}
	return nil // unknown IDs are ignored
}
