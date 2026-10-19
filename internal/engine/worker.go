package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/Nohed/GoTorrent/internal/peer"
)

// TODO: should add tracking ( bad data, not sending and so on)
type Worker struct {
	Peer              *peer.Peer
	OutGoingEvents    chan Event         //reference to managers channel
	IncommingCommands chan Command       //own listening channel
	ctx               context.Context    // When the gorotuines should to stop
	cancel            context.CancelFunc // send cancel
}

// TODO: We are passing a mutex as a copy, look at later
func NewWorker(parent context.Context, conn peer.Connection, events chan Event, numPieces int) *Worker {
	// CRETE OWN CHANNEL
	ctx, cancel := context.WithCancel(parent)
	return &Worker{
		Peer:              peer.NewPeer(conn, numPieces),
		IncommingCommands: make(chan Command, 256),
		OutGoingEvents:    events,
		ctx:               ctx,
		cancel:            cancel,
	}
}

// Run blocks until the session ends and returns the error that ended it.
func (w *Worker) Run() error {
	defer w.cancel()

	errc := make(chan error, 2) // buffered so neither loop can leak
	go func() { errc <- w.readLoop() }()
	go func() { errc <- w.writeLoop() }()

	if err := w.emit(Event{Eve: EventPeerReady}); err != nil {
		return err
	}
	err := <-errc           // first loop to stop ends the session
	w.cancel()              // tell the write loop to stop
	_ = w.Peer.Conn.Close() // unblock a read stuck on the socket
	<-errc                  // wait for the other loop

	_ = w.emit(Event{Eve: EventPeerClosed, Err: err})
	return err
}

// called by manager to send command to peer!!!
func (w *Worker) Send(cmd Command) bool {
	select {
	case w.IncommingCommands <- cmd:
		return true
	default:
		return false
	}
}

func (w *Worker) readLoop() error {
	p := w.Peer
	for {
		// A dead peer sends nothing, not even keep-alives
		p.Conn.Conn.SetReadDeadline(time.Now().Add(3 * time.Minute))

		msg, err := peer.ReadMessage(p.Conn.Conn)
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

func (w *Worker) emit(e Event) error {
	e.SenderID = w.Peer.Conn.PeerID
	select {
	case w.OutGoingEvents <- e:
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	}
}

func (w *Worker) writeLoop() error {
	// TODO: send a keep-alive every ~2 minutes when idle.
	for {
		select {
		case cmd := <-w.IncommingCommands:
			if err := w.execute(cmd); err != nil {
				return err
			}
		case <-w.ctx.Done():
			return nil
		}
	}
}

func (w *Worker) handle(m *peer.Message) error {
	switch m.ID {
	case peer.MsgChoke:
		return w.emit(Event{Eve: EventPeerChoked})

	case peer.MsgUnchoke:
		return w.emit(Event{Eve: EventPeerUnchoked})

	case peer.MsgInterested:
		return w.emit(Event{Eve: EventPeerInterested})

	case peer.MsgNotInterested:
		return w.emit(Event{Eve: EventPeerNotInterested})

	case peer.MsgHave:
		index, err := m.ParseHave()
		if err != nil {
			return err
		}
		return w.emit(Event{Eve: EventHave, PieceIndex: index})

	case peer.MsgBitfield:
		// The manager validates the length against the piece count.
		return w.emit(Event{Eve: EventBitfield, Data: m.Payload})

	case peer.MsgRequest:
		index, begin, length, err := m.ParseRequest()
		if err != nil {
			return err
		}
		return w.emit(Event{
			Eve:        EventBlockRequested,
			PieceIndex: index,
			Begin:      begin,
			Length:     length,
		})

	case peer.MsgPiece:
		index, begin, data, err := m.ParsePiece()
		if err != nil {
			return err
		}
		return w.emit(Event{
			Eve:        EventBlockReceived,
			PieceIndex: index,
			Begin:      begin,
			Data:       data,
		})

	case peer.MsgCancel:
		// Ignored for now: uploads are written immediately, nothing to cancel.
	}
	return nil // unknown IDs are ignored
}

func (w *Worker) createMessageOfCommand(cmd Command) (*peer.Message, error) {
	var msg *peer.Message

	switch cmd.Cmd {
	case commandInterested:
		msg = peer.NewInterested()
	case commandNotInterested:
		msg = peer.NewNotInterested()
	case commandChoke:
		msg = peer.NewChoke()
	case commandUnchoke:
		msg = peer.NewUnchoke()
	case commandHave:
		msg = peer.NewHave(cmd.PieceIndex)
	case commandRequestBlock:
		msg = peer.NewRequest(cmd.PieceIndex, cmd.Begin, cmd.Length)
	case commandCancelBlock:
		msg = peer.NewCancel(cmd.PieceIndex, cmd.Begin, cmd.Length)
	case commandSend:
		msg = peer.NewPiece(cmd.PieceIndex, cmd.Begin, cmd.Data)
	case commandSendBitfield:
		msg = peer.NewBitfield(cmd.Data)
	default:
		return nil, fmt.Errorf("unknown command %d", cmd.Cmd)
	}
	return msg, nil
}

func (w *Worker) execute(cmd Command) error {
	msg, err := w.createMessageOfCommand(cmd)
	if err != nil {
		return err
	}
	if err := w.Peer.Conn.Send(msg); err != nil {
		return err
	}

	if cmd.Cmd == commandSend {
		return w.emit(Event{
			Eve:        EventBlockUploaded,
			PieceIndex: cmd.PieceIndex,
			Begin:      cmd.Begin,
			Length:     len(cmd.Data),
		})
	}
	return nil
}

/*
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
}*/
