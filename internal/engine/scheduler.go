package engine

import (
	"encoding/hex"
	"time"

	"github.com/Nohed/GoTorrent/internal/peer"
	"github.com/Nohed/GoTorrent/internal/storage"
	"github.com/Nohed/GoTorrent/internal/torrent"
	"github.com/Nohed/GoTorrent/internal/types"
)

const (
	blockSize          = 16 * 1024
	maxRequestLen      = 32 * 1024 // largest block a peer may ask us for
	maxInflightPerPeer = 8         // outstanding block requests per peer
	maxInProgress      = 16        // pieces being assembled at once
	maxQueuedReads     = 16        // upload requests waiting on a disk read, per peer
	requestTimeout     = 30 * time.Second
)

type blockKey struct{ piece, begin int }

type pendingRequest struct {
	peer          [20]byte
	begin, length int
}

type scheduler struct {
	torrent      *torrent.Torrent
	pieceIO      *storage.PieceIO
	choker       *Choker
	numPieces    int
	peers        map[[20]byte]*peer.Peer
	have         []bool                 // pieces we have completed
	inProgress   map[int]*storage.Piece // pieces currently being assembled
	availability []int                  // how many peers have each piece
}

// TODO: NEED TO HANDLE SEEDING
func newScheduler(t *torrent.Torrent, pio *storage.PieceIO) *scheduler {
	n := len(t.PieceHashes)
	return &scheduler{
		torrent:      t,
		pieceIO:      pio,
		choker:       NewChoker(),
		numPieces:    n,
		peers:        make(map[[20]byte]*peer.Peer),
		have:         make([]bool, n),
		inProgress:   make(map[int]*storage.Piece),
		availability: make([]int, n),
	}
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
			m.handleEvent(event)

		case <-rechokeTicker.C:
			m.scheduler.choker.RecalculateChoking()

		case <-optimisticTicker.C:
			// TODO:
			// should be changed to recalc more seldom then normal rechoke
			m.scheduler.choker.RecalculateChoking()

		case <-m.ctx.Done(): // cancel signal
			return
		}
	}
}

func (s *scheduler) changeChoked() {
	dec := s.choker.RecalculateChoking()

	for _, p := range dec.Choke {
		s.peers[p.Conn.PeerID].AmChoking = true
	}

	for _, p := range dec.Unchoke {
		s.peers[p.Conn.PeerID].AmChoking = false
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

func (m *Manager) handleEvent(ev types.Event) {
	switch ev.Eve {
	case types.EventPeerReady:
		// New worker is up and running
		m.onPeerReady(ev)
	case types.EventPeerClosed:
		// Peer closed connection
		m.onPeerClosed(ev)
	case types.EventPeerInterested:
		m.onPeerInterested(ev)
	case types.EventPeerNotInterested:
		m.onPeerNotInterested(ev)
	case types.EventPeerChoked:
		m.onPeerChoked(ev)
	case types.EventPeerUnchoked:
		m.onPeerUnchoked(ev)
	case types.EventBitfield:
		m.onBitfield(ev)
	case types.EventHave:
		m.onHave(ev)
	case types.EventBlockReceived:
		m.onBlockReceived(ev) // Write to disk
	case types.EventBlockRequested:
		m.onBlockRequested(ev)
	case types.EventBlockUploaded:
		m.onBlockUploaded(ev)
	case types.EventReadDone:
		m.onReadDone(ev)
	}
}

func (m *Manager) onPeerReady(ev types.Event) {
	m.mu.Lock()
	w, ok := m.workers[ev.SenderID]
	m.mu.Lock()
	if !ok {
		return
	}

	p := w.Peer
	m.scheduler.peers[ev.SenderID] = p
	m.scheduler.choker.AddPeer(p)

	// Send over our bitfield to peer
	w.Send(types.Command{
		Cmd: types.CommandSendBitfield,
	})

	// Are we intereted?
	// We send interested upon receiving a bitfield?
}

func (m *Manager) onPeerClosed(ev types.Event) {
	p, ok := m.scheduler.peers[ev.SenderID]
	if !ok {
		return
	}

	// Do we have any blocks inflight, if so release thenm TODO: FIX INFLIGHT?=?? DO WE NEED INFLIGHT EVEN=??
	// DOES WORKER track inflight?
	for block := range p.Inflight {
		if currentPiece := m.scheduler.inProgress[block.Index]; currentPiece != nil {
			currentPiece.ReleaseBlock(block.Begin)
		}
	}

	for i := 0; i < m.scheduler.numPieces; i++ {
		if p.Has(i) {
			m.scheduler.availability[i]--
		}
	}
	m.scheduler.choker.RemovePeer(p)
	delete(m.scheduler.peers, ev.SenderID)
}

func (m *Manager) onPeerInterested(ev types.Event) {
	if p, ok := m.scheduler.peers[ev.SenderID]; ok {
		p.AmInterested = true
	}
}

func (m *Manager) onPeerNotInterested(ev types.Event) {
	if p, ok := m.scheduler.peers[ev.SenderID]; ok {
		p.AmInterested = false
	}
}

func (m *Manager) onPeerChoked(ev types.Event) {
	if p, ok := m.scheduler.peers[ev.SenderID]; ok {
		p.AmChoking = true
	}
}

func (m *Manager) onPeerUnchoked(ev types.Event) {
	if p, ok := m.scheduler.peers[ev.SenderID]; ok {
		p.AmChoking = false
	}
}

func (m *Manager) onBitfield(ev types.Event) {
	p, ok := m.scheduler.peers[ev.SenderID]
	if !ok {
		return
	}
	if err := p.SetBitfield(ev.Data); err != nil {
		m.log.Warn("Bad bitfield", "peer", hex.EncodeToString(ev.SenderID[:]), "error", err)
		return
	}

	// update avalibility
	for i := 0; i < m.scheduler.numPieces; i++ {
		if p.Has(i) {
			m.scheduler.availability[i]++
		}
	}

	// Recalc interest and send command on change
	m.calcInterested(p.Conn.PeerID)
}

func (m *Manager) onHave(ev types.Event) {
	p, ok := m.scheduler.peers[ev.SenderID]
	if !ok {
		return
	}

	if ev.PieceIndex < 0 || ev.PieceIndex >= m.scheduler.numPieces {
		return
	}

	if !p.Has(ev.PieceIndex) {
		p.SetPiece(ev.PieceIndex)
		m.scheduler.availability[ev.PieceIndex]++
	}
	m.calcInterested(p.Conn.PeerID)

	if !p.PeerChoking && p.AmInterested {
		// TODO: SEND PIECE TO requestor
	}
}

func (m *Manager) onBlockReceived(ev types.Event) {

	p, ok := m.getPeerForEvent(ev)
	if !ok {
		return // peer already closed, doesnt exist or something
	}

	m.scheduler.choker.IncrementDownload(p, len(ev.Data))
	m.downloaded.Add(uint64(len(ev.Data)))

	piece := m.scheduler.inProgress[ev.PieceIndex]
	if piece == nil {
		return // piece finished or was abandoned while this block was in flight
	}

	err := piece.AddBlock(ev.Begin, ev.Data)
	if err != nil {
		m.log.Warn("bad block",
			"peer", hex.EncodeToString(ev.SenderID[:]),
			"piece", ev.PieceIndex,
			"begin", ev.Begin,
			"error", err)
		piece.ReleaseBlock(ev.Begin) // let another peer fetch it
		return
	}

	if piece.Complete() {
		//dont take away inProgress yet! Wait until we get confirmation from IO that it actually went to disk and remove it on confirmation
		m.scheduler.pieceIO.Requests <- storage.DiskRequest{Kind: storage.DiskWrite,
			SenderID:   p.Conn.PeerID,
			PieceIndex: ev.PieceIndex,
			Begin:      ev.Begin, Length: ev.Length,
			Piece: piece}
	}
	//m.broadcastHave(index) //Do this now or when we finish writing???
}

func (m *Manager) onBlockRequested(ev types.Event) {
	w := m.workers[ev.SenderID]
	if w == nil {
		return
	}
	p, ok := m.getPeerForEvent(ev)
	if !ok {
		return // peer already closed, doesnt exist or something
	}

	if ev.PieceIndex < 0 || ev.PieceIndex >= m.scheduler.numPieces || ev.Begin < 0 || ev.Length <= 0 || ev.Length > maxRequestLen {
		w.Stop() //Should we stop just becasue of this
		return
	}

	if p.AmChoking || !p.PeerInterested || !m.scheduler.have[ev.PieceIndex] { //dont send, we may have choked while message is in transit or something esle
		return
	}

	req := storage.DiskRequest{
		Kind:       storage.DiskRead,
		SenderID:   ev.SenderID,
		PieceIndex: ev.PieceIndex,
		Begin:      ev.Begin,
		Length:     ev.Length,
	}
	m.scheduler.pieceIO.Requests <- req
}

func (m *Manager) onBlockUploaded(ev types.Event)

func (m *Manager) onReadDone(ev types.Event) {

}

func (m *Manager) calcInterested(peerID [20]byte) {
	p := m.scheduler.peers[peerID]
	want := false

	for i := 0; i < m.scheduler.numPieces; i++ {
		if !m.scheduler.have[i] && p.Has(i) {
			want = true
			break
		}
	}

	if want && !p.AmInterested {
		p.AmInterested = true
		m.workers[p.Conn.PeerID].Send(types.Command{Cmd: types.CommandInterested})
	} else if !want && p.AmInterested {
		p.AmInterested = false
		m.workers[p.Conn.PeerID].Send(types.Command{Cmd: types.CommandNotInterested})
	}
}

func (m *Manager) getPeerForEvent(ev types.Event) (*peer.Peer, bool) {
	p, ok := m.scheduler.peers[ev.SenderID]
	return p, ok
}
