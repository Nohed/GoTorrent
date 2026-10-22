package engine

import (
	"encoding/hex"
	"errors"
	"io"
	"math"
	"time"

	"github.com/Nohed/GoTorrent/internal/peer"
	"github.com/Nohed/GoTorrent/internal/storage"
	"github.com/Nohed/GoTorrent/internal/torrent"
	"github.com/Nohed/GoTorrent/internal/types"
)

const (
	blockSize          = 16 * 1024
	maxRequestLen      = 32 * 1024 // largest block a peer may ask us for
	maxInflightPerPeer = 128       // outstanding block requests per peer
	maxInProgress      = 64        // pieces being assembled at once
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
	inflight     map[[20]byte]map[blockKey]time.Time
	availability []int // how many peers have each piece
	Bitfield     *storage.Bitfield
	haveCount    int // Insta check the number of peices
}

// It should probably be a atmoic and accesed safely

// TODO: DEADLOCK POSSIBLITY:
// When a peer requests a block or a piece finishes, scheduler attempts to write to pieceIO.Requests:
// When PieceIO finishes reading or writing disk blocks and attempts to send the result back to m.events
// IF disk IO is too slow, Both channels might fill upp.
// * Scheduler blocks and hangs on writes to pieceIO.Requests:
// * PeiceIO blocks and hangs on writes to m.events
// = Both routines freeze forever.
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
		inflight:     make(map[[20]byte]map[blockKey]time.Time),
		availability: make([]int, n),
		Bitfield:     storage.NewBitfield(n),
		haveCount:    0,
	}
}

func (m *Manager) runScheduler() {
	rechokeTicker := time.NewTicker(10 * time.Second)
	defer rechokeTicker.Stop()

	timeoutTicker := time.NewTicker(2 * time.Second)
	defer timeoutTicker.Stop()

	for {
		select {
		case event := <-m.events:
			if event.Err != nil {
				if event.Eve == types.EventPeerClosed || errors.Is(event.Err, io.EOF) {
					m.log.Debug("peer disconnected", "peer", hex.EncodeToString(event.SenderID[:]), "error", event.Err)
				} else {
					m.log.Warn("peer event error", "peer", hex.EncodeToString(event.SenderID[:]), "error", event.Err)
				}
			}
			m.handleEvent(event)

		case <-rechokeTicker.C:
			m.scheduler.changeChoked(m) // Keeps track of optimistic vs regular choking itself

		case now := <-timeoutTicker.C:
			m.checkRequestTimeouts(now)

		case <-m.ctx.Done(): // cancel signal
			return
		}
	}
}

func (s *scheduler) changeChoked(m *Manager) {
	dec := s.choker.RecalculateChoking()

	// -- Change internal state
	for _, p := range dec.Choke {
		p.AmChoking.Store(true)
		if p.Conn == nil {
			continue
		}
		m.mu.Lock()
		w, ok := m.workers[p.Conn.PeerID]
		m.mu.Unlock()
		if ok {
			if !w.Send(types.Command{Cmd: types.CommandChoke}) {
				m.log.Warn("dropping choke message: worker command buffer full\n") //DROP? WAIT? WHAT??

			}

		}
	}

	for _, p := range dec.Unchoke {
		p.AmChoking.Store(false)
		if p.Conn == nil {
			continue
		}
		m.mu.Lock()
		w, ok := m.workers[p.Conn.PeerID]
		m.mu.Unlock()
		if ok {
			if !w.Send(types.Command{Cmd: types.CommandUnchoke}) {
				m.log.Warn("dropping unchoke message: worker command buffer full\n") //DROP? WAIT? WHAT??
			}

		}
	}

}

func (m *Manager) left() int64 {
	return m.leftBytes.Load()
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
		m.onBlockReceived(ev)
	case types.EventBlockRequested:
		m.onBlockRequested(ev)
	case types.EventBlockUploaded:
		m.onBlockUploaded(ev)
	case types.EventDiskRead:
		m.onDiskRead(ev)
	case types.EventDiskWrite:
		m.onDiskWrite(ev)
	}
}

func (m *Manager) onPeerReady(ev types.Event) {
	m.mu.Lock()
	w, ok := m.workers[ev.SenderID]
	m.mu.Unlock()
	if !ok {
		return
	}

	p := w.Peer
	m.scheduler.peers[ev.SenderID] = p
	m.scheduler.choker.AddPeer(p)
	// Send over our bitfield to peer
	if !w.Send(types.Command{Cmd: types.CommandSendBitfield, Data: m.scheduler.Bitfield.Bytes()}) {
		m.log.Warn("dropping SendBitfield message: worker command buffer full\n") //DROP? WAIT? WHAT??

	}
}

func (m *Manager) onPeerClosed(ev types.Event) {
	p, ok := m.scheduler.peers[ev.SenderID]
	if !ok {
		return
	}

	if inflight, ok := m.scheduler.inflight[ev.SenderID]; ok {
		for block := range inflight {
			if piece := m.scheduler.inProgress[block.piece]; piece != nil {
				piece.ReleaseBlock(block.begin)
			}
		}
		delete(m.scheduler.inflight, ev.SenderID)
	}

	for i := 0; i < m.scheduler.numPieces; i++ {
		if p.Bfield.Has(i) {
			m.scheduler.availability[i]--
		}
	}
	m.scheduler.choker.RemovePeer(p)
	delete(m.scheduler.peers, ev.SenderID)
	m.requestBlocksFromAll()
}

func (m *Manager) onPeerInterested(ev types.Event) {
	if p, ok := m.scheduler.peers[ev.SenderID]; ok {
		p.PeerInterested.Store(true)
	}
}

func (m *Manager) onPeerNotInterested(ev types.Event) {
	if p, ok := m.scheduler.peers[ev.SenderID]; ok {
		p.PeerInterested.Store(false)
	}
}

func (m *Manager) onPeerChoked(ev types.Event) {
	// Set state
	if p, ok := m.scheduler.peers[ev.SenderID]; ok {
		p.PeerChoking.Store(true)

		// Relase in flight blocks requested from this peer
		if inflight, ok := m.scheduler.inflight[ev.SenderID]; ok {
			for block := range inflight {
				if piece := m.scheduler.inProgress[block.piece]; piece != nil {
					piece.ReleaseBlock(block.begin)
				}
			}
			clear(inflight)
		}
		m.requestBlocksFromAll()
	}

}

func (m *Manager) onPeerUnchoked(ev types.Event) {
	if p, ok := m.scheduler.peers[ev.SenderID]; ok {
		p.PeerChoking.Store(false)
		m.requestBlocks(ev.SenderID)
	}
}

func (m *Manager) onBitfield(ev types.Event) {

	p, ok := m.scheduler.peers[ev.SenderID]
	if !ok {
		m.log.Warn("calcInterested: unknown peer", "peer", hex.EncodeToString(ev.SenderID[:4]))

		return
	}

	if err := p.Bfield.SetBitfield(ev.Data); err != nil {
		m.log.Warn("Bad bitfield", "peer", hex.EncodeToString(ev.SenderID[:]), "error", err)
		return
	}

	// update avalibility
	for i := 0; i < m.scheduler.numPieces; i++ {
		if p.Bfield.Has(i) {
			m.scheduler.availability[i]++
		}
	}

	// Recalc interest and send command on change
	m.calcInterested(p.Conn.PeerID)

	if !p.PeerChoking.Load() && p.AmInterested.Load() {
		m.requestBlocks(ev.SenderID)
	}
}

func (m *Manager) onHave(ev types.Event) {

	p, ok := m.scheduler.peers[ev.SenderID]
	if !ok {
		return
	}

	if ev.PieceIndex < 0 || ev.PieceIndex >= m.scheduler.numPieces {
		return
	}

	if !p.Bfield.Has(ev.PieceIndex) {
		p.Bfield.SetPiece(ev.PieceIndex)
		m.scheduler.availability[ev.PieceIndex]++
	}
	m.calcInterested(p.Conn.PeerID)

	if !p.PeerChoking.Load() && p.AmInterested.Load() {
		m.requestBlocks(ev.SenderID)
	}
}

func (m *Manager) onBlockReceived(ev types.Event) {

	p, ok := m.getPeerForEvent(ev)
	if !ok {
		return // peer already closed, doesnt exist or something
	}

	// Free the inflight spot as we have received the block
	key := blockKey{piece: ev.PieceIndex, begin: ev.Begin}
	if inflight, ok := m.scheduler.inflight[ev.SenderID]; ok {
		delete(inflight, key)
	}

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

	// increment only accepted dat
	m.scheduler.choker.IncrementDownload(p, len(ev.Data))
	m.downloaded.Add(uint64(len(ev.Data)))

	if piece.Complete() {
		//dont take away inProgress yet! Wait until we get confirmation from IO that it actually went to disk and remove it on confirmation
		m.scheduler.pieceIO.Requests <- storage.DiskRequest{Kind: storage.DiskWrite,
			SenderID:   p.Conn.PeerID,
			PieceIndex: ev.PieceIndex,
			Begin:      ev.Begin, Length: ev.Length,
			Piece: piece}
	}

	// Slot freed up, we can request next block from this peer
	m.requestBlocks(ev.SenderID)

}

func (m *Manager) onBlockRequested(ev types.Event) {

	w := m.getWorkerForEvent(ev)

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

	if p.AmChoking.Load() || !p.PeerInterested.Load() || !m.scheduler.have[ev.PieceIndex] { //dont send, we may have choked while message is in transit or something esle
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

func (m *Manager) onDiskRead(ev types.Event) {

	if ev.Err != nil {
		return // already logged by runScheduler; nothing to send
	}
	p, ok := m.getPeerForEvent(ev)
	if !ok {
		return // peer closed while we were reading
	}
	if p.AmChoking.Load() {
		return // we choked them meanwhile, so don't upload
	}

	w := m.getWorkerForEvent(ev)
	if w == nil {
		return
	}

	if !w.Send(types.Command{
		Cmd:        types.CommandSend,
		PieceIndex: ev.PieceIndex,
		Begin:      ev.Begin,
		Data:       ev.Data}) {
		// Do not requeue, a full event channel will block the scheduler
		// Drop this peer as its progress has stopped = prevents deadlock
		m.log.Debug("worker queue full, dropping peer", "peer", hex.EncodeToString(ev.SenderID[:]))
		w.Stop()
		return
	}
}

func (m *Manager) onDiskWrite(ev types.Event) {

	// safety check
	if ev.PieceIndex < 0 || ev.PieceIndex >= m.scheduler.numPieces {
		m.log.Warn("onDiskWrite: piece index out of bounds", "piece", ev.PieceIndex)
		return
	}

	piece := m.scheduler.inProgress[ev.PieceIndex]

	if ev.Err != nil {
		m.log.Warn("piece disk write failed", "piece", ev.PieceIndex, "err", ev.Err)

		// If verification or writing failed, discard the piece so it can be re-downloaded
		delete(m.scheduler.inProgress, ev.PieceIndex)
		if piece != nil {
			piece.Reset()
		}
		m.requestBlocksFromAll()
		return
	}
	// ---- Writing succeded

	if !m.scheduler.have[ev.PieceIndex] {
		m.scheduler.have[ev.PieceIndex] = true
		m.scheduler.Bitfield.SetPiece(ev.PieceIndex)
		m.scheduler.haveCount++
		m.havePieces.Add(1)
		m.leftBytes.Add(-int64(m.pieceIO.PieceSize(ev.PieceIndex)))
	}
	// Remove from active assembly & broadcast to swarm
	delete(m.scheduler.inProgress, ev.PieceIndex)
	m.broadcastHave(ev.PieceIndex)

	// Are we finnished?
	if m.scheduler.haveCount == m.scheduler.numPieces {
		m.doneOnce.Do(func() {
			close(m.done)
			m.scheduler.choker.SetSeeding(true)
			m.log.Info("Torrent download complete!")
		})
	}

	// Recalc Interested
	for peerID := range m.scheduler.peers {
		m.calcInterested(peerID)
	}

	m.requestBlocksFromAll()
}

func (m *Manager) onBlockUploaded(ev types.Event) {
	p, ok := m.getPeerForEvent(ev)
	if !ok {
		return
	}

	if ev.Err != nil {
		m.log.Warn("failed to upload block",
			"peer", hex.EncodeToString(ev.SenderID[:]),
			"piece", ev.PieceIndex,
			"begin", ev.Begin,
			"error", ev.Err,
		)
		return
	}

	m.uploaded.Add(uint64(ev.Length))
	m.scheduler.choker.IncrementUpload(p, ev.Length)
}

func (m *Manager) calcInterested(peerID [20]byte) {
	p := m.scheduler.peers[peerID]
	want := false

	for i := 0; i < m.scheduler.numPieces; i++ {
		if !m.scheduler.have[i] && p.Bfield.Has(i) {
			want = true
			break
		}
	}

	m.mu.Lock()
	w, ok := m.workers[peerID]
	m.mu.Unlock()
	if !ok {
		return
	}

	if want && !p.AmInterested.Load() {
		p.AmInterested.Store(true)
		m.log.Info("We want data, we are intersted\n")
		if !w.Send(types.Command{Cmd: types.CommandInterested}) {
			m.log.Warn("dropping intersted message: worker command buffer full\n") //DROP? WAIT? WHAT??

		}
	} else if !want && p.AmInterested.Load() {
		p.AmInterested.Store(false)
		if !w.Send(types.Command{Cmd: types.CommandNotInterested}) {
			m.log.Warn("dropping uninterested message: worker command buffer full\n") //DROP? WAIT? WHAT??

		}
	}
}

func (m *Manager) getPeerForEvent(ev types.Event) (*peer.Peer, bool) {
	p, ok := m.scheduler.peers[ev.SenderID]
	return p, ok
}

func (m *Manager) getWorkerForEvent(ev types.Event) *Worker {
	m.mu.Lock()
	defer m.mu.Unlock()

	w := m.workers[ev.SenderID]

	return w
}

func (m *Manager) broadcastHave(pieceIndex int) {
	// Snapshot workers
	m.mu.Lock()
	workers := make([]*Worker, 0, len(m.workers))
	for _, w := range m.workers {
		workers = append(workers, w)
	}
	m.mu.Unlock()

	cmd := types.Command{
		Cmd:        types.CommandHave,
		PieceIndex: pieceIndex,
	}

	// Send outside lock, fails on filled channel buffers
	for _, w := range workers {
		if !w.Send(cmd) {
			m.log.Warn("dropping have message: worker command buffer full",
				"peer", hex.EncodeToString(w.Peer.Conn.PeerID[:]),
				"piece", pieceIndex,
			)
		}
	}
}

func (m *Manager) requestBlocks(peerID [20]byte) {
	p := m.scheduler.peers[peerID]
	if p == nil || p.PeerChoking.Load() {
		return // Peer is not connected or is choking us
	}

	w := m.getWorkerForEvent(types.Event{SenderID: peerID})
	if w == nil {
		return
	}

	if m.scheduler.inflight[peerID] == nil {
		m.scheduler.inflight[peerID] = make(map[blockKey]time.Time)
	}

	peerInflight := m.scheduler.inflight[peerID]

	// Fill the pipeline up to maxInflightPerPeer (8)
	for len(peerInflight) < maxInflightPerPeer {
		pieceIndex, begin, length, ok := m.chooseNextBlock(p)
		if !ok {
			break // No blocks available that this peer can supply
		}

		key := blockKey{piece: pieceIndex, begin: begin}
		peerInflight[key] = time.Now()

		// Send request command to the worker
		sent := w.Send(types.Command{
			Cmd:        types.CommandRequestBlock,
			PieceIndex: pieceIndex,
			Begin:      begin,
			Length:     length,
		})

		if !sent {
			m.log.Warn("dropping unchoke message: worker command buffer full\n") //DROP? WAIT? WHAT??
			// If sending failed, release the claimed block
			delete(peerInflight, key)
			if piece := m.scheduler.inProgress[pieceIndex]; piece != nil {
				piece.ReleaseBlock(begin)
			}
			break
		}
	}
}

func (m *Manager) chooseNextBlock(p *peer.Peer) (pieceIndex, begin, length int, ok bool) {
	s := m.scheduler

	// Prioritize finishing in-progress pieces
	for i, piece := range s.inProgress {
		if p.Bfield.Has(i) {
			if offset, blen, found := piece.NextUnrequested(); found {
				return i, offset, blen, true
			}
		}
	}

	// Check new peice can be started
	if len(s.inProgress) >= maxInProgress {
		return 0, 0, 0, false
	}

	// Pick the rarest piece that this peer has and we don't have
	bestPiece := -1
	bestAvailability := math.MaxInt
	for i := 0; i < s.numPieces; i++ {
		if !s.have[i] && s.inProgress[i] == nil && p.Bfield.Has(i) {
			avail := s.availability[i]
			if avail < bestAvailability {
				bestAvailability = avail
				bestPiece = i
			}
		}
	}

	if bestPiece == -1 {
		return 0, 0, 0, false // Peer has no pieces we need
	}

	// Create new in-progress piece
	pieceLength := m.pieceIO.PieceSize(bestPiece)
	newPiece := storage.NewPiece(bestPiece, pieceLength, m.torrent.PieceHashes[bestPiece])
	s.inProgress[bestPiece] = newPiece
	offset, blen, _ := newPiece.NextUnrequested()
	return bestPiece, offset, blen, true
}

func (m *Manager) requestBlocksFromAll() {
	for peerID, p := range m.scheduler.peers {
		if !p.PeerChoking.Load() && p.AmInterested.Load() {
			m.requestBlocks(peerID)
		}
	}
}

func (m *Manager) checkRequestTimeouts(now time.Time) {
	timedOutAny := false
	for peerID, inflight := range m.scheduler.inflight {
		w := m.getWorkerForEvent(types.Event{SenderID: peerID})
		for block, requestedAt := range inflight {
			if now.Sub(requestedAt) >= requestTimeout {
				delete(inflight, block)
				if piece := m.scheduler.inProgress[block.piece]; piece != nil {
					piece.ReleaseBlock(block.begin)
					if w != nil {
						if !w.Send(types.Command{
							Cmd:        types.CommandCancelBlock,
							PieceIndex: block.piece,
							Begin:      block.begin,
							Length:     blockSize,
						}) {
							m.log.Warn("dropping cancel message: worker command buffer full\n") //DROP? WAIT? WHAT??

						}
					}
				}
				timedOutAny = true
			}
		}
	}
	if timedOutAny {
		m.requestBlocksFromAll()
	}
}

func (m *Manager) markHave(have []bool) {
	var count int32
	var left int64
	for i, ok := range have {
		if ok {
			m.scheduler.have[i] = true
			m.scheduler.Bitfield.SetPiece(i)
			m.scheduler.haveCount++
			count++
		} else {
			left += int64(m.pieceIO.PieceSize(i))
		}
	}
	m.havePieces.Store(count)
	m.leftBytes.Store(left)

	if m.scheduler.haveCount == m.scheduler.numPieces {
		m.doneOnce.Do(func() {
			close(m.done)
			m.scheduler.choker.SetSeeding(true) // we have the whole file, focus on sending
			m.log.Info("Torrent download complete!")
		})
	}
}
