package manager

import (
	"sync"

	"github.com/Nohed/GoTorrent/internal/storage"
)

// THIS STRUCT IS SHARED BETWEEN ALL WORKERS
type Manager struct {
	mutex    *sync.Mutex            //syncronization between workers
	io       *storage.PieceIO       // write to disk
	hashes   [][20]byte             //hashes of all the pieces
	have     []bool                 //pieces that are downloaded and done
	inflight map[int]*storage.Piece // pieces currently being downloaded
}

// NewManager creates and initializes a new piece Manager.
func NewManager(io *storage.PieceIO, hashes [][20]byte) *Manager {
	return &Manager{
		mutex:    &sync.Mutex{},
		io:       io,
		hashes:   hashes,
		have:     make([]bool, len(hashes)),    // All pieces start as not downloaded
		inflight: make(map[int]*storage.Piece), // Safely initialized map!
	}
}

// NextRequest picks a block this peer can supply.
// peerHas reports whether the peer has a given piece.
func (manager *Manager) NextRequest(peerHas func(int) bool) (pieceIndex, blockBegin, blockLength int, ok bool) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	// 1. Finish pieces already started (keeps few partial pieces in memory, paralellalize pieces).
	for pieceIndex, piece := range manager.inflight {
		if peerHas(pieceIndex) {
			if blockBegin, blockLength, ok := piece.NextUnrequested(); ok {
				return pieceIndex, blockBegin, blockLength, true
			}
		}
	}

	// 2. Start a new piece (replace with rarest-first later).
	for pieceIndex := range manager.have {
		if !manager.have[pieceIndex] &&
			manager.inflight[pieceIndex] == nil &&
			peerHas(pieceIndex) {

			piece := storage.NewPiece(
				pieceIndex,
				manager.io.PieceSize(pieceIndex),
				manager.hashes[pieceIndex],
			)

			manager.inflight[pieceIndex] = piece

			blockBegin, blockLength, _ := piece.NextUnrequested()
			return pieceIndex, blockBegin, blockLength, true
		}
	}

	return 0, 0, 0, false
}

// AddBlock returns done=true when the piece is completed, verified, and written.
func (manager *Manager) AddBlock(pieceIndex, blockBegin int, blockData []byte) (done bool, err error) {
	manager.mutex.Lock()

	piece := manager.inflight[pieceIndex]
	if piece == nil { // unsolicited block or duplicate after completion
		manager.mutex.Unlock()
		return false, nil
	}

	if err := piece.AddBlock(blockBegin, blockData); err != nil {
		manager.mutex.Unlock()
		return false, err
	}

	if !piece.Complete() {
		manager.mutex.Unlock()
		return false, nil
	}

	delete(manager.inflight, pieceIndex) // we now own the piece exclusively
	manager.mutex.Unlock()

	// Hash + disk I/O outside the lock.
	if err := manager.io.WritePiece(piece); err != nil {
		piece.Reset()

		manager.mutex.Lock()
		manager.inflight[pieceIndex] = piece // back in the pool to retry
		manager.mutex.Unlock()

		return false, err
	}

	manager.mutex.Lock()
	manager.have[pieceIndex] = true
	manager.mutex.Unlock()

	return true, nil
}

// Have reports whether the requested piece has been downloaded and verified.
func (manager *Manager) Have(pieceIndex int) bool {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	return pieceIndex >= 0 &&
		pieceIndex < len(manager.have) &&
		manager.have[pieceIndex]
}

// Release is called for each block belonging to a dead or choked peer's inflight set.
func (manager *Manager) Release(pieceIndex, blockBegin int) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if piece := manager.inflight[pieceIndex]; piece != nil {
		piece.ReleaseBlock(blockBegin)
	}
}

func (m *Manager) NumPieces() int {
	return len(m.hashes)
}
