package storage

import "fmt"

// BlockSize is the size of the chunks requested from peers (16 KiB).
// Pieces are downloaded as blocks and only verified once every block has arrived.
const BlockSize = 16384

// Piece collects the blocks of a single piece in memory until it is complete.
type Piece struct {
	Index  int
	Length int
	Hash   [20]byte

	data           []byte
	blockReceived  []bool // true once the block has arrived
	blockRequested []bool // true while a request for the block is in flight
	receivedCount  int
}

// NewPiece creates an empty piece ready to receive blocks.
func NewPiece(index, length int, hash [20]byte) *Piece {
	blockCount := (length + BlockSize - 1) / BlockSize
	return &Piece{
		Index:          index,
		Length:         length,
		Hash:           hash,
		data:           make([]byte, length),
		blockReceived:  make([]bool, blockCount),
		blockRequested: make([]bool, blockCount),
	}
}

// blockLength returns the expected length of the block at blockIndex.
// Every block is BlockSize except possibly the last one.
func (piece *Piece) blockLength(blockIndex int) int {
	isLastBlock := blockIndex == len(piece.blockReceived)-1
	if isLastBlock {
		return piece.Length - blockIndex*BlockSize
	}
	return BlockSize
}

// AddBlock stores a block received from a peer. blockOffset is the byte offset within the piece.
// Duplicate blocks are ignored.
func (piece *Piece) AddBlock(blockOffset int, blockData []byte) error {
	if blockOffset < 0 || blockOffset >= piece.Length {
		return fmt.Errorf("block offset %d out of range for piece of length %d", blockOffset, piece.Length)
	}
	if blockOffset%BlockSize != 0 {
		return fmt.Errorf("block offset %d is not aligned to block size %d", blockOffset, BlockSize)
	}

	blockIndex := blockOffset / BlockSize
	expectedLength := piece.blockLength(blockIndex)
	if len(blockData) != expectedLength {
		return fmt.Errorf("block %d has length %d, want %d", blockIndex, len(blockData), expectedLength)
	}

	if piece.blockReceived[blockIndex] {
		return nil
	}
	copy(piece.data[blockOffset:], blockData)
	piece.blockReceived[blockIndex] = true
	piece.receivedCount++
	return nil
}

// ReleaseBlock clears the request flag (peer choked, timed out, or disconnected)
// so the block can be handed out again.
func (piece *Piece) ReleaseBlock(offset int) {
	i := offset / BlockSize
	if i >= 0 && i < len(piece.blockRequested) && !piece.blockReceived[i] {
		piece.blockRequested[i] = false
	}
}

// Reset discards all data after a failed hash check.
func (piece *Piece) Reset() {
	clear(piece.blockReceived)
	clear(piece.blockRequested)
	piece.receivedCount = 0
}

func (piece *Piece) NextUnrequested() (offset, length int, ok bool) {
	for i := range piece.blockReceived {
		if !piece.blockReceived[i] && !piece.blockRequested[i] {
			piece.blockRequested[i] = true // claimed
			return i * BlockSize, piece.blockLength(i), true
		}
	}
	return 0, 0, false
}

// Complete reports whether every block has been received.
func (piece *Piece) Complete() bool {
	return piece.receivedCount == len(piece.blockReceived)
}

// MissingBlocks returns the offsets and lengths of blocks still needed, for building requests.
func (piece *Piece) MissingBlocks() (missingOffsets, missingLengths []int) {
	for blockIndex, received := range piece.blockReceived {
		if !received {
			missingOffsets = append(missingOffsets, blockIndex*BlockSize)
			missingLengths = append(missingLengths, piece.blockLength(blockIndex))
		}
	}
	return missingOffsets, missingLengths
}

func (piece *Piece) Bytes() []byte {
	return piece.data
}
