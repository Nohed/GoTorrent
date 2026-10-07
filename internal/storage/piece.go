package storage

import "fmt"

// BlockSize is the size of the chunks requested from peers (16 KiB).
// Pieces are downloaded as blocks and only verified once every block has arrived.
const BlockSize = 16384

// Piece collects the blocks of a single piece in memory until it is complete.
type Piece struct {
	Index  int      // Index of the piece in the torrent
	Length int      // Length of this piece (the last piece may be shorter)
	Hash   [20]byte // Expected SHA-1 from the torrent file

	data          []byte // The piece's bytes, filled in block by block
	blockReceived []bool // One flag per block, true once that block has arrived
	receivedCount int    // Number of distinct blocks received so far
}

// NewPiece creates an empty piece ready to receive blocks.
func NewPiece(index, length int, hash [20]byte) *Piece {
	blockCount := (length + BlockSize - 1) / BlockSize
	return &Piece{
		Index:         index,
		Length:        length,
		Hash:          hash,
		data:          make([]byte, length),
		blockReceived: make([]bool, blockCount),
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
