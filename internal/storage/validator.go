package storage

import (
	"crypto/sha1"
	"errors"
	"fmt"
)

// ErrHashMismatch is returned when a piece's SHA-1 does not match the hash from the torrent file.
var ErrHashMismatch = errors.New("piece hash mismatch")

// Verify checks that the SHA-1 of data equals the expected hash.
func Verify(data []byte, expected [20]byte) error {
	got := sha1.Sum(data)
	if got != expected {
		return fmt.Errorf("%w: got %x, want %x", ErrHashMismatch, got, expected)
	}
	return nil
}

// VerifyPiece checks that a piece is complete and that its assembled data matches its expected hash.
func VerifyPiece(piece *Piece) error {
	if !piece.Complete() {
		missingOffsets, _ := piece.MissingBlocks()
		return fmt.Errorf("piece %d is incomplete: %d blocks missing", piece.Index, len(missingOffsets))
	}
	return Verify(piece.Bytes(), piece.Hash)
}
