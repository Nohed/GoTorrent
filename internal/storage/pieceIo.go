package storage

import (
	"fmt"
	"os"
)

// PieceIO writes verified pieces to a single file on disk.
type PieceIO struct {
	file        *os.File
	totalLength int // Length of the whole file in bytes
	pieceLength int // Length of each piece (except possibly the last)
}

// NewPieceIO creates (or truncates) the output file and preallocates it to the full length.
func NewPieceIO(path string, totalLength, pieceLength int) (*PieceIO, error) {
	if totalLength <= 0 || pieceLength <= 0 {
		return nil, fmt.Errorf("invalid sizes: length %d, piece length %d", totalLength, pieceLength)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open output file: %w", err)
	}
	if err := file.Truncate(int64(totalLength)); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to preallocate file: %w", err)
	}

	return &PieceIO{file: file, totalLength: totalLength, pieceLength: pieceLength}, nil
}

// PieceSize returns the size of the piece at pieceIndex, accounting for a shorter last piece.
func (pieceIO *PieceIO) PieceSize(pieceIndex int) int {
	pieceStart := pieceIndex * pieceIO.pieceLength
	pieceEnd := pieceStart + pieceIO.pieceLength
	if pieceEnd > pieceIO.totalLength {
		pieceEnd = pieceIO.totalLength
	}
	return pieceEnd - pieceStart
}

func (pieceIO *PieceIO) WritePiece(piece *Piece) error {
	if piece.Index < 0 || piece.Index*pieceIO.pieceLength >= pieceIO.totalLength {
		return fmt.Errorf("piece index %d out of range", piece.Index)
	}

	expectedSize := pieceIO.PieceSize(piece.Index)
	if piece.Length != expectedSize {
		return fmt.Errorf("piece %d has length %d, want %d", piece.Index, piece.Length, expectedSize)
	}

	if err := piece.Verify(); err != nil {
		return err
	}

	fileOffset := int64(piece.Index) * int64(pieceIO.pieceLength)
	if _, err := pieceIO.file.WriteAt(piece.data, fileOffset); err != nil {
		return fmt.Errorf("failed to write piece %d: %w", piece.Index, err)
	}
	return nil
}

func (pieceIO *PieceIO) Close() error {
	return pieceIO.file.Close()
}
