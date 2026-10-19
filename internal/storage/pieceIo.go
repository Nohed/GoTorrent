package storage

import (
	"context"
	"fmt"
	"os"

	"github.com/Nohed/GoTorrent/internal/types"
)

type DiskRequestKind uint8

const (
	DiskRead DiskRequestKind = iota
	DiskWrite
)

type DiskRequest struct {
	Kind DiskRequestKind

	SenderID [20]byte

	PieceIndex int
	Begin      int
	Length     int

	Piece *Piece // Used for writes
}

// PieceIO writes verified pieces to a single file on disk by listening on a channel and writing to an output if something goes wrong.
type PieceIO struct {
	file        *os.File
	totalLength int
	pieceLength int

	Requests chan DiskRequest
	events   chan<- types.Event
}
type WriteError struct {
	Index int
	Err   error
}

// NewPieceIO creates the output file and preallocates it to the full length.
func NewPieceIO(path string, totalLength int, pieceLength int, events chan<- types.Event,
) (*PieceIO, error) {
	if totalLength <= 0 || pieceLength <= 0 {
		return nil, fmt.Errorf("invalid sizes: length %d, piece length %d", totalLength, pieceLength)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open output file: %w", err)
	}

	if err := file.Truncate(int64(totalLength)); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to resize output file: %w", err)
	}

	return &PieceIO{
		file:        file,
		totalLength: totalLength,
		pieceLength: pieceLength,
		Requests:    make(chan DiskRequest, 64),
		events:      events,
	}, nil
}

func (pio *PieceIO) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case req, ok := <-pio.Requests:
			if !ok {
				return
			}

			var ev types.Event

			switch req.Kind {
			case DiskRead:
				data, err := pio.ReadBlock(req.PieceIndex, req.Begin, req.Length)

				ev = types.Event{
					Eve:        types.EventDiskRead,
					SenderID:   req.SenderID,
					PieceIndex: req.PieceIndex,
					Begin:      req.Begin,
					Length:     req.Length,
					Data:       data,
					Err:        err,
				}

			case DiskWrite:
				var err error

				if req.Piece == nil {
					err = fmt.Errorf("missing piece for disk write")
				} else {
					err = pio.WritePiece(req.Piece)
				}

				ev = types.Event{
					Eve:        types.EventDiskWrite,
					SenderID:   req.SenderID,
					PieceIndex: req.PieceIndex,
					Err:        err,
				}

			default:
				// Log or handle invalid request kinds here.
				continue
			}

			select {
			case pio.events <- ev:
			case <-ctx.Done():
				return
			}
		}
	}
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

// WritePiece verifies the piece and only then writes it at its offset in the file.
// Unverified data never reaches the disk.
func (pieceIO *PieceIO) WritePiece(piece *Piece) error {
	if piece.Index < 0 || piece.Index*pieceIO.pieceLength >= pieceIO.totalLength {
		return fmt.Errorf("piece index %d out of range", piece.Index)
	}

	expectedSize := pieceIO.PieceSize(piece.Index)
	if piece.Length != expectedSize {
		return fmt.Errorf("piece %d has length %d, want %d", piece.Index, piece.Length, expectedSize)
	}

	if err := VerifyPiece(piece); err != nil {
		return err
	}

	fileOffset := int64(piece.Index) * int64(pieceIO.pieceLength)
	if _, err := pieceIO.file.WriteAt(piece.Bytes(), fileOffset); err != nil {
		return fmt.Errorf("failed to write piece %d: %w", piece.Index, err)
	}
	return nil
}

// ReadBlock reads a block from a piece at the specified offset.
func (pieceIO *PieceIO) ReadBlock(pieceIndex int, begin int, length int) ([]byte, error) {
	if pieceIndex < 0 || pieceIndex*pieceIO.pieceLength >= pieceIO.totalLength {
		return nil, fmt.Errorf("piece index %d out of range", pieceIndex)
	}

	pieceSize := pieceIO.PieceSize(pieceIndex)

	if begin < 0 || length <= 0 || begin+length > pieceSize {
		return nil, fmt.Errorf("invalid block range")
	}

	fileOffset := int64(pieceIndex)*int64(pieceIO.pieceLength) + int64(begin)

	data := make([]byte, length)
	n, err := pieceIO.file.ReadAt(data, fileOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read block: %w", err)
	}

	if n != length {
		return nil, fmt.Errorf("short read: got %d bytes, want %d", n, length)
	}

	return data, nil
}

// Close closes the underlying file.
func (pieceIO *PieceIO) Close() error {
	return pieceIO.file.Close()
}
