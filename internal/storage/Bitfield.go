package storage

import "fmt"

// Bitfield tracks which pieces are available.
type Bitfield struct {
	numPieces int
	data      []byte
}

// NewBitfield creates an empty bitfield for the given number of pieces.
func NewBitfield(numPieces int) *Bitfield {
	return &Bitfield{
		numPieces: numPieces,
		data:      make([]byte, (numPieces+7)/8),
	}
}

// Has reports whether the piece at index is available.
func (b *Bitfield) Has(index int) bool {
	if !b.validIndex(index) {
		return false
	}

	byteIndex := index / 8
	mask := byte(0x80) >> (index % 8)

	return b.data[byteIndex]&mask != 0
}

// SetPiece marks a piece as available.
func (b *Bitfield) SetPiece(index int) error {
	if !b.setBit(index, true) {
		return fmt.Errorf("piece index %d out of range", index)
	}

	return nil
}

// ClearPiece marks a piece as unavailable.
func (b *Bitfield) ClearPiece(index int) {
	b.setBit(index, false)
}

// SetBitfield replaces the raw bitfield data.
func (b *Bitfield) SetBitfield(data []byte) error {
	if len(data) != len(b.data) {
		return fmt.Errorf(
			"bitfield length %d, want %d",
			len(data), len(b.data),
		)
	}

	copy(b.data, data)
	return nil
}

// Bytes returns a copy of the raw bitfield.
func (b *Bitfield) Bytes() []byte {
	return append([]byte(nil), b.data...)
}

// NumPieces returns the number of pieces represented.
func (b *Bitfield) NumPieces() int {
	return b.numPieces
}

func (b *Bitfield) validIndex(index int) bool {
	return index >= 0 && index < b.numPieces
}

func (b *Bitfield) setBit(index int, on bool) bool {
	if !b.validIndex(index) {
		return false
	}

	byteIndex := index / 8
	mask := byte(0x80) >> (index % 8)

	if on {
		b.data[byteIndex] |= mask
	} else {
		b.data[byteIndex] &^= mask
	}

	return true
}
