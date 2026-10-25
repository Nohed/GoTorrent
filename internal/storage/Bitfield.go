package storage

import "fmt"

// Bitfield tracks which pieces are available.
type Bitfield struct {
	numPieces int
	data      []byte
}

func NewBitfield(numPieces int) *Bitfield {
	return &Bitfield{
		numPieces: numPieces,
		data:      make([]byte, (numPieces+7)/8),
	}
}

func (b *Bitfield) Has(index int) bool {
	if !b.validIndex(index) {
		return false
	}

	byteIndex := index / 8
	mask := byte(0x80) >> (index % 8)

	return b.data[byteIndex]&mask != 0
}

func (b *Bitfield) SetPiece(index int) error {
	if !b.setBit(index, true) {
		return fmt.Errorf("piece index %d out of range", index)
	}

	return nil
}

func (b *Bitfield) ClearPiece(index int) {
	b.setBit(index, false)
}

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

func (b *Bitfield) Bytes() []byte {
	return append([]byte(nil), b.data...)
}

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
