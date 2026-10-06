package torrent

import (
	"crypto/sha1"
	"fmt"
	"os"

	"github.com/Nohed/GoTorrent/internal/bencode"
)

const hashLength = 20

type bencodeInfo struct {
	Pieces      string `bencode:"pieces"`
	PieceLength int    `bencode:"piece length"`
	Length      int    `bencode:"length"`
	Name        string `bencode:"name"`
}

type bencodeTorrent struct {
	Announce string             `bencode:"announce"`
	Info     bencode.RawMessage `bencode:"info"`
}

type Torrent struct {
	Announce    string     // URL of Tracker with IP list of peers currently seeding or leaching file
	InfoHash    [20]byte   // SHA-1 hash of raw bencoded info section of torrent file
	PieceHashes [][20]byte // List of hashes for each piece
	PieceLength int        // Length of each piece
	Length      int        // Length of file
	Name        string     // Name of file
}

// Parse file from disk and return a Torrent struct
func ParseFile(path string) (*Torrent, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to read torrent file: %v", err)
	}
	return Parse(data)
}

// Parse torrent data and return a Torrent struct
func Parse(data []byte) (*Torrent, error) {
	var bt bencodeTorrent
	if err := bencode.Unmarshal(data, &bt); err != nil {
		return nil, fmt.Errorf("Failed to unmarshal torrent data: %v", err)
	}

	if len(bt.Info) == 0 {
		return nil, fmt.Errorf("Torrent file is missing info section")
	}

	var info bencodeInfo
	if err := bencode.Unmarshal(bt.Info, &info); err != nil {
		return nil, fmt.Errorf("Failed to unmarshal info section: %v", err)
	}

	if info.PieceLength <= 0 {
		return nil, fmt.Errorf("Invalid piece length: %d", info.PieceLength)
	}

	if info.Length <= 0 {
		return nil, fmt.Errorf("Invalid file length: %d", info.Length)
	}

	hashes, err := splitPieceHashes(info.Pieces)
	if err != nil {
		return nil, err
	}

	sizeFromInfo := (info.Length + info.PieceLength - 1) / info.PieceLength
	if len(hashes) != sizeFromInfo {
		return nil, fmt.Errorf("File incorrect, expected %d pieces, got %d", sizeFromInfo, len(hashes))
	}

	infoHash := sha1.Sum(bt.Info)
	return &Torrent{
		Announce:    bt.Announce,
		InfoHash:    infoHash,
		PieceHashes: hashes,
		PieceLength: info.PieceLength,
		Length:      info.Length,
		Name:        info.Name,
	}, nil
}

// Turn "pieces" string into 20-byte hash
func splitPieceHashes(pieces string) ([][20]byte, error) {
	bytes := []byte(pieces)                            // Convert string into byte slice
	if len(bytes) == 0 || len(bytes)%hashLength != 0 { // Check if length is a multiple of 20
		return nil, fmt.Errorf("malformed pieces string: length %d", len(bytes))
	}

	hashes := make([][20]byte, len(bytes)/hashLength) // Slice of 20-byte arrays
	for i := range hashes {
		copy(hashes[i][:], bytes[i*hashLength:(i+1)*hashLength]) // Copy 20 byte slice
	}
	return hashes, nil
}

// ============== Helpers ==============
