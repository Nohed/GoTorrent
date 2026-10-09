package peer

import (
	"fmt"
	"io"
)

const protocolID = "BitTorrent protocol"
const protocolIDLen = 0x13

// Length of Protocol identifier = 0x13 (19 base10 bytes)
// Protocol identifier = "BitTorrent protocol"
// Eight reserved bytes set to 0x00
// Info hash of file we want
// Our peer ID
type Handshake struct {
	Pstr     string   // Always "BitTorrent protocol"
	InfoHash [20]byte // SHA1 hash of the info key in the torrent file
	PeerID   [20]byte // Unique identifier for the peer
}

func NewHandshake(infoHash, peerID [20]byte) *Handshake {
	return &Handshake{Pstr: protocolID, InfoHash: infoHash, PeerID: peerID}
}

/*
1byte    19 bytes    8 bytes       20 bytes   20 byte
[pstrlen][pstr][8 reserved bytes][info hash][peer id]
*/
func (h *Handshake) Serialize() []byte {
	buf := make([]byte, 49+len(h.Pstr))
	buf[0] = byte(len(h.Pstr)) // Length of identifier
	index := 1
	index += copy(buf[index:], []byte(h.Pstr))  // Protocl identifier
	index += copy(buf[index:], make([]byte, 8)) // Reserved bytes, should be 0x00, i think this can be ignored and do just index += 8
	index += copy(buf[index:], h.InfoHash[:])   // Info hash
	copy(buf[index:], h.PeerID[:])              // Peer ID
	return buf
}

/*
1byte    19 bytes    8 bytes       20 bytes   20 byte
[pstrlen][pstr][8 reserved bytes][info hash][peer id]
*/
func ReadHandshake(r io.Reader) (*Handshake, error) {
	var length [1]byte
	// Get length of protocol identifier
	if _, err := io.ReadFull(r, length[:]); err != nil {
		return nil, err
	}
	if length[0] != protocolIDLen {
		return nil, fmt.Errorf("invalid protocol identifier length: %d", length[0])
	}

	buffer := make([]byte, protocolIDLen+8+20+20) // 19 + 8 + 20 + 20 = 67 bytes
	if _, err := io.ReadFull(r, buffer); err != nil {
		return nil, fmt.Errorf("Error reading handshake message: %w", err)
	}

	// Build struct
	Hs := &Handshake{}
	Hs.Pstr = string(buffer[:protocolIDLen])
	if Hs.Pstr != protocolID {
		return nil, fmt.Errorf("unsupported protocol %q", Hs.Pstr)
	}

	copy(Hs.InfoHash[:], buffer[protocolIDLen+8:protocolIDLen+8+20])
	copy(Hs.PeerID[:], buffer[protocolIDLen+8+20:])

	return Hs, nil
}
