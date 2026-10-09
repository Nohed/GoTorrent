package peer

import (
	"encoding/binary"
	"fmt"
	"io"
)

const maxMessageLen = 1 << 20 // max message length is 1 MiB 2^20 bit
const (
	MsgChoke         messageID = 0
	MsgUnchoke       messageID = 1
	MsgInterested    messageID = 2
	MsgNotInterested messageID = 3
	MsgHave          messageID = 4
	MsgBitfield      messageID = 5
	MsgRequest       messageID = 6
	MsgPiece         messageID = 7
	MsgCancel        messageID = 8
)

type messageID uint8

// *** FROM BEP3 ***
// "The peer wire protocol consists of a handshake followed by a never-ending stream of length-prefixed messages."
// "Messages of length zero are keepalives, and ignored"
// "All non-keepalive messages start with a single byte which gives their type."

type Message struct {
	ID      messageID
	Payload []byte
}

/*
4 bytes           1 byte     n bytes
[length prefix][message ID][payload... .. .]
*/
func (m *Message) serialize() []byte {
	if m == nil { // If m = nil, return a 4-byte slice of zeros (Keep alive)
		return make([]byte, 4)
	}

	// Single byte that gives type
	// Payload
	length := uint32(1 + len(m.Payload))    // 1 byte for message ID + length of payload
	buf := make([]byte, 4+length)           // add 4 bytes for length prefix
	binary.BigEndian.PutUint32(buf, length) // length prefix
	buf[4] = byte(m.ID)                     // message ID
	copy(buf[5:], m.Payload)                // payload

	return buf
}

// ---- Create messages -------
func NewChoke() *Message             { return &Message{ID: MsgChoke} }
func NewUnchoke() *Message           { return &Message{ID: MsgUnchoke} }
func NewInterested() *Message        { return &Message{ID: MsgInterested} }
func NewNotInterested() *Message     { return &Message{ID: MsgNotInterested} }
func NewBitfield(bf []byte) *Message { return &Message{ID: MsgBitfield, Payload: bf} }

func NewHave(index int) *Message {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(index))
	return &Message{ID: MsgHave, Payload: payload}
}

func NewRequest(index int, begin, length int) *Message {
	message := &Message{ID: MsgRequest}
	// 4 byte index, 4 byte begin, 4 byte length
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	message.Payload = payload
	return message
}

func NewPiece(index int, begin int, data []byte) *Message {
	message := &Message{ID: MsgPiece}
	payload := make([]byte, 8+len(data)) // 4B Index, 4B being
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	copy(payload[8:], data)
	message.Payload = payload
	return message

}

func NewCancel(index int, begin, length int) *Message {
	message := NewRequest(index, begin, length) // Cancel has the same payload as Request
	message.ID = MsgCancel
	return message
}

// ---- Read messages & Handshake -------

/*
4 bytes           1 byte     n bytes
[length prefix][message ID][payload... .. .]
*/
func ReadMessage(reader io.Reader) (*Message, error) {
	// Read message id and then create slice size?
	var lengthBuf [4]byte
	if _, err := io.ReadFull(reader, lengthBuf[:]); err != nil {
		return nil, err // pass io.EOF through unwrapped so callers can detect a clean close
	}
	length := binary.BigEndian.Uint32(lengthBuf[:])
	if length == 0 {
		return nil, nil // Keep-alive
	}
	if length > maxMessageLen {
		return nil, fmt.Errorf("message too large: %d bytes", length)
	}

	// Rest
	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, fmt.Errorf("read message body: %w", err)
	}
	return &Message{ID: messageID(buf[0]), Payload: buf[1:]}, nil

}

// ---- Parse Messages -------
