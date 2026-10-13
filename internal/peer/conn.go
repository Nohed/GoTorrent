package peer

import (
	"errors"
	"net"
	"sync"
	"time"
)

const (
	connectTimeout   = 5 * time.Second
	HandshakeTimeout = 10 * time.Second
	readTimeout      = 3 * time.Minute
	writeTimeout     = 10 * time.Second
)

type Connection struct {
	Addr   string
	PeerID [20]byte // the remote peer's ID, from its handshake

	conn    net.Conn
	writeMu sync.Mutex // Send may be called from several goroutines
}

func Connect(addr string, infoHash, peerID [20]byte) (*Connection, error) {
	conn, err := net.DialTimeout("tcp", addr, connectTimeout)
	if err != nil {
		return nil, err
	}

	// INitiate handshake
	conn.SetDeadline(time.Now().Add(HandshakeTimeout))

	OutgoingHandshake := NewHandshake(infoHash, peerID)
	if _, err := conn.Write(OutgoingHandshake.Serialize()); err != nil {
		conn.Close()
		return nil, err
	}

	IncomingHandshake, err := ReadHandshake(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if IncomingHandshake.InfoHash != infoHash {
		conn.Close()
		return nil, errors.New("info hash mismatch")
	}
	// Tracker will contain our own IP.
	if IncomingHandshake.PeerID == peerID {
		conn.Close()
		return nil, errors.New("connected to ourselves")
	}

	conn.SetDeadline(time.Time{}) // clear it; per-operation deadlines from here on
	return &Connection{Addr: addr, PeerID: IncomingHandshake.PeerID, conn: conn}, nil
}

// Accept a incoming connection and handshake
func Accept(conn net.Conn, infoHash, peerID [20]byte) (*Connection, error) {
	conn.SetDeadline(time.Now().Add(HandshakeTimeout))

	// Read handshake from remote peer
	IncomingHandshake, err := ReadHandshake(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if IncomingHandshake.InfoHash != infoHash {
		conn.Close()
		return nil, errors.New("info hash mismatch")
	}

	// Tracker will contain our own IP.
	if IncomingHandshake.PeerID == peerID {
		conn.Close()
		return nil, errors.New("connected to ourselves")
	}

	// Send handshake to remote peer
	OutgoingHandshake := NewHandshake(infoHash, peerID)
	if _, err := conn.Write(OutgoingHandshake.Serialize()); err != nil {
		conn.Close()
		return nil, err
	}
	conn.SetDeadline(time.Time{})
	return &Connection{Addr: conn.RemoteAddr().String(), PeerID: IncomingHandshake.PeerID, conn: conn}, nil
}

func (c *Connection) Send(m *Message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := c.conn.Write(m.serialize())
	return err
}

func (c *Connection) Receive() (*Message, error) {
	c.conn.SetReadDeadline(time.Now().Add(readTimeout))
	return ReadMessage(c.conn)
}

func (c *Connection) Close() error { return c.conn.Close() }
