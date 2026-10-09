package peer

import (
	"net"
	"sync"
)

type Conn struct {
	Addr   string
	PeerID [20]byte // the remote peer's ID, from its handshake

	conn    net.Conn
	writeMu sync.Mutex // Send may be called from several goroutines
}

func Connect(addr string, infoHash, peerID [20]byte) (*Conn, error) {}

func Accept(nc net.Conn, infoHash, peerID [20]byte) (*Conn, error) {}

func (c *Conn) Send(m *Message) error {}

func (c *Conn) Receive() (*Message, error) {}

func (c *Conn) Close() error { return c.conn.Close() }
