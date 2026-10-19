package engine

import (
	"context"
	"errors"
	"net"
	"time"
)

func (m *Manager) runListener(ln net.Listener) {
	stop := context.AfterFunc(m.ctx, func() { _ = ln.Close() })
	defer stop()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if m.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			m.log.Warn("accept failed", "error", err)
			if !m.waitFor(time.Second) { // avoid a hot loop on persistent errors (e.g. fd exhaustion)
				return
			}
			continue
		}
		m.acceptPeer(conn)
	}
}
