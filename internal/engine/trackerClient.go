package engine

import (
	"net"
	"strconv"
	"time"

	"github.com/Nohed/GoTorrent/internal/tracker"
)

func (m *Manager) runTracker() {
	for {
		response, err := m.trackerClient.Announce(m.ctx, tracker.AnnounceRequest{
			InfoHash:   m.torrent.InfoHash,
			PeerID:     m.peerID,
			Port:       m.port,
			Uploaded:   int64(m.uploaded.Load()),
			Downloaded: int64(m.downloaded.Load()),
			Left:       m.left(),
		})

		if err != nil {
			if m.ctx.Err() != nil {
				return
			}
			m.log.Warn("tracker announce failed", "error", err)
			if !m.waitFor(time.Minute) {
				return
			}
			continue
		}
		m.log.Info("received peers from tracker", "count", len(response.Peers))

		for _, p := range response.Peers {
			if m.ctx.Err() != nil {
				return
			}

			addr := net.JoinHostPort(
				p.IP.String(),
				strconv.Itoa(int(p.Port)),
			)
			m.connectToPeer(addr)
		}

		interval := response.Interval
		if interval <= 0 {
			interval = time.Minute
		}
		if !m.waitFor(interval) {
			return
		}
	}
}
