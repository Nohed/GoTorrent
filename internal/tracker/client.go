package tracker

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/Nohed/GoTorrent/internal/bencode"
)

type Peer struct {
	IP   net.IP
	Port uint16
}

type Client struct {
	trackerURL string
	httpClient *http.Client
}

type AnnounceRequest struct {
	InfoHash   [20]byte
	PeerID     [20]byte
	Port       uint16
	Uploaded   int64
	Downloaded int64
	Left       int64
}

type AnnounceResponse struct {
	Interval time.Duration
	Peers    []Peer
}

func NewClient(trackerURL string) *Client {
	return &Client{
		trackerURL: trackerURL,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

type wireResponse struct {
	Interval      int    `bencode:"interval"`
	Peers         string `bencode:"peers"`
	FailureReason string `bencode:"failure reason"`
}

func (c *Client) Announce(ctx context.Context, a AnnounceRequest) (*AnnounceResponse, error) {
	u, err := url.Parse(c.trackerURL)
	if err != nil {
		return nil, fmt.Errorf("parse tracker URL: %w", err)
	}

	q := u.Query()
	q.Set("info_hash", string(a.InfoHash[:]))
	q.Set("peer_id", string(a.PeerID[:]))
	q.Set("port", strconv.Itoa(int(a.Port)))
	q.Set("uploaded", strconv.FormatInt(a.Uploaded, 10))
	q.Set("downloaded", strconv.FormatInt(a.Downloaded, 10))
	q.Set("left", strconv.FormatInt(a.Left, 10))
	q.Set("compact", "1") // For now always compact
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, u.String(), nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create announce request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("announce to tracker: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tracker returned HTTP %s", resp.Status)
	}

	// Prevent unexpectedly large responses.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("read tracker response: %w", err)
	}

	var wire wireResponse
	if err := bencode.Unmarshal(body, &wire); err != nil {
		return nil, fmt.Errorf("bencode decode tracker response: %w", err)
	}

	if wire.FailureReason != "" {
		return nil, fmt.Errorf("tracker failure: %s", wire.FailureReason)
	}

	peers, err := decodeCompactPeers(wire.Peers)
	if err != nil {
		return nil, err
	}

	return &AnnounceResponse{
		Interval: time.Duration(wire.Interval) * time.Second,
		Peers:    peers,
	}, nil
}

func decodeCompactPeers(data string) ([]Peer, error) {
	// Compact IPv4 peers contain exactly 6 bytes each.
	if len(data)%6 != 0 {
		return nil, fmt.Errorf(
			"invalid compact peer list: %d bytes", len(data),
		)
	}

	peers := make([]Peer, 0, len(data)/6)

	for i := 0; i < len(data); i += 6 {
		ip := net.IPv4(
			data[i],
			data[i+1],
			data[i+2],
			data[i+3],
		)

		port := binary.BigEndian.Uint16(
			[]byte(data[i+4 : i+6]),
		)

		if port == 0 {
			continue
		}

		peers = append(peers, Peer{
			IP:   ip,
			Port: port,
		})
	}

	return peers, nil
}
