package tracker

/*
FROM BEP3
- Tracker responses are bencoded dictionaries
- If a tracker response has a key failure reason,
	then that maps to a human readable string which explains why the query failed,

- If info hash is correct,
	the tracker shall respond with two keys.
		* interval - which maps to the number of seconds the downloader should wait between regular rerequests
		* peers    - maps to a list of dictionaries corresponding to peers, each of which contains the keys peer
			 		id, ip, and port, which map to the peer's self-selected ID, IP address or dns name as a string,
					and port number, respectively


FROM BEP23
- To reduce the size of tracker responses and to reduce memory and computational requirements in trackers,
  trackers may return peers as a packed string rather than as a bencoded list.

  - In BEP3 Each dict in the list contains three fields: peer id, ip, and port.
  - The peer id is 20 bytes plus 3 bytes bencoding overhead
  - The ip is a string containing a domain name or an IP address, and an integer port number.
	- IP is variable length but its longest form is qa domain name and 255 bytes (+4 byte bencode overhead)
	- Port number is 7 bytes including bencoding overhead.
  - For a total of N*(289) Bytes with N peers.

- BEP23 introduces a compact form in which each peer is 6 bytes.
	- first 4 byte contains 32-bit ipv4 adress
	- Two byte port number

	- Peer id is sent in handshake

	By including compact=1 in the announce URL, we want compact
	compact = 0, we want old
*/

// TODO: There is a optional key "event" for startedf, completed, stopped. include in announce
import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Nohed/GoTorrent/internal/bencode"
)

const MaxPeersPerSwarm = 32

type peerEntry struct {
	ip         net.IP
	port       uint16
	left       int64
	uploaded   int64 // for stats
	downloaded int64 // For state, not needed by protocl
	lastSeen   time.Time
}

type Server struct {
	mu       sync.Mutex
	swarms   map[[20]byte]map[[20]byte]*peerEntry // key outermap(info hash) key innner(Peer id)
	interval time.Duration                        // Announce interval
	now      func() time.Time                     // replaceable in tests
	log      *slog.Logger
}

type announceResponse struct {
	Interval int    `bencode:"interval"`
	Peers    string `bencode:"peers"` // compact: 6 bytes per peer
}

type failureResponse struct {
	Reason string `bencode:"failure reason"`
}

// interval is reasonably 10 - 30sec for us

func NewServer(interval time.Duration, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &Server{
		swarms:   make(map[[20]byte]map[[20]byte]*peerEntry),
		interval: interval,
		now:      time.Now,
		log:      logger,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/announce", s.handleAnnounce)
	return mux
}

// Announce with query params
// 1. clients peer id
// Only accept compact resposnes
func (s *Server) handleAnnounce(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	infoHash, ok := to20Byte(query.Get("info_hash"))
	if !ok {
		s.fail(w, r, "invalid info_hash")
		return
	}
	peerID, ok := to20Byte(query.Get("peer_id"))
	if !ok {
		s.fail(w, r, "invalid peer_id")
		return
	}
	port, err := strconv.ParseUint(query.Get("port"), 10, 16) // 16 bit base 10
	if err != nil {
		s.fail(w, r, "invalid port")
		return
	}

	left, err := strconv.ParseInt(query.Get("left"), 10, 64) // 64 bit base 10
	if err != nil {
		s.fail(w, r, err.Error())
		return
	}
	/* TODO: IMPLELEMTN STATS to document client traffic
	downloaded, err := strconv.ParseInt(query.Get("downloaded"), 10, 64)
	if err != nil {
		s.fail(w, r, err.Error())
		return
	}
	uploaded, err := strconv.ParseInt(query.Get("uploaded"), 10, 64)
	if err != nil {
		s.fail(w, r, err.Error())
	}
	*/
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		s.fail(w, r, "bad remote address")
		return
	}
	ipv4 := net.ParseIP(ip).To4()
	if ipv4 == nil {
		s.fail(w, r, "only IPv4 peers are supported")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock() // Release on function exit
	now := s.now()

	swarm := s.swarms[infoHash]
	if swarm == nil {
		swarm = make(map[[20]byte]*peerEntry)
		s.swarms[infoHash] = swarm
	}
	s.expire(swarm, now)

	// TODO: Optional Event

	// Check swarm size
	_, known := swarm[peerID]
	if !known && len(swarm) >= MaxPeersPerSwarm {
		s.fail(w, r, "swarm is full")
		return
	}
	// insert
	swarm[peerID] = &peerEntry{ip: ipv4, port: uint16(port), left: left, lastSeen: now}
	if !known {
		s.log.Info("Peer joined", "info_hash", hex.EncodeToString(infoHash[:]), "Peer",
			hex.EncodeToString(peerID[:4]), "Swarm size", len(swarm))
	}

	// TODO: OPTIMIZE
	peersToReturn := make([]*peerEntry, 0, len(swarm))
	for id, peer := range swarm {
		if id != peerID { // don't send the client its own address
			peersToReturn = append(peersToReturn, peer)
		}
	}
	s.respond(w, s.interval, peersToReturn)
}

// Drop peer that have missed two intervals, assume crash
func (s *Server) expire(swarm map[[20]byte]*peerEntry, now time.Time) {
	for id, peer := range swarm {
		if now.Sub(peer.lastSeen) > 2*s.interval {
			s.log.Info("Dropped client", "peer", hex.EncodeToString(id[:4]), "ip", peer.ip.String())
			delete(swarm, id)
		}
	}
}

func (s *Server) respond(w http.ResponseWriter, interval time.Duration, peers []*peerEntry) {
	buffer := make([]byte, 0, 6*len(peers)) // 6 Bytes peer per
	for _, peer := range peers {
		buffer = append(buffer, peer.ip...)
		buffer = binary.BigEndian.AppendUint16(buffer, peer.port)
	}
	body, err := bencode.Marshal(announceResponse{int(interval.Seconds()), string(buffer)})
	if err != nil {
		s.log.Error("Internal error mashalling message")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Write(body)
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, reason string) {
	s.log.Warn("Announce rejected", "Reason", reason, "Client", r.RemoteAddr)

	body, _ := bencode.Marshal(failureResponse{reason})
	w.Header().Set("Content-Type", "text/plain") // write bencode in plaintext
	w.Write(body)
}

func to20Byte(s string) ([20]byte, bool) {
	var res [20]byte
	if len(s) != 20 {
		return res, false
	}

	copy(res[:], s)
	return res, true
}
