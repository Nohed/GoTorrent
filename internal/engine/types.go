package engine

const (
	EventPeerReady EventKind = iota // Peer creatred and ready = send bitfield
	EventPeerClosed
	EventPeerInterested
	EventPeerNotInterested
	EventPeerChoked
	EventPeerUnchoked
	EventHave
	EventBlockReceived
	EventBlockUploaded
	EventReadDone
	EventBlockRequested
	EventBitfield
)
const (
	commandInterested CommandKind = iota
	commandNotInterested
	commandChoke
	commandUnchoke
	commandHave
	commandRequestBlock
	commandCancelBlock
	commandSend
)

type CommandKind uint8
type EventKind uint8

// Commands passed to workers	(one channel per peer)
type Command struct {
	Cmd        CommandKind
	PieceIndex int // For request, cancel etc
	Begin      int // --||--
	Length     int
	Data       []byte
}

// events got from workers	(one shared channel for all peers)
type Event struct {
	Eve      EventKind
	SenderID [20]byte //Creater of event
	Err      error

	PieceIndex int
	Begin      int
	Length     int
	Data       []byte
}
