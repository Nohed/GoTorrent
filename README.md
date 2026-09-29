# GoTorrent
Bittorrent client written in GO.


# project plan



## Phase 1: Bencode & Torrent Parsing
**build** internal/bencode/bencode.go & internal/torrent/torrent.go

**Goal:** parse .torrent filesi nto go structs

## Phase 2: Manage pieces and storage
**Build:** internal/storage/piece.go & internal/storage/validator.go

**goal:** Manage writing chunks to disk and cryptographic validation.

## Phase 3: Peer to peer communication

## Phase 4: Tracker

## Phase 5: Choking mechanism,

## Phase 5: Engine and full clients
**Build:** internal/engine/client.go & cmd/client/main.go

**Goal:** Connect all packages into a functional downloading and seeding client.

## Phase 6: Fault tollerance and error handling


## Phase 7: Docker, Kubernetes?
