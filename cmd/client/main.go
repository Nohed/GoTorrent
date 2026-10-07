package main

import (
	"fmt"

	"github.com/Nohed/GoTorrent/internal/torrent"
)

const torrentName = "testdata/ubuntu-26.04.1-desktop-amd64.iso.torrent"

func main() {
	t, bt := torrent.ParseFile(torrentName)
	if t == nil {
		panic(bt)
	}
	fmt.Println("Torrent parsed successfully")
	fmt.Println("Announce URL:    ", t.Announce)
	fmt.Printf("Info Hash:        %x\n", t.InfoHash)
	fmt.Println("Piece Length:    ", t.PieceLength)
	fmt.Println("Total Length:    ", t.Length)
	fmt.Println("File Name:       ", t.Name)
	fmt.Println("Number of Pieces:", len(t.PieceHashes))
}
