package main

import (
	"fmt"

	"github.com/Nohed/GoTorrent/internal/torrent"
)

const torrentName = "testdata/debian-13.7.0-amd64-netinst.iso.torrent"

func main() {
	man := newManager()
	man.start("asd.torrent")

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
