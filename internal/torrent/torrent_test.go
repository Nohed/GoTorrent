package torrent

import (
	"crypto/sha1"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testAnnounce = "http://tracker.test/announce"

// Creates a fake piece using sha1 (20 bytes)
func fakePieces(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		h := sha1.Sum([]byte{byte(i)})
		sb.Write(h[:])
	}
	return sb.String()
}
// buildInfo builds a bencoded info dictionary 
func buildInfo(length, pieceLength int, pieces string) string {
	return fmt.Sprintf("d6:lengthi%de4:name8:test.iso12:piece lengthi%de6:pieces%d:%se",
		length, pieceLength, len(pieces), pieces)
}

// Helper to build torrent byte 
func buildTorrent(info string) []byte {
	return []byte(fmt.Sprintf("d8:announce%d:%s4:info%se", len(testAnnounce), testAnnounce, info))
}

//Test parse uneven length fit of pieces in total lenght
func TestParse_Valid(t *testing.T) {
	pieces := fakePieces(3)
	info := buildInfo(100, 40, pieces)
	data := buildTorrent(info)

	got, err := Parse(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.Announce != testAnnounce {
		t.Errorf("Announce = %q, want %q", got.Announce, testAnnounce)
	}
	if got.Name != "test.iso" {
		t.Errorf("Name = %q, want %q", got.Name, "test.iso")
	}
	if got.Length != 100 {
		t.Errorf("Length = %d, want 100", got.Length)
	}
	if got.PieceLength != 40 {
		t.Errorf("PieceLength = %d, want 40", got.PieceLength)
	}
	if len(got.PieceHashes) != 3 {
		t.Fatalf("len(PieceHashes) = %d, want 3", len(got.PieceHashes))
	}
	for i, h := range got.PieceHashes {
		want := pieces[i*hashLength : (i+1)*hashLength]
		if string(h[:]) != want {
			t.Errorf("PieceHashes[%d] = %q, want %q", i, h[:], want)
		}
	}
}

func TestParse_InfoHashIsSHA1OfRawInfo(t *testing.T) {
	info := buildInfo(100, 40, fakePieces(3))
	got, err := Parse(buildTorrent(info))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := sha1.Sum([]byte(info))
	if got.InfoHash != want {
		t.Errorf("InfoHash = %x, want %x", got.InfoHash, want)
	}
}

func TestParse_ExactMultipleOfPieceLength(t *testing.T) {
	// 80 / 40 = exactly 2 pieces, no remainder piece.
	data := buildTorrent(buildInfo(80, 40, fakePieces(2)))
	got, err := Parse(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got.PieceHashes) != 2 {
		t.Errorf("len(PieceHashes) = %d, want 2", len(got.PieceHashes))
	}
}

func TestParse_Errors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty data", []byte{}},
		{"not bencode", []byte("this is not a torrent")},
		{"missing info section", []byte("d8:announce3:fooe")},
		{"zero piece length", buildTorrent(buildInfo(100, 0, fakePieces(3)))},
		{"negative piece length", buildTorrent(buildInfo(100, -5, fakePieces(3)))},
		{"zero file length", buildTorrent(buildInfo(0, 40, fakePieces(3)))},
		{"negative file length", buildTorrent(buildInfo(-1, 40, fakePieces(3)))},
		{"pieces not multiple of 20", buildTorrent(buildInfo(100, 40, strings.Repeat("x", 25)))},
		{"empty pieces", buildTorrent(buildInfo(100, 40, ""))},
		{"too few pieces", buildTorrent(buildInfo(100, 40, fakePieces(2)))},
		{"too many pieces", buildTorrent(buildInfo(100, 40, fakePieces(4)))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.data)
			if err == nil {
				t.Errorf("expected error, got torrent %+v", got)
			}
			if got != nil {
				t.Errorf("expected nil torrent on error, got %+v", got)
			}
		})
	}
}

func TestParseFile(t *testing.T) {
	t.Run("valid file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.torrent")
		data := buildTorrent(buildInfo(100, 40, fakePieces(3)))
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("setup: %v", err)
		}

		got, err := ParseFile(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Name != "test.iso" || got.Length != 100 {
			t.Errorf("unexpected torrent: %+v", got)
		}
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := ParseFile(filepath.Join(t.TempDir(), "does-not-exist.torrent"))
		if err == nil {
			t.Error("expected error for missing file, got nil")
		}
	})

	t.Run("invalid contents", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bad.torrent")
		if err := os.WriteFile(path, []byte("garbage"), 0o600); err != nil {
			t.Fatalf("setup: %v", err)
		}
		if _, err := ParseFile(path); err == nil {
			t.Error("expected error for invalid contents, got nil")
		}
	})
}

func TestSplitPieceHashes(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		pieces := fakePieces(3)
		hashes, err := splitPieceHashes(pieces)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(hashes) != 3 {
			t.Fatalf("got %d hashes, want 3", len(hashes))
		}
		for i, h := range hashes {
			want := pieces[i*hashLength : (i+1)*hashLength]
			if string(h[:]) != want {
				t.Errorf("hashes[%d] = %x, want %x", i, h[:], want)
			}
		}
	})

	t.Run("errors", func(t *testing.T) {
		for name, in := range map[string]string{
			"empty":        "",
			"too short":    "abc",
			"off by one":   strings.Repeat("x", hashLength+1),
			"not multiple": strings.Repeat("x", hashLength*2-1),
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := splitPieceHashes(in); err == nil {
					t.Errorf("expected error for length %d, got nil", len(in))
				}
			})
		}
	})
}