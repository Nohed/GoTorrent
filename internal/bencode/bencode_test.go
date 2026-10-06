package bencode

import (
	"reflect"
	"testing"
)

type testInfo struct {
    Pieces      string `bencode:"pieces"`
    PieceLength int    `bencode:"piece length"`
    Length      int    `bencode:"length"`
    Name        string `bencode:"name"`
}

//Test we can extract specifc fields without changing anything
func TestRawMessagePreservesBytes(t *testing.T) {
    info := "d4:name4:test6:lengthi100ee"

    data := []byte("d4:info" + info + "e")

    var torrent struct {
        Info RawMessage `bencode:"info"`
    }

    if err := Unmarshal(data, &torrent); err != nil {
        t.Fatalf("Unmarshal failed: %v", err)
    }

    if string(torrent.Info) != info {
        t.Errorf("got %q, want %q", torrent.Info, info)
    }
}


//test that our data is kept the same after roundtrip
func TestMarshalUnmarshal(t *testing.T) {
    input := testInfo{
        Pieces:      "some piece hashes",
        PieceLength: 16384,
        Length:      32768,
        Name:        "test.txt",
    }

    data, err := Marshal(input)
    if err != nil {
        t.Fatalf("Marshal failed: %v", err)
    }

    var output testInfo
    if err := Unmarshal(data, &output); err != nil {
        t.Fatalf("Unmarshal failed: %v", err)
    }

    if !reflect.DeepEqual(input, output) {
        t.Errorf("got %+v, want %+v", output, input)
    }
}

