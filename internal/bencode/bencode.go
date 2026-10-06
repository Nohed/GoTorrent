package bencode

// Bencode encoder/decoder
// Wrapper is built so underlying encoder/decoder later can be swapped out.
import (
	"bytes"

	bc "github.com/jackpal/bencode-go"
)

type RawMessage = bc.RawMessage // Alias for modularity

func Unmarshal(data []byte, v any) error {
	return bc.Unmarshal(bytes.NewReader(data), v)
}

func Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := bc.Marshal(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
