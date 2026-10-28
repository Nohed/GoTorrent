#!/usr/bin/env bash
set -e

# --- edit these ---
OPERATE_DIR="${1:-testdata/client1F}"
     # folder name, or pass it as the first argument
FILENAME="debian.iso"           
PORT=6882                     

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$OPERATE_DIR"
rm -f "$OPERATE_DIR/$FILENAME"
cp -f "$SCRIPT_DIR/splits/split1.iso" "$OPERATE_DIR/$FILENAME"	#make sure to run splitFile.py before to create the split file 
echo "Copied split1.iso -> $OPERATE_DIR/$FILENAME"


go build -o client ./cmd/client

exec ./client \
  -torrent "./testdata/online.torrent" \
  -out "$OPERATE_DIR/$FILENAME" \
  -port "$PORT" \
  -peers 50 \
  -seed=true
# Replace with your real command, e.g.:
# go run ../cmd/gotorrent -torrent ../debian.torrent -out "$FILENAME" -port "$PORT"