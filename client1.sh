#!/usr/bin/env bash
set -e

OPERATE_DIR="testdata/client1F"
FILENAME="debian.iso"
PORT=6882

mkdir -p "$OPERATE_DIR"
rm -f "$OPERATE_DIR/$FILENAME"
cp -f ./splits/split1.iso "$OPERATE_DIR/$FILENAME"
echo "Copied split1.iso -> $OPERATE_DIR/$FILENAME"

go build -o client ./cmd/client

exec ./client \
  -torrent "./testdata/online.torrent" \
  -out "$OPERATE_DIR/$FILENAME" \
  -port "$PORT" \
  -peers 50 \
  -seed=true