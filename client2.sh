#!/bin/bash
set -e

OPERATE_DIR="${1:-testdata/client2F}"
FILENAME="debian.iso"
PORT=6881
# ------------------

cd "$(dirname "${BASH_SOURCE[0]}")"   # run from the repo root

mkdir -p "$OPERATE_DIR"
rm -f "$OPERATE_DIR/$FILENAME"
cp -f ./splits/split2.iso "$OPERATE_DIR/$FILENAME"		#make sure to run splitFile.py before to create the split file 
echo "Copied split2.iso -> $OPERATE_DIR/$FILENAME"

go build -o client ./cmd/client

exec ./client \
  -torrent "./testdata/online.torrent" \
  -out "$OPERATE_DIR/$FILENAME" \
  -port "$PORT" \
  -peers 50 \
  -seed=true