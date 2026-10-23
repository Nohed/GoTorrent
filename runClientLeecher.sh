
#!/bin/bash
set -e

go build -o client ./cmd/client

rm -f /testdata/leech/ubuntu.iso	#make sure we dont ahve the file 

exec ./client \
    -torrent "./testdata/online.torrent" \
    -out "./testdata/leech/ubuntu.iso" \
    -port 6881 \
    -peers 50 \
    -seed=true \