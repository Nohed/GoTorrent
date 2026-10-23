
#!/bin/bash
set -e

go build -o client ./cmd/client

exec ./client \
    -torrent ".testdata/online.torrent" \
    -out ".testdata/seed/ubuntu.iso" \
    -port 6881 \
    -peers 50 \
    -seed=true \
