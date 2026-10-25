#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage:   $0 <output_download_path> <metrics_csv_path> [torrent_path] [port]"
    exit 1
fi

OUT_FILE="$1"
CSV_FILE="$2"
TORRENT_FILE="${3:-./testdata/online.torrent}"
PORT="${4:-6881}"

# 1. Compile client if needed
if [ ! -f "./client" ]; then
    go build -o client ./cmd/client
fi

# 2. Ensure output dir for CSV exists
mkdir -p "$(dirname "${CSV_FILE}")"

# 3. Clean any existing file so it starts from 0%
rm -f "${OUT_FILE}"

# 4. Run the leecher
./client \
    -torrent "${TORRENT_FILE}" \
    -out "${OUT_FILE}" \
    -port "${PORT}" \
    -peers 50 \
    -seed=false \
    -csv "${CSV_FILE}"

echo "Done! Metrics saved to ${CSV_FILE}"