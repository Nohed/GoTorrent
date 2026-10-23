
#!/bin/bash
set -e

go build -o tracker ./cmd/tracker
exec ./tracker
