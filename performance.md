Run the client with pprof flag

Opens web UI on 8081

## CPU Profiling

Samples callstack 100 times per second for 60 seconds:

go tool pprof -http=:8081 "http://localhost:6060/debug/pprof/profile?seconds=20"

flat: Time spent directly executing instructions inside this specific function.

cum (cumulative): Time spent in this function plus all functions it called.

## Heap / Memory Profiling

### A. finding memory leaks with -inuse_space

go tool pprof -http=:8081 -inuse_space "http://localhost:6060/debug/pprof/heap"

### Finding High-Churn allocations -alloc_space

Shows the cumulative total bytes allocated over the lifetime of the program, even if already freed by GC.

go tool pprof -http=:8081 -alloc_space "http://localhost:6060/debug/pprof/heap"

## Goroutine profiling

curl "http://localhost:6060/debug/pprof/goroutine?debug=1"

- Check the count while downloading with 20 peers (e.g., total 45 goroutines).
- Stop the download or wait until it switches to seeding.
  Run curl again: the count should drop back down.

# Test command

go run cmd/client/main.go -torrent testdata/debian-13.7.0-amd64-netinst.iso.torrent -out /tmp/debian.iso -pprof
