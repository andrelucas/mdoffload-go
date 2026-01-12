# mdoffload-go

Minimal gRPC server and CLI client implementing the `MDOffloadService` defined in `protos/mdoffload/v1/mdoffload.proto`. The server stores bucket and object attributes via a pluggable storage interface (current implementation: in-memory).

## Prerequisites
- Go 1.25.5+

## Run the server
Default bind: `127.0.0.1:8004`.

```bash
# From repository root
go run ./cmd/mdoffload-server --listen 127.0.0.1:8004
```

## Run the client
Defaults target `127.0.0.1:8004`. Add `--addr host:port` if you changed the server.

### Bucket commands
- Get: `go run ./cmd/mdoffload-client get-bucket --bucket-id b1`
- Set (merge): `go run ./cmd/mdoffload-client set-bucket --bucket-id b1 --add color=blue --add env=dev`
- Set (replace all): `go run ./cmd/mdoffload-client set-bucket --bucket-id b1 --replace --add owner=alice`
- Delete keys: `go run ./cmd/mdoffload-client set-bucket --bucket-id b1 --delete env`
- Purge: `go run ./cmd/mdoffload-client purge-bucket --bucket-id b1`

### Object commands
- Get: `go run ./cmd/mdoffload-client get-object --bucket-id b1 --object-key obj1`
- Set (merge): `go run ./cmd/mdoffload-client set-object --bucket-id b1 --object-key obj1 --add tier=gold`
- Set as new instance (clear first): `go run ./cmd/mdoffload-client set-object --bucket-id b1 --object-key obj1 --new-instance --add checksum=abc123`
- Delete keys: `go run ./cmd/mdoffload-client set-object --bucket-id b1 --object-key obj1 --delete tier`
- Purge: `go run ./cmd/mdoffload-client purge-object --bucket-id b1 --object-key obj1`

### Options
Common flags:
- `--addr host:port` (client) target server (default `127.0.0.1:8004`)
- `--timeout 5s` (client) per-RPC timeout
- `--listen host:port` (server) override bind address
- Bucket identification can be via `--bucket-id` (`-i`) or `--bucket-name` (`-b`) (optionally `--user-id` to scope names).
- Object identification requires `--object-key` (`-o`); `--instance-id` is optional (default instance used if omitted).

## Testing
```bash
go test ./...
```
