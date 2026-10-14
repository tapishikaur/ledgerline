#!/usr/bin/env bash
# Regenerates the Go code for proto/ (committed under gen/). CI runs this and fails if the
# result differs from what's committed.
#
# Needs protoc $PROTOC_VERSION (scripts/env.sh). The Go plugins are pinned in go.mod as tools.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"
cd "$ROOT"

have="$(protoc --version | awk '{print $2}')"
if [ "$have" != "$PROTOC_VERSION" ]; then
  echo "protoc $PROTOC_VERSION needed (generated files record the version), found $have" >&2
  exit 1
fi

# The plugins at the versions pinned in go.mod.
go install google.golang.org/protobuf/cmd/protoc-gen-go google.golang.org/grpc/cmd/protoc-gen-go-grpc

protoc --proto_path=proto \
  --go_out=. --go_opt=module=github.com/tapishikaur/ledgerline \
  --go-grpc_out=. --go-grpc_opt=module=github.com/tapishikaur/ledgerline \
  proto/ledgerline/v1/ledgerline.proto
echo "generated: $(ls gen/ledgerline/v1)"
