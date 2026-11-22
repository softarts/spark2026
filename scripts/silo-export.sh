#!/usr/bin/env bash
# Copy the objects of a Silo bucket to the Mac as ordinary files.
#
# Silo stores data inside the Docker Desktop VM (local-path PVC), in MinIO's
# on-disk format (one directory + xl.meta per object), so the files cannot be
# opened directly from macOS. This script downloads the *logical* objects
# through Silo's bundled `mcli` client.
#
# Usage: scripts/silo-export.sh [bucket] [prefix]
#   default bucket: spark-data; output: ./data/silo-export/<bucket>/<key>
set -euo pipefail
source "$(dirname "$0")/common.sh"

BUCKET="${1:-$SILO_BUCKET}"
PREFIX="${2:-}"
OUT="$LAB_ROOT/data/silo-export/$BUCKET"

require_tools kubectl python3

# Runs mcli inside the Silo Pod, using the Pod's own root credentials.
mcli() {
  kc -n "$LAB_NAMESPACE" exec deploy/silo -- sh -c \
    'export MC_HOST_local="http://$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD@127.0.0.1:9000"; exec mcli "$@"' mcli "$@"
}

keys=$(mcli ls --recursive --json "local/$BUCKET/$PREFIX" \
  | python3 -c 'import json,sys
for line in sys.stdin:
    o = json.loads(line)
    if o.get("status") == "success" and o.get("type") == "file":
        print(o["key"])')

[ -n "$keys" ] || die "no objects under s3://$BUCKET/$PREFIX"

count=0
while IFS= read -r key; do
  dest="$OUT/$PREFIX$key"
  mkdir -p "$(dirname "$dest")"
  mcli cat "local/$BUCKET/$PREFIX$key" > "$dest"
  count=$((count + 1))
done <<< "$keys"

log "Exported $count objects to $OUT"
