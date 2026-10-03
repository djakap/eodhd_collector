#!/bin/bash
# Copy QuestDB backup files from prefect-worker container to local backups/ folder.
# Needed because Docker Desktop + WSL2 bind mounts are not directly accessible from WSL.
#
# Usage:
#   ./scripts/copy_backup.sh            # copy all missing backups
#   ./scripts/copy_backup.sh --latest   # copy only the latest backup

set -e

CONTAINER="prefect-worker"
BACKUP_DIR="$(cd "$(dirname "$0")/.." && pwd)/backups"
mkdir -p "$BACKUP_DIR"

echo "=== QuestDB Backup Copy ==="
echo "Container : $CONTAINER"
echo "Destination: $BACKUP_DIR"
echo ""

# Check container is running
if ! docker inspect --format '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -q true; then
    echo "ERROR: Container '$CONTAINER' is not running."
    exit 1
fi

# List backup files in container
FILES=$(docker exec "$CONTAINER" sh -c 'ls /backup/questdb_backup_*.tar.gz 2>/dev/null | sort' 2>/dev/null)
if [ -z "$FILES" ]; then
    echo "No backup files found in container at /backup/"
    exit 1
fi

# If --latest flag, only take the last file
if [ "$1" = "--latest" ]; then
    FILES=$(echo "$FILES" | tail -1)
fi

COPIED=0
SKIPPED=0
MANIFEST_COPIED=0
MANIFEST_SKIPPED=0
MISSING_MANIFEST=0

while IFS= read -r FILEPATH; do
    FILENAME=$(basename "$FILEPATH")
    DEST="$BACKUP_DIR/$FILENAME"
    MANIFEST_FILENAME="${FILENAME%.tar.gz}.manifest.json"
    MANIFEST_PATH="$(dirname "$FILEPATH")/$MANIFEST_FILENAME"
    MANIFEST_DEST="$BACKUP_DIR/$MANIFEST_FILENAME"

    if [ -f "$DEST" ]; then
        echo "SKIP  $FILENAME (already exists)"
        SKIPPED=$((SKIPPED + 1))
    else
        echo -n "COPY  $FILENAME ... "
        docker exec "$CONTAINER" cat "$FILEPATH" > "$DEST"
        SIZE=$(du -h "$DEST" | cut -f1)
        echo "done ($SIZE)"
        COPIED=$((COPIED + 1))
    fi

    if ! docker exec "$CONTAINER" test -f "$MANIFEST_PATH"; then
        echo "ERROR $MANIFEST_FILENAME is missing in the container"
        MISSING_MANIFEST=$((MISSING_MANIFEST + 1))
    elif [ -f "$MANIFEST_DEST" ]; then
        echo "SKIP  $MANIFEST_FILENAME (already exists)"
        MANIFEST_SKIPPED=$((MANIFEST_SKIPPED + 1))
    else
        echo -n "COPY  $MANIFEST_FILENAME ... "
        docker exec "$CONTAINER" cat "$MANIFEST_PATH" > "$MANIFEST_DEST"
        echo "done"
        MANIFEST_COPIED=$((MANIFEST_COPIED + 1))
    fi
done <<< "$FILES"

echo ""
echo "Done. Copied: $COPIED | Skipped (already exist): $SKIPPED"
echo "Manifests copied: $MANIFEST_COPIED | Skipped: $MANIFEST_SKIPPED | Missing: $MISSING_MANIFEST"
echo "Files in $BACKUP_DIR:"
ls -lh "$BACKUP_DIR"/questdb_backup_* 2>/dev/null || echo "  (none)"

if [ "$MISSING_MANIFEST" -gt 0 ]; then
    exit 1
fi
