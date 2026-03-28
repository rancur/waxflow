#!/bin/bash
# Backup Lexicon DJ database before sync operations
# Called by the sync-worker before any Lexicon writes

set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/app/data/lexicon-backups}"
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
MAX_BACKUPS=30

mkdir -p "$BACKUP_DIR"

# Lexicon stores its DB on the Mac Mini — we backup via the API
# This script is a safety net for direct file backups if mounted
LEXICON_DB="${LEXICON_DB_PATH:-}"

if [ -n "$LEXICON_DB" ] && [ -f "$LEXICON_DB" ]; then
    BACKUP_FILE="${BACKUP_DIR}/lexicon_${TIMESTAMP}.db"
    cp "$LEXICON_DB" "$BACKUP_FILE"

    # Verify backup integrity
    sqlite3 "$BACKUP_FILE" "PRAGMA integrity_check;" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "Backup created: $BACKUP_FILE ($(du -h "$BACKUP_FILE" | cut -f1))"
    else
        echo "ERROR: Backup integrity check failed!" >&2
        rm -f "$BACKUP_FILE"
        exit 1
    fi

    # Prune old backups (keep last MAX_BACKUPS)
    ls -1t "${BACKUP_DIR}"/lexicon_*.db 2>/dev/null | tail -n +$((MAX_BACKUPS + 1)) | xargs -r rm -f
else
    echo "No Lexicon DB path configured or file not found. Skipping file backup."
    echo "Lexicon backups are handled via the API."
fi
