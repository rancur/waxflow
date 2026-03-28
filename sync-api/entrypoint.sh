#!/bin/bash
set -e

echo "Initializing database..."
python init_db.py

echo "Starting sync-api on port 8402..."
exec uvicorn main:app --host 0.0.0.0 --port 8402
