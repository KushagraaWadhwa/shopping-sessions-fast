#!/bin/sh
set -eu
export HOME="${HOME:-/scratch}"
export TMPDIR="${SCRATCH_DIR:-/scratch}"
exec python /app/pipeline/pipeline.py

