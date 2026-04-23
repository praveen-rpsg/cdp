#!/bin/sh
set -eu

# The checked-in prod env file uses PG_* names for the app.
# Export the POSTGRES_* variables expected by the official image at runtime.
export POSTGRES_USER="${POSTGRES_USER:-${PG_USER:-}}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-${PG_PASSWORD:-}}"
export POSTGRES_DB="${POSTGRES_DB:-${PG_DB:-}}"

exec docker-entrypoint.sh "$@"
