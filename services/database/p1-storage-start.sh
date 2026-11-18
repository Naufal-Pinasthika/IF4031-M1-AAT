#!/bin/sh
# p1: run the official entrypoint, then persist the actual Kubernetes node on every start.
set -eu
rm -f /tmp/p1-storage-ready
docker-entrypoint.sh postgres &
postgres_pid=$!
trap 'kill -TERM "$postgres_pid" 2>/dev/null || true; wait "$postgres_pid" || true; exit 0' TERM INT
until pg_isready -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
  kill -0 "$postgres_pid" 2>/dev/null || { wait "$postgres_pid"; exit 1; }
  sleep 1
done
# p1: psql variables quote values; neither node names nor credentials become SQL code.
PGPASSWORD="$POSTGRES_PASSWORD" psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -v ON_ERROR_STOP=1 --set=unit_name="$P1_UNIT_NAME" --set=node_name="$NODE_NAME" <<'SQL'
CREATE TABLE IF NOT EXISTS p1_metadata (
 singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),
 unit_name TEXT NOT NULL,node_name TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'new',
 checkpoint TEXT NOT NULL DEFAULT '',copied BIGINT NOT NULL DEFAULT 0
);
INSERT INTO p1_metadata(singleton,unit_name,node_name)
 VALUES (true, :'unit_name', :'node_name')
 ON CONFLICT(singleton) DO UPDATE SET unit_name=EXCLUDED.unit_name,node_name=EXCLUDED.node_name;
SQL
touch /tmp/p1-storage-ready
wait "$postgres_pid"
