#!/usr/bin/env bash
#
# End-to-end verification for the lineagedb Postgres-wire server.
#
# Boots the server on a test port with a throwaway data dir, drives it with `psql`
# (SELECT/INSERT happy paths + error cases), asserts the output, and tears everything down
# reliably via an EXIT trap (so a failed run never leaves a server holding the port).
#
# Usage:  clients/postgres/verify.sh [port]
# Exit:   0 if every check passes, 1 otherwise.

set -uo pipefail

PORT="${1:-55433}"
HOST="127.0.0.1"
CONN="host=$HOST port=$PORT user=postgres dbname=lineagedb"
DATA="$(mktemp -d "${TMPDIR:-/tmp}/lineagedb-pg-verify.XXXXXX")"
BIN="target/debug/lineagedb-postgres"
SERVER_PID=""

cleanup() {
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    fi
    rm -rf "$DATA"
}
trap cleanup EXIT

# Run from the repo root (this script lives at clients/postgres/verify.sh).
cd "$(dirname "$0")/../.." || { echo "could not cd to repo root"; exit 1; }

if pg_isready -h "$HOST" -p "$PORT" -q 2>/dev/null; then
    echo "ERROR: something is already listening on $HOST:$PORT — pass a different port: $0 <port>" >&2
    exit 1
fi

echo "building postgres-server..."
cargo build -p postgres-server --quiet || { echo "build failed"; exit 1; }

echo "starting server on $PORT (data: $DATA)..."
"$BIN" --port "$PORT" --address "$HOST" --data "$DATA" >"$DATA/server.log" 2>&1 &
SERVER_PID=$!

# Wait for the server to accept connections (or bail if it dies on startup).
for _ in $(seq 1 50); do
    pg_isready -h "$HOST" -p "$PORT" -q 2>/dev/null && break
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "server exited during startup:"; cat "$DATA/server.log"; exit 1
    fi
    sleep 0.2
done

pass=0
fail=0

# assert_contains <label> <expected-substring> <actual-output>
assert_contains() {
    local label="$1" expected="$2" actual="$3"
    if printf '%s' "$actual" | grep -qF -- "$expected"; then
        echo "  PASS: $label"
        pass=$((pass + 1))
    else
        echo "  FAIL: $label"
        echo "        expected to contain: $expected"
        echo "        actual: $actual"
        fail=$((fail + 1))
    fi
}

q() { psql "$CONN" -c "$1" 2>&1; }

echo "running checks..."

# --- INSERT (M2) ---
assert_contains "insert with explicit id" "INSERT 0 1" \
    "$(q "INSERT INTO person (id, full_name, email) VALUES ('1', 'Alice', 'alice@example.com')")"
assert_contains "insert with generated id + NULL email" "INSERT 0 1" \
    "$(q "INSERT INTO person (full_name, email) VALUES ('Bob', NULL)")"
assert_contains "insert without a column list" "INSERT 0 1" \
    "$(q "INSERT INTO person VALUES ('3', 'Carol', 'carol@x.com')")"

# --- SELECT round-trip (M1 + M3) ---
selected="$(q "SELECT * FROM person")"
assert_contains "select shows Alice" "Alice" "$selected"
assert_contains "select shows Bob" "Bob" "$selected"
assert_contains "select shows Carol" "Carol" "$selected"
assert_contains "select returns 3 rows" "(3 rows)" "$selected"

# --- Errors (M4) ---
assert_contains "duplicate id is rejected" "ERROR" \
    "$(q "INSERT INTO person (id, full_name) VALUES ('1', 'Dup')")"
assert_contains "missing full_name is rejected (not-null)" "not-null" \
    "$(q "INSERT INTO person (id, email) VALUES ('9', 'x@y.com')")"
assert_contains "unknown table is rejected" "does not exist" \
    "$(q "SELECT * FROM widgets")"
assert_contains "unknown column is rejected" "does not exist" \
    "$(q "INSERT INTO person (id, age) VALUES ('9', '30')")"
assert_contains "malformed SQL is rejected" "ERROR" \
    "$(q "not valid sql")"

# --- Server survives errors ---
assert_contains "server still serves after errors" "Alice" \
    "$(q "SELECT * FROM person")"

# --- Catalog discovery (so GUI clients list the table + columns) ---
assert_contains "version() responds" "PostgreSQL" \
    "$(q "SELECT version()")"
assert_contains "table list shows person" "person" \
    "$(q "SELECT table_name, table_schema, table_type FROM information_schema.tables")"
assert_contains "columns list shows person columns" "full_name" \
    "$(q "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='person'")"

echo
echo "RESULT: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
