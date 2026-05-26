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

assert_absent() {
    local label="$1" unexpected="$2" actual="$3"
    if printf '%s' "$actual" | grep -qF -- "$unexpected"; then
        echo "  FAIL: $label"
        echo "        did not expect: $unexpected"
        echo "        actual: $actual"
        fail=$((fail + 1))
    else
        echo "  PASS: $label"
        pass=$((pass + 1))
    fi
}

q() { psql "$CONN" -c "$1" 2>&1; }

# Drives two interleaved sessions that both insert the same id at the same snapshot, then
# commit one after the other. Echoes both sessions' combined output. First-committer-wins:
# one COMMIT succeeds, the other reports a write-write conflict.
concurrent_conflict_output() {
    local dir
    dir="$(mktemp -d)"
    local fa="$dir/a.fifo" fb="$dir/b.fifo"
    mkfifo "$fa" "$fb"

    psql "$CONN" -f "$fa" >"$dir/out_a" 2>&1 &
    local pa=$!
    psql "$CONN" -f "$fb" >"$dir/out_b" 2>&1 &
    local pb=$!

    exec 3>"$fa" 4>"$fb"
    printf "BEGIN;\nINSERT INTO person (id, full_name) VALUES ('conc','sessionA');\n" >&3
    printf "BEGIN;\nINSERT INTO person (id, full_name) VALUES ('conc','sessionB');\n" >&4
    sleep 0.5
    printf "COMMIT;\n" >&3 # A commits first -> wins
    sleep 0.5
    printf "COMMIT;\n" >&4 # B commits second -> conflicts
    sleep 0.3
    exec 3>&- 4>&-
    wait "$pa" "$pb" 2>/dev/null

    cat "$dir/out_a" "$dir/out_b"
    rm -rf "$dir"
}

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

# --- Transactions (M-T1..M-T4) ---
psql "$CONN" >/dev/null 2>&1 <<'EOF'
BEGIN;
INSERT INTO person (id, full_name) VALUES ('tx-commit', 'CommittedInTx');
COMMIT;
EOF
assert_contains "BEGIN/COMMIT persists" "CommittedInTx" "$(q "SELECT * FROM person")"

psql "$CONN" >/dev/null 2>&1 <<'EOF'
BEGIN;
INSERT INTO person (id, full_name) VALUES ('tx-rollback', 'RolledBackInTx');
ROLLBACK;
EOF
assert_absent "BEGIN/ROLLBACK discards" "RolledBackInTx" "$(q "SELECT * FROM person")"

# Read-your-writes: the in-transaction SELECT sees the not-yet-committed insert.
ryw="$(psql "$CONN" 2>&1 <<'EOF'
BEGIN;
INSERT INTO person (id, full_name) VALUES ('tx-ryw', 'ReadYourWrites');
SELECT * FROM person;
ROLLBACK;
EOF
)"
assert_contains "read-your-writes inside a tx" "ReadYourWrites" "$ryw"

# Failed transaction: an error aborts the block; later statements are rejected; COMMIT rolls back.
failed="$(psql "$CONN" 2>&1 <<'EOF'
BEGIN;
SELECT * FROM nonexistent;
INSERT INTO person (id, full_name) VALUES ('tx-fail', 'ShouldNotPersist');
COMMIT;
EOF
)"
assert_contains "aborted tx rejects later statements" "current transaction is aborted" "$failed"
assert_absent "aborted tx persists nothing" "ShouldNotPersist" "$(q "SELECT * FROM person")"

# Concurrent first-committer-wins.
conc="$(concurrent_conflict_output)"
assert_contains "concurrent: one session commits" "COMMIT" "$conc"
assert_contains "concurrent: the other conflicts" "Write-write conflict" "$conc"

echo
echo "RESULT: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
