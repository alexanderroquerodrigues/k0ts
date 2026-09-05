#!/usr/bin/env bash
# Smoke test: 3-node full mesh, fragmented sends, duplicate/out-of-order
# replication, tie-break, peer failure + recovery, close semantics.
set -u
cd "$(dirname "$0")/.."

BIN=./auction
PIDS=()

cleanup() {
    for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
}
trap cleanup EXIT

log() { echo "== $* =="; }

# Sends each line on ONE connection, one write per line (separate recv()s
# on the server side), then prints whatever the server sent back.
send_lines() {
    local host=$1 port=$2; shift 2
    exec 9<>"/dev/tcp/$host/$port"
    for line in "$@"; do
        printf '%s\n' "$line" >&9
        sleep 0.05
    done
    timeout 0.3 cat <&9
    exec 9<&- 9>&-
}

# Sends one command byte-by-byte on a single connection to prove framing
# doesn't assume recv() == one command, then a normal line after it.
send_fragmented_then() {
    local host=$1 port=$2 fragcmd=$3; shift 3
    exec 9<>"/dev/tcp/$host/$port"
    local n=${#fragcmd}
    for ((i=0;i<n;i++)); do
        printf '%s' "${fragcmd:$i:1}" >&9
        sleep 0.005
    done
    printf '\n' >&9
    for line in "$@"; do
        printf '%s\n' "$line" >&9
        sleep 0.05
    done
    timeout 0.3 cat <&9
    exec 9<&- 9>&-
}

rm -f node-A.log node-B.log node-C.log

log "starting 3-node full mesh: A=9000 B=9001 C=9002"
$BIN A 9000 9001 9002 >node-A.log 2>&1 & PIDS+=($!)
$BIN B 9001 9000 9002 >node-B.log 2>&1 & PIDS+=($!)
$BIN C 9002 9000 9001 >node-C.log 2>&1 & PIDS+=($!)
sleep 1

log "basic register/bid/status on A"
send_lines 127.0.0.1 9000 "REGISTER alice" "BID 500" "STATUS"

log "fragmented REGISTER (byte-by-byte) + BID on B, same connection"
send_fragmented_then 127.0.0.1 9001 "REGISTER bob" "BID 700"

sleep 1
log "convergence check: STATUS on all three nodes (expect 700 bob everywhere)"
echo "A: $(send_lines 127.0.0.1 9000 STATUS)"
echo "B: $(send_lines 127.0.0.1 9001 STATUS)"
echo "C: $(send_lines 127.0.0.1 9002 STATUS)"

log "tie-break: equal bids on A and C (both beat current 700)"
send_lines 127.0.0.1 9000 "REGISTER carol" "BID 900"
send_lines 127.0.0.1 9002 "REGISTER dave" "BID 900"
sleep 1
echo "A: $(send_lines 127.0.0.1 9000 STATUS)"
echo "B: $(send_lines 127.0.0.1 9001 STATUS)"
echo "C: $(send_lines 127.0.0.1 9002 STATUS)"
log "(all three nodes must report the SAME winner for the tie)"

log "peer failure: kill C, confirm A and B keep serving"
kill "${PIDS[2]}"
sleep 0.5
send_lines 127.0.0.1 9000 "REGISTER eve" "BID 1000"
sleep 0.5
echo "A after C died: $(send_lines 127.0.0.1 9000 STATUS)"
echo "B after C died: $(send_lines 127.0.0.1 9001 STATUS)"

log "peer recovery: restart C, expect it to converge via SYNC"
$BIN C 9002 9000 9001 >>node-C.log 2>&1 & PIDS[2]=$!
sleep 2
echo "C after restart: $(send_lines 127.0.0.1 9002 STATUS)"

log "close semantics on A only (not replicated)"
send_lines 127.0.0.1 9000 CLOSE
echo "A after close, new bid rejected: $(send_lines 127.0.0.1 9000 REGISTER x BID 5000)"
echo "B still open: $(send_lines 127.0.0.1 9001 STATUS)"

log "done -- logs in node-A.log node-B.log node-C.log"
