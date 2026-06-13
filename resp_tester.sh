#!/usr/bin/env bash
#
# Load-test data for the Rust Redis server.
# Sends ~60 SET commands with a mix of EX / PX / no-expiry over RESP.
#
# Usage:   ./load_test.sh [host] [port]
#          ./load_test.sh 127.0.0.1 6379   (defaults)
#
# One connection per command, matching the server's read-until-EOF model.
# If nc hangs instead of closing after sending, your nc build needs a flag:
#   GNU netcat:  add  -q1
#   OpenBSD nc:  add  -N
#   ncat:        add  -w1
# e.g.  nc -q1 "$HOST" "$PORT"

HOST="${1:-127.0.0.1}"
PORT="${2:-6379}"

# Build a RESP array from the given args and send it over one connection.
# Computes every $<len> prefix from the actual byte length, so it can't drift.
send() {
  local args=("$@")
  local out="*${#args[@]}\r\n"
  local a
  for a in "${args[@]}"; do
    out+="\$${#a}\r\n${a}\r\n"
  done
  printf '%b' "$out" | nc "$HOST" "$PORT"
  printf '\n'
}

keys=(user session token cart order invoice cache lock job metric \
      flag device feed page note tag rank score room slot)
vals=(alpha bravo charlie delta echo foxtrot golf hotel india juliet \
      kilo lima mike november oscar papa quebec romeo sierra tango)

i=0
for k in "${keys[@]}"; do
  for suffix in a b c d e f g; do
    i=$((i + 1))
    key="${k}:${suffix}${i}"
    val="${vals[$(( (i - 1) % ${#vals[@]} ))]}_${i}"
    case $(( i % 7 )) in
      0) send SET "$key" "$val" ;;            # no expiry        (persistent)
      1) send SET "$key" "$val" EX 5 ;;       # 5s   -> recycler should reap soon
      2) send SET "$key" "$val" EX 12 ;;      # 12s  -> reaped a bit later
      3) send SET "$key" "$val" PX 8000 ;;    # 8s in ms -> exercises the PX branch
      4) send SET "$key" "$val" EX 60 ;;      # 60s  -> sticks around
      5) send SET "$key" "$val" EX 30 ;;      # 30s 
      6) send SET "$key" "$val" EX 600 ;;     # 600s -> long-lived
    esac
  done
done

echo "Sent $i keys."