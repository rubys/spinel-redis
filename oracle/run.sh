#!/bin/sh
# Oracle harness: replay every snapshot-gated flow through the REAL
# redis-rb gem and diff against the committed spinel snapshots. The
# snapshots were frozen from the compiled client; this proves the real
# gem derives the identical output from the identical flows — snapshot
# verification with zero hand-authored expectations.
#
# Usage: sh oracle/run.sh          (from the repo root)
# Needs: ruby with the redis gem, redis-server, redis-cli on PATH.

ORACLE_PORT=16420
OUTDIR=build/oracle
mkdir -p "$OUTDIR"

redis-server --port $ORACLE_PORT --save '' --appendonly no --daemonize yes \
  --pidfile /tmp/spinel-redis-oracle.pid --logfile /tmp/spinel-redis-oracle.log

tries=0
until redis-cli -p $ORACLE_PORT ping >/dev/null 2>&1; do
  tries=$((tries + 1))
  if [ $tries -gt 50 ]; then
    echo "oracle: redis-server did not come up on $ORACLE_PORT" >&2
    exit 2
  fi
  sleep 0.1
done

fails=0
ran=0
for flow in example_basic example_incr_decr example_list example_sets \
            example_pubsub example_prefork live_smoke pubsub_live; do
  redis-cli -p $ORACLE_PORT flushall >/dev/null
  ruby "oracle/$flow.rb" $ORACLE_PORT > "$OUTDIR/$flow.out" 2>&1
  ran=$((ran + 1))
  if diff -u "test/${flow}_test.rb.expected" "$OUTDIR/$flow.out" > "$OUTDIR/$flow.diff" 2>&1; then
    echo "ok   $flow"
    rm -f "$OUTDIR/$flow.diff"
  else
    echo "FAIL $flow (see $OUTDIR/$flow.diff)"
    fails=$((fails + 1))
  fi
done

redis-cli -p $ORACLE_PORT shutdown nosave 2>/dev/null
echo "$((ran - fails))/$ran flows match redis-rb"
[ $fails -eq 0 ]
