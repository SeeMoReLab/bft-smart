#!/bin/bash

set -e

# Usage check
if [ $# -ne 3 ]; then
  echo "Usage: $0 <num_shards> <replicas_per_shard> <use_failure_injection:true|false>"
  exit 1
fi

NUM_SHARDS=$1
REPLICAS_PER_SHARD=$2
USE_FAILURE_INJECTION=$3
PROJECT_DIR="."
INSTALL_DIR="$PROJECT_DIR/build/install/library"

if [ "$USE_FAILURE_INJECTION" != "true" ] && [ "$USE_FAILURE_INJECTION" != "false" ]; then
  echo "Error: <use_failure_injection> must be 'true' or 'false'"
  echo "Usage: $0 <num_shards> <replicas_per_shard> <use_failure_injection:true|false>"
  exit 1
fi

echo "[*] Building project..."
cd "$PROJECT_DIR"
./gradlew installDist

# Step 1: Create shard/replica folders and copy binaries
echo "[*] Preparing replica directories..."
for ((shard=0; shard<NUM_SHARDS; shard++)); do
    SHARD_DIR="$PROJECT_DIR/shard${shard}"
    rm -rf "$SHARD_DIR"
    for ((replica=0; replica<REPLICAS_PER_SHARD; replica++)); do
        REPLICA_DIR="$SHARD_DIR/replica${replica}"
        echo "Setting up $REPLICA_DIR..."
        rm -rf "$REPLICA_DIR"
        mkdir -p "$REPLICA_DIR"
        cp -r "$INSTALL_DIR/"* "$REPLICA_DIR/"
    done
done

echo "[*] Generating hosts files for shards..."
python3 generate_shard_hosts.py "$NUM_SHARDS" "$REPLICAS_PER_SHARD"

# Step 2: Launch replicas in separate Terminal windows
# Start time is 20 seconds ahead so that after the 10-second warmup wait,
# benchmark/failure injection begins 10 seconds later.
START_UNIX_MS=$(( ($(date +%s) * 1000) + 20000 ))
echo "[*] Using shared start unix ms: $START_UNIX_MS"

echo "[*] Launching replicas..."
for ((shard=0; shard<NUM_SHARDS; shard++)); do
    for ((replica=0; replica<REPLICAS_PER_SHARD; replica++)); do
        REPLICA_DIR="$(cd "$PROJECT_DIR/shard${shard}/replica${replica}" && pwd)"
        echo "Starting shard $shard replica $replica..."
        SERVER_CMD="./smartrun.sh bftsmart.demo.smallbank2pc.SmallBankServer2PC $shard $replica --config-dir config"
        if [ "$USE_FAILURE_INJECTION" = "true" ]; then
            SERVER_CMD="$SERVER_CMD --failure-spec config/failure_spec.xml --failure-start-unix-ms $START_UNIX_MS"
        fi
        osascript <<EOF
tell application "Terminal"
    do script "cd '$REPLICA_DIR' && $SERVER_CMD"
end tell
EOF
    done
done

# Step 3: Launch a client with synchronized benchmark start
echo "[*] Waiting 10 seconds for replicas to warm up..."
sleep 10
PROJECT_ABS_DIR="$(cd "$PROJECT_DIR" && pwd)"
osascript <<EOF
tell application "Terminal"
    do script "cd '$PROJECT_ABS_DIR/shard0/replica0' && ./smartrun.sh bftsmart.demo.smallbank2pc.SmallBankClient2PC --config-dir config -s $NUM_SHARDS --shard-config '$PROJECT_ABS_DIR' --create --execute --start-unix-ms $START_UNIX_MS"
end tell
EOF
