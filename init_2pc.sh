#!/bin/bash

set -e

# Usage check
if [ $# -ne 2 ]; then
  echo "Usage: $0 <num_shards> <replicas_per_shard>"
  exit 1
fi

NUM_SHARDS=$1
REPLICAS_PER_SHARD=$2
PROJECT_DIR="."
INSTALL_DIR="$PROJECT_DIR/build/install/library"

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
echo "[*] Launching replicas..."
for ((shard=0; shard<NUM_SHARDS; shard++)); do
    for ((replica=0; replica<REPLICAS_PER_SHARD; replica++)); do
        REPLICA_DIR="$(cd "$PROJECT_DIR/shard${shard}/replica${replica}" && pwd)"
        echo "Starting shard $shard replica $replica..."
        osascript <<EOF
tell application "Terminal"
    do script "cd '$REPLICA_DIR' && ./smartrun.sh bftsmart.demo.smallbank2pc.SmallBankServer2PC $shard $replica"
end tell
EOF
    done
done

# Step 3: Launch a client after a short delay
sleep 10s
PROJECT_ABS_DIR="$(cd "$PROJECT_DIR" && pwd)"
osascript <<EOF
tell application "Terminal"
    do script "cd '$PROJECT_ABS_DIR/shard0/replica0' && ./smartrun.sh bftsmart.demo.smallbank2pc.SmallBankClient2PC -c config/smallbank_config.xml -s $NUM_SHARDS --shard-config '$PROJECT_ABS_DIR' --create --execute"
end tell
EOF
