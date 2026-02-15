#!/bin/bash

# Exit immediately if a command fails
set -e

# Check input
if [ $# -ne 2 ]; then
  echo "Usage: $0 <num_replicas> <use_failure_injection:true|false>"
  exit 1
fi

NUM_REPLICAS=$1
USE_FAILURE_INJECTION=$2
PROJECT_DIR="."
INSTALL_DIR="$PROJECT_DIR/build/install/library"

if [ "$USE_FAILURE_INJECTION" != "true" ] && [ "$USE_FAILURE_INJECTION" != "false" ]; then
  echo "Error: <use_failure_injection> must be 'true' or 'false'"
  echo "Usage: $0 <num_replicas> <use_failure_injection:true|false>"
  exit 1
fi

# Step 1: Build the project
echo "Building project with Gradle..."
cd "$PROJECT_DIR"
./gradlew installDist

# Step 2: Create replica folders
echo "Preparing replica directories..."
for ((i=0; i<NUM_REPLICAS; i++)); do
  REPLICA_DIR="$PROJECT_DIR/replica$i"
  echo "Setting up $REPLICA_DIR..."
  rm -rf "$REPLICA_DIR"
  mkdir -p "$REPLICA_DIR"
  cp -r "$INSTALL_DIR/"* "$REPLICA_DIR/"
done

# Step 3: Launch replicas in separate Terminal windows
# Start time is 20 seconds ahead so that after the 10-second warmup wait,
# benchmark/failure injection begins 10 seconds later.
START_UNIX_MS=$(( ($(date +%s) * 1000) + 20000 ))
echo "Using shared start unix ms: $START_UNIX_MS"

echo "Launching replicas..."
for ((i=0; i<NUM_REPLICAS; i++)); do
  REPLICA_DIR="$(cd "$PROJECT_DIR/replica$i" && pwd)"
  echo "Starting replica $i..."
  SERVER_CMD="./smartrun.sh bftsmart.demo.smallbank.SmallBankServer $i --config-dir config"
  if [ "$USE_FAILURE_INJECTION" = "true" ]; then
    SERVER_CMD="$SERVER_CMD --failure-spec config/failure_spec.xml --failure-start-unix-ms $START_UNIX_MS"
  fi
  osascript <<EOF
tell application "Terminal"
    do script "cd '$REPLICA_DIR' && $SERVER_CMD"
end tell
EOF
done

mkdir -p "$PROJECT_DIR/output"

REPLICA_DIR="$(cd "$PROJECT_DIR/replica0" && pwd)"
echo "Waiting 10 seconds for replicas to warm up..."
sleep 10
osascript <<EOF
tell application "Terminal"
    do script "cd '$REPLICA_DIR' && ./smartrun.sh bftsmart.demo.smallbank.SmallBankClient --config-dir config --create --execute --start-unix-ms $START_UNIX_MS"
end tell
EOF


echo "All replicas launched!"
