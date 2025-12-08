#!/bin/bash

# Exit immediately if a command fails
set -e

# Check input
if [ $# -ne 1 ]; then
  echo "Usage: $0 <num_replicas>"
  exit 1
fi

NUM_REPLICAS=$1
PROJECT_DIR="."
INSTALL_DIR="$PROJECT_DIR/build/install/library"

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
echo "Launching replicas..."
for ((i=0; i<NUM_REPLICAS; i++)); do
  REPLICA_DIR="$(cd "$PROJECT_DIR/replica$i" && pwd)"
  echo "Starting replica $i..."
  osascript <<EOF
tell application "Terminal"
    do script "cd '$REPLICA_DIR' && ./smartrun.sh bftsmart.benchmark.ThroughputLatencyServer $i 64"
end tell
EOF
done

REPLICA_DIR="$(cd "$PROJECT_DIR/replica0" && pwd)"
sleep 5s
osascript <<EOF
tell application "Terminal"
    do script "cd '$REPLICA_DIR' && ./smartrun.sh bftsmart.benchmark.ThroughputLatencyClient 0 1 1000 32 true true true"
end tell
EOF


echo "All replicas launched!"
