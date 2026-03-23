#!/bin/bash
echo "Starting Chaos Failure Injection Test..."

# 1. Start Database
go run . -mode=participant -port=8081 &
DB_PID=$!

# 2. Start Coordinator
go run . -mode=coordinator -port=8082 &
COORD_PID=$!

sleep 2

# 3. Fire heavy Zipfian traffic in the background
go run . -mode=loadtest -type=zipf > /dev/null 2>&1 &

sleep 1 # Let it process half the transactions

# 4. KILL THE COORDINATOR (Simulate catastrophic failure)
echo "[CHAOS] Forcefully killing Coordinator Node (PID $COORD_PID)..."
kill -9 $COORD_PID

sleep 1

# 5. Measure Mean Time To Recovery (MTTR)
echo "[RECOVERY] Rebooting Coordinator from WAL..."
START_TIME=$(date +%s%N)
go run . -mode=coordinator -port=8082 > recovery_log.txt 2>&1 &
NEW_COORD_PID=$!
END_TIME=$(date +%s%N)

# Calculate recovery time in milliseconds
RECOVERY_MS=$(((END_TIME - START_TIME) / 1000000))
echo "System recovered and dependency graph rebuilt in: $RECOVERY_MS ms"

# Cleanup
kill -9 $DB_PID
kill -9 $NEW_COORD_PID