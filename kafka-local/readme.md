#!/bin/bash

# Ensure topic exists
docker exec -e KAFKA_OPTS="" kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29091 \
  --topic orders \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 2>/dev/null || echo "Topic already exists"

echo "=== Experiment 1: acks=0 (fire and forget) ==="
echo "Watch Grafana: remotetimems should be 0, totaltimems lowest"
sleep 3

docker exec -e KAFKA_OPTS="" kafka-1 kafka-producer-perf-test \
  --topic orders \
  --num-records 10000 \
  --record-size 1024 \
  --throughput 1000 \
  --producer-props bootstrap.servers=kafka-1:29091 acks=0

echo ""
echo "Waiting 15 seconds for metrics to stabilize..."
sleep 15

echo "=== Experiment 2: acks=1 (leader only) ==="
echo "Watch Grafana: remotetimems still 0, totaltimems ~5-10ms"
sleep 3

docker exec -e KAFKA_OPTS="" kafka-1 kafka-producer-perf-test \
  --topic orders \
  --num-records 10000 \
  --record-size 1024 \
  --throughput 1000 \
  --producer-props bootstrap.servers=kafka-1:29091 acks=1

echo ""
echo "Waiting 15 seconds for metrics to stabilize..."
sleep 15

echo "=== Experiment 3: acks=all (wait for ISR) ==="
echo "Watch Grafana: remotetimems will appear! Shows follower wait time"
sleep 3

docker exec -e KAFKA_OPTS="" kafka-1 kafka-producer-perf-test \
  --topic orders \
  --num-records 10000 \
  --record-size 1024 \
  --throughput 1000 \
  --producer-props bootstrap.servers=kafka-1:29091 acks=all



## The Internal Flow for Each Message
```
You send message
      ↓
Producer batches it (waits up to linger.ms or batch fills)
      ↓
Network: producer → leader broker (~5ms)
      ↓
Leader writes to local log (~10-50ms depending on disk)
      ↓
Leader sends ack back to producer (~5ms)
      ↓
Producer receives ack ✓
      └─ Total: ~20-100ms for acks=1 (followers replicate async in background)
```

**With acks=1, followers are NOT in the critical path** — the producer gets an ack as soon as the leader writes to disk. The 124ms median includes network + leader disk write + ack response.
