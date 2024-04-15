#!/bin/bash

echo "docker compose -p spark up -d --scale spark-worker=3" && \
docker compose -p spark up -d --scale spark-worker=3 && \
echo "docker exec spark-spark-1 start-history-server.sh" && \
docker exec spark-spark-1 start-history-server.sh
