#!/bin/bash

workers=3

if [ "$#" -eq 1 ]; then
    workers=$1
fi

echo "docker compose -p spark up -d --scale spark-worker=$workers" && \
docker compose -p spark up -d --scale spark-worker=$workers && \
echo "docker exec spark-spark-1 start-history-server.sh" && \
docker exec spark-spark-1 start-history-server.sh
