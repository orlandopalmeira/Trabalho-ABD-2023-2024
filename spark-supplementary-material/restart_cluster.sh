#!/bin/bash

workers=3

if [ "$#" -eq 1 ]; then
    workers=$1
fi

docker compose -p spark down
./raise_cluster.sh $workers