#!/bin/bash

echo "docker exec spark-spark-1 python3 gen_files.py $1"
docker exec spark-spark-1 python3 gen_files.py $1
