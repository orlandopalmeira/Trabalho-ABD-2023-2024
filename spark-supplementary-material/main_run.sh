#!/bin/bash

# Pode-se especificar workload (w1, w2, w3, w4)
echo "docker exec spark-spark-1 python3 main.py $1"
docker exec spark-spark-1 python3 main.py $1


# echo "docker exec spark-spark-1 python3 main.py"
# docker exec spark-spark-1 python3 main.py