#!/bin/bash

# Runs the SQL file passed as an argument multiple times and prints the execution time

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <filesql> [times]"
    exit 1
fi

filesql=$1
times=${2:-3}  # Default value is 3

if [ ! -f "$filesql" ]; then
    echo "File not found: $filesql"
    exit 1
fi

comando="EXPLAIN (ANALYZE, BUFFERS) $(cat $filesql)"

total=0

for ((i=1; i<=$times; i++)); do
    echo "Run $i:"
    res=$(psql -d stack -c "$comando" | grep "Execution Time")
    echo "$res"
    # add the time to the total
    time=$(echo $res | awk '{print $3}' | sed 's/ms$//')
    total=$(echo "$total + $time" | bc)

done

# calculate the average time
average=$(echo "scale=2; $total / $times" | bc)
echo "Average time: $average ms"
