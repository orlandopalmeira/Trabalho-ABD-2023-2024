#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 file_name"
    exit 1
fi

file_name=$1
# comando="EXPLAIN ANALYZE $(cat $file_name)"
comando="EXPLAIN (ANALYZE, BUFFERS) $(cat $file_name)"
# comando="EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) $(cat $file_name)"

psql -d stack -c "$comando"
