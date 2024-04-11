#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 file_name"
    exit 1
fi

file_name=$1
comando="EXPLAIN ANALYZE $(cat $file_name)"
# Run the query with EXPLAIN ANALYZE
psql -d stack -c "$comando"
