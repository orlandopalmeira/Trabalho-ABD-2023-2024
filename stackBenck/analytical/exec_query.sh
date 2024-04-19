#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 file_name [-e flag(explain flag)]"
    exit 1
fi

file_name=$1

comando="$(cat $file_name)"
# Check if flag -e (explain) is provided, so it will do the EXPLAIN (ANALYZE, BUFFERS) command
for arg in "$@"; do
    if [[ $arg == "-e"* ]]; then
        comando="EXPLAIN (ANALYZE, BUFFERS) $comando"
    fi
done

psql -d stack -c "$comando"
