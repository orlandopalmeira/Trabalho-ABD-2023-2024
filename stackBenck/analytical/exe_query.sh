#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 file_name"
    exit 1
fi

file_name=$1
psql -d stack -f $file_name
