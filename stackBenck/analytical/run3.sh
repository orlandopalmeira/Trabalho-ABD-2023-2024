#!/bin/bash

# Corre três vezes o ficheiro sql passado como argumento e imprime o tempo de execução

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <filesql>"
    exit 1
fi

filesql=$1
comando="EXPLAIN (ANALYZE, BUFFERS) $(cat $filesql)"

for i in {1..3}; do
    psql -d stack -c "$comando" | grep "Execution Time"
done
