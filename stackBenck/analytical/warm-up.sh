#!/bin/bash

# ./explain_query.sh q1.sql > test_results/q1.txt
# ./explain_query.sh q2.sql > test_results/q2.txt
# ./explain_query.sh q3.sql > test_results/q3.txt
# ./explain_query.sh q4.sql > test_results/q4.txt

# comando="EXPLAIN (ANALYZE, BUFFERS) $(cat q1.sql)"

psql -d stack -c "EXPLAIN (ANALYZE, BUFFERS) $(cat q1/q1.sql)" > /dev/null
psql -d stack -c "EXPLAIN (ANALYZE, BUFFERS) $(cat q2/q2.sql)" > /dev/null
psql -d stack -c "EXPLAIN (ANALYZE, BUFFERS) $(cat q3/q3.sql)" > /dev/null
psql -d stack -c "EXPLAIN (ANALYZE, BUFFERS) $(cat q4/q4.sql)" > /dev/null