#!/usr/bin/env bash

RESULTS=results.tsv
DIR=`dirname $0`
START_ROW_COUNT=$((1024*1024));
MAX_ROW_COUNT=$((40*1024*1024));
#RDD partititions. Should be equals to number of workers or cluster nodes?
W_PARTITION_SIZE=$((1024*1024));

# number of repeated runs
RUNS=4

for COLUMNS_PER_ROW in 1 10 100 1024 10240; do
   ops=$START_ROW_COUNT
   while  [[ MAX_ROW_COUNT -gt ops ]]; do
    RDD_PARTITIONS=$((ops/W_PARTITION_SIZE))
    rows=$((ops/COLUMNS_PER_ROW))
    $DIR/run.sh dse -p $RDD_PARTITIONS -y $rows -o $ops -d -n 1 writerandomwiderow;
    sleep 10
    echo $DIR/run.sh dse -p $RDD_PARTITIONS -y $rows -o $ops -n $RUNS -S $RESULTS readAll
    $DIR/run.sh dse -p $RDD_PARTITIONS -y $rows -o $ops -n $RUNS -S $RESULTS readAll
    ops=$((ops*2));
  done
done
