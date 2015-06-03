#!/usr/bin/env bash

RESULTS=results.tsv
DIR=`dirname $0`
CQL_ROW_COUNT=$((256*1024));
#RDD partititions. Should be equals to number of workers or cluster nodes?
RDD_PARTITIONS=4

# number of repeated runs
RUNS=4

for TEST in writerandomwiderow writeshortrow writeperfrow writewiderow writewiderowbypartition; do
  for METHOD in driver bulk; do
    for COLUMNS_PER_ROW in 1 10 100 1024 10240; do
      ops=$CQL_ROW_COUNT
      rows=$((ops/COLUMNS_PER_ROW))
      echo $DIR/run.sh dse -p $RDD_PARTITIONS -y $rows -o $ops -d -n $RUNS -S $RESULTS -m $METHOD $TEST;
      while $DIR/run.sh dse -p $RDD_PARTITIONS -y $rows -o $ops -d -n $RUNS -S $RESULTS -m $METHOD $TEST; do
        ops=$((ops*2));
        rows=$((ops/COLUMNS_PER_ROW))
        echo $DIR/run.sh dse -p $RDD_PARTITIONS -y $rows -o $ops -d -n $RUNS -S $RESULTS -m $METHOD $TEST;
      done
    done
  done
done