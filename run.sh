#!/usr/bin/env bash

JAR=build/libs/SparkCassandraStress-1.0.jar
CLASS=com.datastax.sparkstress.SparkCassandraStress

SUBMIT="spark-submit --class $CLASS $JAR"

if [[ $1 == "dse" ]]; then
  dse $SUBMIT ${*:2}
elif [[ $1 == "apache" ]]; then
  $SPARK_HOME/bin/$SUBMIT ${*:2}
else
  echo "dse or apache required as the first argument"
fi
