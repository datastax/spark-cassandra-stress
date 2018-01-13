#!/usr/bin/env bash

PRE_JAR=""
POST_JAR=""

TARGET=$1
shift

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --conf) # SparkConf Options go first
        PRE_JAR+=" --conf $2"
        shift # past argument
        shift # past value
        ;;
    --master) # As do master control options
        PRE_JAR+=" --master $2"
        shift # past argument
        shift # past value
        ;;
    dse*|spark*|yarn*|mesos*|local*) #We supported master urls without flags
        PRE_JAR+=" --master $1"
        shift
        ;;
    *) # All other items are Jar args
        POST_JAR+=" $1"
        shift # past arg
        ;;
esac
done

JAR=build/libs/SparkCassandraStress-1.0.jar
CLASS=com.datastax.sparkstress.SparkCassandraStress

SUBMIT="spark-submit $PRE_JAR --class $CLASS $JAR $POST_JAR"

echo "Submit Script:: $SUBMIT"

if [[ $TARGET == "dse" ]]; then
  dse $SUBMIT
elif [[ $TARGET == "apache" ]]; then
  $SPARK_HOME/bin/$SUBMIT 
else
  echo "dse or apache required as the first argument"
fi
