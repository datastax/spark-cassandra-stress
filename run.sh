#!/usr/bin/env bash

startConnIdx=0
for i in "$@"; do
  startConnIdx=$((startConnIdx+1))
  if [ $i == "--conf" ]; then break; fi
done

if [ $startConnIdx -eq $# ]; then 
  connectorArgs="" 
  stressArgs=${*:2}
else 
  connectorArgs=${*:$((startConnIdx))} 
  stressArgs=${*:2:$((startConnIdx-2))}
fi

JAR=build/libs/SparkCassandraStress-1.0.jar
CLASS=com.datastax.sparkstress.SparkCassandraStress

SUBMIT="spark-submit $connectorArgs --class $CLASS $JAR $stressArgs"

if [[ $1 == "dse" ]]; then
  dse $SUBMIT 
elif [[ $1 == "apache" ]]; then
  $SPARK_HOME/bin/$SUBMIT 
else
  echo "dse or apache required as the first argument"
fi
echo
