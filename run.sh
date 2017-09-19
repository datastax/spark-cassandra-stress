#!/usr/bin/env bash

startConnIdx=0
connectorArgsFound=0
for i in "$@"; do
  startConnIdx=$((startConnIdx+1))
  if [ $i == "--conf" ]; then connectorArgsFound=1; break; fi
  lastParam=$i
done

if [ $connectorArgsFound -eq 0 ]; then 
  connectorArgs="" 
else 
  connectorArgs=${*:$((startConnIdx))} 
fi

echo "$lastParam" | grep -E "^(yarn|spark|mesos|local)" >& /dev/null
noMasterProvided=$? 
if [ $noMasterProvided -eq 1 ]; then
  sparkmaster=""
  if [ $connectorArgsFound -eq 0 ]; then
    stressArgs=${*:2:$((startConnIdx-1))}
  else
    stressArgs=${*:2:$((startConnIdx-2))}
  fi
else
  sparkmaster="--master $lastParam" 
  if [ $connectorArgsFound -eq 0 ]; then
    stressArgs=${*:2:$((startConnIdx-2))}
  else
    stressArgs=${*:2:$((startConnIdx-3))}
  fi
fi

JAR=build/libs/SparkCassandraStress-1.1.jar
CLASS=com.datastax.sparkstress.SparkCassandraStress

SUBMIT="spark-submit $sparkmaster $connectorArgs --class $CLASS $JAR $stressArgs"

if [[ $1 == "dse" ]]; then
  dse $SUBMIT 
elif [[ $1 == "apache" ]]; then
  $SPARK_HOME/bin/$SUBMIT 
else
  echo "dse or apache required as the first argument"
fi
