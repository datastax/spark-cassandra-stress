package com.datastax.sparkstress

import org.apache.spark.SparkContext
import com.datastax.spark.connector._

abstract class ReadTask extends StressTask{
  var config = Config()

  def run(sc: SparkContext)

  def setConfig(c:Config){
    config=c
  }

  def runTrials(sc: SparkContext): Seq[Long] = {
    println("About to Start Trials")
    for (trial <- 1 to config.trials) yield {time(run(sc))}
  }
}

class ReadAll extends ReadTask{

  val keyspace = config.keyspace
  val table = config.table
  var count: Long = 0
  def run(sc: SparkContext){
    //  do not use count optimization               VVVVVVVVV
    count = sc.cassandraTable(keyspace,table).filter(x=> true).count
    if (config.totalOps != count) {
      println(s"Read verification failed! Expected ${config.totalOps}, returned $count");
    }
  }
}


