package com.datastax.sparkstress

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
/**
 * Created by russellspitzer on 6/6/14.
 */
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

  def run(sc: SparkContext){
    sc.cassandraTable(keyspace,table).count
  }
}


