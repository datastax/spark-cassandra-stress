package com.datastax.sparkstress

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import com.datastax.spark.connector._

object WriteTask {
  val ValidTasks = Set(
    "writeshortrow",
    "writeperfrow",
    "writewiderow",
    "writerandomwiderow"
  )
}

abstract class WriteTask( var config: Config, val sc: SparkContext) extends StressTask {

  def setupCQL() = {
    CassandraConnector(sc.getConf).withSessionDo{ session =>
      if (config.deleteKeyspace){
      println("Destroying Keyspace")
      session.execute(s"DROP KEYSPACE IF EXISTS ${config.keyspace}")
      }
      val kscql = getKeyspaceCql(config.keyspace)
      val tbcql = getTableCql(config.table)
      println(s"Running the following create statements\n$kscql\n$tbcql")
      session.execute(kscql)
      session.execute(tbcql)
    }
    printf("Done Setting up CQL Keyspace/Table\n")
  }

  def getKeyspaceCql(ksName: String): String = s"CREATE KEYSPACE IF NOT EXISTS $ksName WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }"

  def getTableCql(tbName: String): String

  def run(sc: SparkContext) : Unit = {
    setupCQL()
    getRDD().saveToCassandra(config.keyspace, config.table)
  }

  def setConfig(c: Config): Unit  = {
    config = c
  }

  def runTrials(sc: SparkContext): Seq[Long] = {
    println("About to Start Trials")
    for (trial <- 1 to config.trials) yield {setupCQL(); time(run(sc))}
  }

  def getRDD(): RDD[_]

}


class WriteShortRow(config: Config, sc: SparkContext) extends WriteTask(config, sc) {

  def getTableCql(tbName: String): String =
    s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key bigint, col1 text, col2 text, col3 text, PRIMARY KEY(key))""".stripMargin

  def getRDD(): RDD[_] = RowGenerator.getShortRowRDD(sc, config.numPartitions, config.totalOps)
}

class WritePerfRow(config: Config, sc: SparkContext) extends WriteTask(config, sc) {

  def getTableCql(tbName: String): String =
    s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key text, size text, qty int, time timestamp, color text,
       |col1 text, col2 text, col3 text, col4 text, col5 text,
       |col6 text, col7 text, col8 text, col9 text, col10text,
       |PRIMARY KEY (key))}
     """.stripMargin

  def getRDD(): RDD[_] =
    RowGenerator
      .getPerfRowRdd(sc, config.numPartitions, config.totalOps)

}

class WriteWideRow(config: Config, sc: SparkContext) extends WriteTask( config, sc){

  def getTableCql(tbName: String): String =
    s"""CREATE TABLE IF NOT EXISTS $tbName
      |(key int, col1 text, col2 text, col3 text,
      |PRIMARY KEY (key, col1))
    """.stripMargin

  def getRDD(): RDD[_] =
    RowGenerator
      .getWideRowRdd(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
}

class WriteRandomWideRow(config: Config, sc: SparkContext) extends WriteTask(config, sc){

  def getTableCql(tbName: String): String =
    s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key int, col1 text, col2 text, col3 text,
       |PRIMARY KEY (key, col1))
     """.stripMargin

  def getRDD(): RDD[_] =
    RowGenerator.getRandomWideRow(sc, config.numPartitions, config.totalOps, config.numTotalKeys)

}