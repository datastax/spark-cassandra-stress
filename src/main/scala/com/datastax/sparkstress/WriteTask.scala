package com.datastax.sparkstress

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.datastax.sparkstress.RowTypes._
import com.datastax.spark.connector._

object WriteTask {
  val ValidTasks = Set(
    "writeshortrow",
    "writeperfrow",
    "writewiderow",
    "writerandomwiderow",
    "writewiderowbypartition"
  )
}

abstract class WriteTask( var config: Config, val sc: SparkContext) extends StressTask {

  def setupCQL() = {
    CassandraConnector(sc.getConf).withSessionDo{ session =>
      if (config.deleteKeyspace){
      println(s"Destroying Keyspace")
      session.execute(s"DROP KEYSPACE IF EXISTS ${config.keyspace}")
      }
      val kscql = getKeyspaceCql(config.keyspace)
      val tbcql = getTableCql(config.table)
      println(s"Running the following create statements\n$kscql\n$tbcql")
      session.execute(kscql)
      session.execute(s"USE ${config.keyspace}")
      session.execute(tbcql)
    }
    printf("Done Setting up CQL Keyspace/Table\n")
  }

  def getKeyspaceCql(ksName: String): String = s"CREATE KEYSPACE IF NOT EXISTS $ksName WITH replication = {'class': 'NetworkTopologyStrategy', 'Analytics': ${config.replicationFactor} }"

  def getTableCql(tbName: String): String

  def run() : Unit

  def setConfig(c: Config): Unit  = {
    config = c
  }

  def runTrials(sc: SparkContext): Seq[Long] = {
    println("About to Start Trials")
    for (trial <- 1 to config.trials) yield {setupCQL(); Thread.sleep(10000); time(run())}
  }


}

/**
 * Writes data to a schema which contains no clustering keys, no partitions will be
 * overwritten.
 */
class WriteShortRow(config: Config, sc: SparkContext) extends WriteTask(config, sc) {

  def getTableCql(tbName: String): String =
    s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key bigint, col1 text, col2 text, col3 text, PRIMARY KEY(key))""".stripMargin

  def getRDD: RDD[ShortRowClass] = {
    println(
      s"""Generating RDD for short rows:
         |${config.totalOps} Total Writes
         |${config.numPartitions} Num Partitions""".stripMargin
    )
    RowGenerator.getShortRowRDD(sc, config.numPartitions, config.totalOps)
  }

  def run(): Unit = {
    getRDD.saveToCassandra(config.keyspace, config.table)
  }
}

/**
 * Writes data in a format similar to the DataStax legacy 'tshirt' schema
 */
class WritePerfRow(config: Config, sc: SparkContext) extends WriteTask(config, sc) {

  def getTableCql(tbName: String): String =
    s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key text, size text, qty int, time timestamp, color text,
       |col1 text, col2 text, col3 text, col4 text, col5 text,
       |col6 text, col7 text, col8 text, col9 text, col10 text,
       |PRIMARY KEY (key))}
     """.stripMargin

  def getRDD: RDD[PerfRowClass] =
    RowGenerator.getPerfRowRdd(sc, config.numPartitions, config.totalOps)

  def run(): Unit = {
    getRDD.saveToCassandra(config.keyspace, config.table)
  }

}

/**
 * Runs inserts to partitions in a round robin fashion. This means partition key 1 will not
 * be written to twice until partition key n is written to once.
 */
class WriteWideRow(config: Config, sc: SparkContext) extends WriteTask( config, sc){
  def getTableCql(tbName: String): String =
    s"""CREATE TABLE IF NOT EXISTS $tbName
      |(key int, col1 text, col2 text, col3 text, col4 text, col5 text, col6 text, col7 text, col8 text, col9 text,
      |PRIMARY KEY ((key, col1), col2, col3, col4, col5, col6, col7, col8, col9))
    """.stripMargin

  def getRDD: RDD[WideRowClass] = {
    println(
      s"""Generating RDD for wide rows with round robin partitions:
         |${config.totalOps} Total Writes,
         |${config.numTotalKeys} Cassandra Partitions""".stripMargin
    )
    RowGenerator
      .getWideRowRdd(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
  }

  def run(): Unit = {
    getRDD.saveToCassandra(config.keyspace, config.table)
  }
}

/**
 * Runs inserts to partitions in a random fashion. The chance that any particular partition
 * key will be written to at a time is 1/N. (flat distribution)
 */
class WriteRandomWideRow(config: Config, sc: SparkContext) extends WriteTask(config, sc){

  def getTableCql(tbName: String): String =
    s"""CREATE TABLE IF NOT EXISTS $tbName
      |(key int, col1 text, col2 text, col3 text, col4 text, col5 text, col6 text, col7 text, col8 text, col9 text,
      |PRIMARY KEY ((key, col1), col2, col3, col4, col5, col6, col7, col8, col9))
     """.stripMargin

  def getRDD[T]: RDD[WideRowClass] = {
    println(
      s"""Generating RDD for random wide rows:
         |${config.totalOps} Total Writes,
         |${config.numTotalKeys} Cassandra Partitions""".stripMargin
    )
    RowGenerator.getRandomWideRow(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
  }

  def run(): Unit = {
    getRDD.saveToCassandra(config.keyspace, config.table)
  }
}

/**
 *  Runs inserts to partitions in an ordered fashion. All writes to partition 1 occur before any
 *  writes to partition 2.
 */
class WriteWideRowByPartition(config: Config, sc: SparkContext) extends WriteTask(config, sc){

  def getTableCql(tbName: String): String =
    s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key int, col1 text, col2 text, col3 text,
       |PRIMARY KEY (key, col1))
     """.stripMargin

  def getRDD[T]: RDD[WideRowClass] = {
    println(
      s"""Generating RDD for wide rows in ordered by partition:
         |${config.totalOps} Total Writes,
         |${config.numTotalKeys} Cassandra Partitions""".stripMargin
    )
    RowGenerator.getWideRowByPartition(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
  }

  def run(): Unit = {
    getRDD.saveToCassandra(config.keyspace, config.table)
  }
}
