package com.datastax.sparkstress

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.datastax.sparkstress.RowTypes._
import com.datastax.spark.connector._
import com.datastax.bdp.spark.writer.BulkTableWriter._

object WriteTask {
  val ValidTasks = Set(
    "writeshortrow",
    "writeperfrow",
    "writewiderow",
    "writerandomwiderow",
    "writewiderowbypartition"
  )
}

abstract class WriteTask[rowType](
  val config: Config,
  val sc: SparkContext)
  (implicit rwf: RowWriterFactory[rowType]) extends StressTask {

  def setupCQL() = {
    CassandraConnector(sc.getConf).withSessionDo{ session =>
      if (config.deleteKeyspace){
        println(s"Destroying Keyspace")
        session.execute(s"DROP KEYSPACE IF EXISTS ${config.keyspace}")
      }
      val kscql = getKeyspaceCql(config.keyspace)
      val tbcql = getTableCql(config.table)
      println(s"""Running the following create statements\n$kscql\n${tbcql.mkString("\n")})""")
      session.execute(kscql)
      session.execute(s"USE ${config.keyspace}")
      for (cql <- tbcql)
       session.execute(cql)
    }
    printf("Done Setting up CQL Keyspace/Table\n")
  }

  def getKeyspaceCql(ksName: String): String = s"CREATE KEYSPACE IF NOT EXISTS $ksName WITH replication = {'class': 'NetworkTopologyStrategy', 'Analytics': ${config.replicationFactor} }"

  def getTableCql(tbName: String): Seq[String]

  def getRDD: RDD[rowType]

  def run() = {
    config.saveMethod match {
      case "bulk" => getRDD.bulkSaveToCassandra(config.keyspace, config.table)
      case _ => getRDD.saveToCassandra(config.keyspace, config.table)
  	}
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
class WriteShortRow(config: Config, sc: SparkContext) extends
  WriteTask[ShortRowClass](config, sc)(implicitly[RowWriterFactory[ShortRowClass]]) {

  def getTableCql(tbName: String): Seq[String] =
    Seq(s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key bigint, col1 text, col2 text, col3 text, PRIMARY KEY(key))""".stripMargin)

  def getRDD: RDD[ShortRowClass] = {
    println(
      s"""Generating RDD for short rows:
         |${config.totalOps} Total Writes
         |${config.numPartitions} Num Partitions""".stripMargin
    )
    RowGenerator.getShortRowRDD(sc, config.numPartitions, config.totalOps)
  }

}

/**
 * Writes data in a format similar to the DataStax legacy 'tshirt' schema
 */
class WritePerfRow(config: Config, sc: SparkContext) extends
  WriteTask[PerfRowClass](config, sc)(implicitly[RowWriterFactory[PerfRowClass]]) {

  def getTableCql(tbName: String): Seq[String] =
    Seq(
      s"""CREATE TABLE IF NOT EXISTS $tbName
       |(store text,
       |order_time timestamp,
       |order_number uuid,
       |color text,
       |size text,
       |qty int,
       |PRIMARY KEY (store, order_time, order_number))
     """.stripMargin,
    s"""CREATE INDEX color ON ${config.keyspace}.$tbName (color) """,
    s"""CREATE INDEX size ON ${config.keyspace}.$tbName (size)"""
    )

  def getRDD: RDD[PerfRowClass] =
    RowGenerator.getPerfRowRdd(sc, config.numPartitions, config.totalOps, config.numTotalKeys)

}

/**
 * Runs inserts to partitions in a round robin fashion. This means partition key 1 will not
 * be written to twice until partition key n is written to once.
 */
class WriteWideRow(config: Config, sc: SparkContext) extends
  WriteTask[WideRowClass](config, sc)(implicitly[RowWriterFactory[WideRowClass]]) {

  def getTableCql(tbName: String): Seq[String] =
    Seq(s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key bigint, col1 text, col2 text, col3 text,
       |PRIMARY KEY (key, col1))
     """.stripMargin)

  def getRDD: RDD[WideRowClass] = {
    println(
      s"""Generating RDD for wide rows with round robin partitions:
         |${config.totalOps} Total Writes,
         |${config.numTotalKeys} Cassandra Partitions""".stripMargin
    )
    RowGenerator
      .getWideRowRdd(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
  }

}

/**
 * Runs inserts to partitions in a random fashion. The chance that any particular partition
 * key will be written to at a time is 1/N. (flat distribution)
 */
class WriteRandomWideRow(config: Config, sc: SparkContext) extends
  WriteTask[WideRowClass](config, sc)(implicitly[RowWriterFactory[WideRowClass]]) {

  def getTableCql(tbName: String): Seq[String] =
    Seq(s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key bigint, col1 text, col2 text, col3 text,
       |PRIMARY KEY (key, col1))
     """.stripMargin)

  def getRDD: RDD[WideRowClass] = {
    println(
      s"""Generating RDD for random wide rows:
         |${config.totalOps} Total Writes,
         |${config.numTotalKeys} Cassandra Partitions""".stripMargin
    )
    RowGenerator.getRandomWideRow(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
  }

}

/**
 *  Runs inserts to partitions in an ordered fashion. All writes to partition 1 occur before any
 *  writes to partition 2.
 */
class WriteWideRowByPartition(config: Config, sc: SparkContext) extends
  WriteTask[WideRowClass](config, sc)(implicitly[RowWriterFactory[WideRowClass]]) {

  def getTableCql(tbName: String): Seq[String] =
    Seq(s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key int, col1 text, col2 text, col3 text,
       |PRIMARY KEY (key, col1))
     """.stripMargin)

  def getRDD: RDD[WideRowClass] = {
    println(
      s"""Generating RDD for wide rows in ordered by partition:
         |${config.totalOps} Total Writes,
         |${config.numTotalKeys} Cassandra Partitions""".stripMargin
    )
    RowGenerator.getWideRowByPartition(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
  }



}
