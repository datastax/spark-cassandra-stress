package com.datastax.sparkstress

import java.util.UUID
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.sparkstress.RowTypes.PerfRowClass
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.datastax.sparkstress.SparkStressImplicits._
import org.joda.time.DateTime

abstract class ReadTask(config: Config, ss: SparkSession) extends StressTask {

  val sc = ss.sparkContext
  val uuidPivot = UUID.fromString("9b657ca1-bfb1-49c0-85f5-04b127adc6f3")
  val timePivot =  new DateTime(2000,1,1,0,0,0,0).plusSeconds(500)
  val keyspace = config.keyspace
  val table = config.table

  val numberNodes = CassandraConnector(sc.getConf).withClusterDo( _.getMetadata.getAllHosts.size)
  val tenthKeys:Int = config.numTotalKeys.toInt / 10

  val cores = sys.env.getOrElse("SPARK_WORKER_CORES", "1").toInt * numberNodes 
  val defaultParallelism = math.max(sc.defaultParallelism, cores) 
  val coresPerNode:Int = defaultParallelism / numberNodes
  def run()

  def runTrials(ss: SparkSession): Seq[TestResult] = {
    println("About to Start Trials")
    for (trial <- 1 to config.trials) yield {
      TestResult(time(run()),0L)
    }
  }
}

/**
  * Supertype used to encapsulate reading logic for all dataset read methods.
  */
abstract class DatasetReadTask(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  import org.apache.spark.sql.functions._ // needed to use col()
  def read_columns(columnNames: Seq[String]): Long = {
    val columns: Seq[org.apache.spark.sql.Column] = columnNames.map(col(_))
    config.saveMethod match {
      // filesystem read methods
      case "parquet" => ss.read.parquet(s"dsefs:///${keyspace}.${table}").select(columnNames.head, columnNames.tail:_*).count
      case "text" => ss.read.text(s"dsefs:///${keyspace}.${table}").select(columnNames.head, columnNames.tail:_*).count
      case "json" => ss.read.json(s"dsefs:///${keyspace}.${table}").select(columnNames.head, columnNames.tail:_*).count
      case "csv" => ss.read.csv(s"dsefs:///${keyspace}.${table}").select(columnNames.head, columnNames.tail:_*).count
      // regular read method from DSE/Cassandra
      case _ => ss
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> table, "keyspace" -> keyspace))
        .load()
        .select(columns:_*)
        .count()
    }
  }
}

/**
  * Full Table Scan One Column using DataSets
  * Performs a full table scan but only retrieves a single column from the underlying
  * table.
  */
class FTSOneColumn_DS(config: Config, ss: SparkSession) extends DatasetReadTask(config, ss) {
  override def run(): Unit = {
    val count = read_columns(Seq("color"))
    println(s"Loaded $count rows")
  }
}

/**
  * Full Table Scan Two Columns using DataSets
  * Performs a full table scan retrieving two columns from the underlying
  * table.
  */
class FTSTwoColumns_DS(config: Config, ss: SparkSession) extends DatasetReadTask(config, ss) {
  override def run(): Unit = {
    val count = read_columns(Seq("color", "size"))
    println(s"Loaded $count rows")
  }
}

/**
  * Full Table Scan Three Columns using DataSets
  * Performs a full table scan but only retrieves a single column from the underlying
  * table.
  */
class FTSThreeColumns_DS(config: Config, ss: SparkSession) extends DatasetReadTask(config, ss) {
  override def run(): Unit = {
    val count = read_columns(Seq("color", "size", "qty"))
    println(s"Loaded $count rows")
  }
}

/**
  * Full Table Scan Four Columns using DataSets
  * Performs a full table scan but only retrieves a single column from the underlying
  * table.
  */
class FTSFourColumns_DS(config: Config, ss: SparkSession) extends DatasetReadTask(config, ss) {
  override def run(): Unit = {
    val count = read_columns(Seq("color", "size", "qty", "order_number"))
    println(s"Loaded $count rows")
  }
}

/**
  * Full Table Scan Five Columns using DataSets
  * Performs a full table scan but only retrieves a single column from the underlying
  * table.
  */
class FTSFiveColumns_DS(config: Config, ss: SparkSession) extends DatasetReadTask(config, ss) {
  override def run(): Unit = {
    val count = read_columns(Seq("order_number", "qty", "color", "size", "order_time"))
    println(s"Loaded $count rows")
  }
}

/**
  * Full Table Scan All Columns using DataSets
  * Performs a full table scan but only retrieves a single column from the underlying
  * table.
  */
class FTSAllColumns_DS(config: Config, ss: SparkSession) extends DatasetReadTask(config, ss) {
  def run(): Unit = {
    val count = read_columns(Seq("order_number", "qty", "color", "size", "order_time", "store"))
    println(s"Loaded $count rows")
  }
}

/**
 * Push Down Count
 * Uses our internally cassandra count pushdown, this means all of the aggregation
 * is done on the C* side
 */
class PDCount(config: Config, ss: SparkSession) extends ReadTask(config, ss) {

  def run(): Unit = {
    val count = sc.cassandraTable(keyspace, table).cassandraCount()
    if (config.totalOps != count) {
      println(s"Read verification failed! Expected ${config.totalOps}, returned $count");
    }
    println(s"Loaded $count rows")
  }
}

/**
 * Full Table Scan One Column
 * Performs a full table scan but only retreives a single column from the underlying
 * table.
 */
class FTSOneColumn(config: Config, ss: SparkSession) extends ReadTask(config, ss) {

  def run(): Unit = {
    val count = sc.cassandraTable[String](keyspace, table).select("color").count
    println(s"Loaded $count rows")
  }
}

/**
 * Full Table Scan One Column
 * Performs a full table scan but only retreives a single column from the underlying
 * table.
 */
class FTSAllColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  def run(): Unit = {
    val count = sc.cassandraTable[PerfRowClass](keyspace, table).count
    println(s"Loaded $count rows")
  }
}

/**
 * Full Table Scan Five Columns
 * Performs a full table scan and only retreives 5 of the coulmns for each row
 */
class FTSFiveColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  def run(): Unit = {
    val count = sc.cassandraTable[(UUID, Int, String, String, org.joda.time.DateTime)](keyspace,
      table)
      .select("order_number", "qty", "color", "size", "order_time")
      .count
    println(s"Loaded $count rows")
  }
}

/**
 * Full Table Scan with a Clustering Column Predicate Pushed down to C*
 */
class FTSPDClusteringAllColumns(config: Config, ss: SparkSession) extends ReadTask(config,
  ss) {
  def run(): Unit = {
    val count = sc.cassandraTable[PerfRowClass](keyspace, table)
      .where("order_time < ?", timePivot)
      .count
    println(s"Loaded $count rows")
  }
}

/**
 * Full Table Scan with a Clustering Column Predicate Pushed down to C*
 * Only 5 columns retreived per row
 */
class FTSPDClusteringFiveColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  def run(): Unit = {
    val count = sc.cassandraTable[(UUID, Int, String, String, org.joda.time.DateTime)](keyspace,
      table)
      .where("order_time < ?", timePivot)
      .select("order_number", "qty", "color", "size", "order_time")
      .count
    println(s"Loaded $count rows")
  }
}

/**
 * Join With C* with 1M Partition Key requests
 */
class JWCAllColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  def run(): Unit = {
    val count = sc.parallelize(1 to tenthKeys)
      .map(num => Tuple1(s"Store $num"))
      .joinWithCassandraTable[PerfRowClass](keyspace, table)
      .count
    println(s"Loaded $count rows")
  }
}

/**
 * Join With C* with 1M Partition Key requests
 * A repartitionByCassandraReplica occurs before retreiving the data
 */
class JWCRPAllColumns(config: Config, ss: SparkSession) extends
ReadTask(config, ss) {
  def run(): Unit = {
    val count = sc.parallelize(1 to tenthKeys)
      .map(num => Tuple1(s"Store $num"))
      .repartitionByCassandraReplica(keyspace, table, coresPerNode)
      .joinWithCassandraTable[PerfRowClass](keyspace, table)
      .count
    println(s"Loaded $count rows")
  }
}

/**
 * Join With C* with 1M Partition Key requests
 * A clustering column predicate is pushed down to limit data retrevial
 */
class JWCPDClusteringAllColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  def run(): Unit = {
    val count = sc.parallelize(1 to tenthKeys)
      .map(num => Tuple1(s"Store $num"))
      .joinWithCassandraTable[PerfRowClass](keyspace, table)
      .where("order_time < ?", timePivot)
      .count
    println(s"Loaded $count rows")
  }
}

/**
 * A single C* partition is retreivied in an RDD
 */
class RetrieveSinglePartition(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  def run(): Unit = {
    val filterResults = sc.cassandraTable[String](keyspace, table)
      .where("store = ? ", "Store 5")
      .collect
    println(filterResults.length)
  }
}

