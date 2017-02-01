package com.datastax.sparkstress

import java.util.UUID

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.sparkstress.RowTypes.PerfRowClass
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector._
import com.datastax.sparkstress.SparkStressImplicits._
import org.joda.time.DateTime

object ReadTask {
  val ValidTasks = Set(
    "ftsallcolumns",
    "ftsfivecolumns",
    "ftsonecolumn",
    "ftspdclusteringallcolumns",
    "ftspdclusteringfivecolumns",
    "jwcallcolumns",
    "jwcpdclusteringallcolumns",
    "jwcrpallcolumns",
    "pdcount",
    "retrievesinglepartition",
    // SparkSQL
    "sqlftsallcolumns",
    "sqlftsfivecolumns",
    "sqlftsonecolumn",
    "sqlftsclusteringallcolumns",
    "sqlftsclusteringfivecolumns",
    "sqljoinallcolumns",
    "sqljoinclusteringallcolumns",
    "sqlcount",
    "sqlretrievesinglepartition",
    "sqlrunuserquery",
    "sqlsliceprimarykey",
    "sqlslicenonprimarykey"
  )
}

abstract class ReadTask(config: Config, sc: SparkContext) extends StressTask {

  val uuidPivot = UUID.fromString("9b657ca1-bfb1-49c0-85f5-04b127adc6f3")
  val timePivot =  new DateTime(2000,1,1,0,0,0,0).plusSeconds(500)
  val keyspace = config.keyspace
  val table = config.table

  val numberNodes = CassandraConnector(sc.getConf).withClusterDo( _.getMetadata.getAllHosts.size)
  val tenthKeys:Int = config.numTotalKeys.toInt / 10
  val userSqlQuery:String = config.userSqlQuery

  // deprecated approach, but compatible across all versions we care about right now
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val cores = sys.env.getOrElse("SPARK_WORKER_CORES", "1").toInt * numberNodes 
  val defaultParallelism = math.max(sc.defaultParallelism, cores) 
  val coresPerNode:Int = defaultParallelism / numberNodes
  def run()

  def runTrials(sc: SparkContext): Seq[TestResult] = {
    println("About to Start Trials")
    for (trial <- 1 to config.trials) yield {
      TestResult(time(run()),0L)
    }
  }
}

/**
 * Push Down Count
 * Uses our internally cassandra count pushdown, this means all of the aggregation
 * is done on the C* side
 */
class PDCount(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val count = sc.cassandraTable("ks","tab").cassandraCount()
    if (config.totalOps != count) {
      println(s"Read verification failed! Expected ${config.totalOps}, returned $count");
    }
    println(count)
  }
}

/**
  * SparkSQL Count
  */
class SparkSqlCount(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val count = sqlContext.sql("SELECT COUNT(*) FROM ks.tab").toDF("count").first.getAs[Long]("count")
    if (config.totalOps != count) {
      println(s"Read verification failed! Expected ${config.totalOps}, returned $count");
    }
    println(count)
  }
}

/**
 * Full Table Scan One Column
 * Performs a full table scan but only retrieves a single column from the underlying
 * table.
 */
class FTSOneColumn(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val colorCounts = sc.cassandraTable[String](keyspace, table).select("color").count
    println(colorCounts)
  }
}

/**
  * SparkSQL Full Table Scan One Column
  */
class SparkSqlFTSOneColumn(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val colorCounts = sqlContext.sql("SELECT color FROM ks.tab").count
    println(colorCounts)
  }
}

/**
 * Full Table Scan One Column
 * Performs a full table scan, retrieves all columns and returns the count.
 */
class FTSAllColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sc.cassandraTable[PerfRowClass](keyspace, table).count
    println(s"Loaded $count rows")
  }
}

/**
  * SparkSQL Full Table Scan One Column
  */
class SparkSqlFTSAllColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sqlContext.sql("SELECT * FROM ks.tab").count
    println(s"Loaded $count rows")
  }
}

/**
 * Full Table Scan Five Columns
 * Performs a full table scan and only retreives 5 of the coulmns for each row
 */
class FTSFiveColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sc.cassandraTable[(UUID, Int, String, String, org.joda.time.DateTime)](keyspace, table)
      .select("order_number", "qty", "color", "size", "order_time")
      .count
    println(s"Loaded $count rows")
  }
}

/**
  * SparkSQL Full Table Scan Five Columns
  */
class SparkSqlFTSFiveColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sqlContext.sql("SELECT order_number,qty,color,size,order_time FROM ks.tab").count
    println(s"Loaded $count rows")
  }
}

/**
 * Full Table Scan with a Clustering Column Predicate Pushed down to C*
 */
class FTSPDClusteringAllColumns(config: Config, sc: SparkContext) extends ReadTask(config,
  sc) {
  def run(): Unit = {
    val count = sc.cassandraTable[PerfRowClass](keyspace, table)
      .where("order_time < ?", timePivot)
      .count
    println(s"Loaded $count rows")
  }
}

/**
  * SparkSQL Full Table Scan with a Clustering Column where clause
  */
class SparkSqlFTSClusteringAllColumns(config: Config, sc: SparkContext) extends ReadTask(config,
  sc) {
  def run(): Unit = {
    val count = sqlContext.sql(s"""SELECT * FROM ks.tab WHERE order_time < "$timePivot" """).count
    println(s"Loaded $count rows")
  }
}

/**
 * Full Table Scan with a Clustering Column Predicate Pushed down to C*
 * Only 5 columns retreived per row
 */
class FTSPDClusteringFiveColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sc.cassandraTable[(UUID, Int, String, String, org.joda.time.DateTime)](keyspace, table)
      .where("order_time < ?", timePivot)
      .select("order_number", "qty", "color", "size", "order_time")
      .count
    println(s"Loaded $count rows")
  }
}

/**
  * SparkSQL Full Table Scan with a Clustering Column where clause, only 5 columns retreived per row
  */
class SparkSqlFTSClusteringFiveColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sqlContext.sql(s"""SELECT qty,color,size,order_time FROM ks.tab WHERE order_time < "$timePivot" """).count
    println(s"Loaded $count rows")
  }
}

/**
 * Join With C* with 1M Partition Key requests
 */
class JWCAllColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sc.parallelize(1 to tenthKeys)
      .map(num => Tuple1(s"Store $num"))
      .joinWithCassandraTable[PerfRowClass](keyspace, table)
      .count
    println(s"Loaded $count rows")
  }
}

/**
  * SparkSQL join table with itself
  */
class SparkSqlJoinAllColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sqlContext.sql("SELECT COUNT(table_b.store) FROM ks.tab AS table_b JOIN ks.tab AS table_a ON table_b.store = table_a.store")
      .toDF("count")
      .first.getAs[Long]("count")
    println(s"Loaded $count rows")
  }
}

/**
 * Join With C* with 1M Partition Key requests
 * A repartitionByCassandraReplica occurs before retreiving the data
 * Note: We do not have a SparkSQL equivalent example
 */
class JWCRPAllColumns(config: Config, sc: SparkContext) extends
ReadTask(config, sc) {
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
 * A clustering column predicate is pushed down to limit data retrieval
 */
class JWCPDClusteringAllColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
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
  * SparkSQL join a table with itself, using where clause on clustering column
  */
class SparkSqlJoinClusteringAllColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sqlContext.sql(
          s"""SELECT COUNT(table_b.store)
              |FROM ks.tab AS table_b
              |JOIN ks.tab AS table_a
              |ON table_b.store = table_a.store
              |WHERE table_a.order_time < "$timePivot" """.stripMargin).toDF("count").first.getAs[Long]("count")
    println(s"Loaded $count rows")
  }
}

/**
 * A single C* partition is retreivied in an RDD
 */
class RetrieveSinglePartition(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val filterResults = sc.cassandraTable[String](keyspace, table)
      .where("store = ? ", "Store 5")
      .collect
    println(filterResults.length)
  }
}

/**
  * SparkSQL grab a single C* partition
  */
class SparkSqlRetrieveSinglePartition(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val filterResults = sqlContext.sql(s"""SELECT * FROM ks.tab WHERE store = "Store 5" """).count
    println(filterResults)
  }
}

/**
  * Run any user-provided SparkSQL query
  */
class SparkSqlRunUserQuery(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    if (userSqlQuery == null) {
      println(s"No user query detected, use the -u or --userSqlQuery options to specify a custom query.")
      val defaultQuery = s"""SELECT * FROM ks.tab WHERE order_time > "$timePivot" AND store = "Store 5" AND order_number < "$uuidPivot" """
      println(s"Running default query: '$defaultQuery'")
      val count = sqlContext.sql(defaultQuery).count
      println(s"Loaded $count rows")

    }
    else {
      println(s"The user defined query is: '$userSqlQuery'")
      val results = sqlContext.sql(userSqlQuery)
      println(results)
    }
  }
}

/**
  * SparkSQL query to slice on primary key columns
  * Experimental: Temporary until SparkSqlRunUserQuery is finished.
  * The timePivot should be roughly in the middle of the expected values.
  * The uuidPivot is arbitrary (I think), the expected values are random.
  * The store value is just to isolate on one parition key.
  */
class SparkSlicePrimaryKey(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sqlContext.sql(
      s"""SELECT * FROM ks.tab
         |WHERE order_time > "$timePivot"
         |AND store = "Store 5"
         |AND order_number < "$uuidPivot" """.stripMargin).count
    println(s"Loaded $count rows")
  }
}

/**
  * SparkSQL query to slice non primary key columns
  * Experimental: Temporary until SparkSqlRunUserQuery is finished.
  * Here we are slicing on the qty column, the value upper-bound is 10,000, so we're cutting
  *   out 90% of the data by using 'qty < 1000'.
  */
class SparkSliceNonePrimaryKey(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sqlContext.sql(
      s"""SELECT * FROM ks.tab WHERE qty < 1000 """.stripMargin).count
    println(s"Loaded $count rows")
  }
}