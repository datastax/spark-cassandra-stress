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
  val ValidTasks = Map(
    "ftsallcolumns" -> (new FTSAllColumns(_, _)),
    "ftsfivecolumns" -> (new FTSFiveColumns(_, _)),
    "ftsfourcolumns" -> (new FTSFourColumns(_, _)),
    "ftsthreecolumns" -> (new FTSThreeColumns(_, _)),
    "ftstwocolumns" -> (new FTSTwoColumns(_, _)),
    "ftsonecolumn" -> (new FTSOneColumn(_, _)),

    "ftspdclusteringallcolumns" -> (new FTSPDClusteringAllColumns(_, _)),
    "ftspdclusteringfivecolumns" -> (new FTSPDClusteringFiveColumns(_, _)),
    "jwcallcolumns" -> (new JWCAllColumns(_, _)),
    "jwcpdclusteringallcolumns" -> (new JWCPDClusteringAllColumns(_, _)),
    "jwcrpallcolumns" -> (new JWCRPAllColumns(_, _)),
    "pdcount" -> (new PDCount(_, _)),
    "retrievesinglepartition" -> (new RetrieveSinglePartition(_, _)),
    // SparkSQL
    "sqlftsallcolumns" -> (new SparkSqlFTSAllColumns(_, _)),
    "sqlftsfivecolumns" -> (new SparkSqlFTSFiveColumns(_, _)),
    "sqlftsonecolumn" -> (new SparkSqlFTSOneColumn(_, _)),
    "sqlftsclusteringallcolumns" -> (new SparkSqlFTSClusteringAllColumns(_, _)),
    "sqlftsclusteringfivecolumns" -> (new SparkSqlFTSClusteringFiveColumns(_, _)),
    "sqljoinallcolumns" -> (new SparkSqlJoinAllColumns(_, _)),
    "sqljoinclusteringallcolumns" -> (new SparkSqlJoinClusteringAllColumns(_, _)),
    "sqlcount" -> (new SparkSqlCount(_, _)),
    "sqlretrievesinglepartition" -> (new SparkSqlRetrieveSinglePartition(_, _)),
    "sqlrunuserquery" -> (new SparkSqlRunUserQuery(_, _)),
    "sqlsliceprimarykey" -> (new SparkSlicePrimaryKey(_, _)),
    "sqlslicenonprimarykey" -> (new SparkSliceNonePrimaryKey(_, _)),

    // Solr benchmarking tests
    "sqlftsclustering" -> (new SQLFTSClustering(_, _)),
    "sqlftsclusteringdata"  -> (new SQLFTSClusteringData(_, _)),
    "sqlftsdata" -> (new SQLFTSData(_, _)),
    "sqlftspk" -> (new SQLFTSPkRestriction(_, _)),
    "sqlftspks" -> (new SQLFTSPkRestrictionSelect(_, _))
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
    val count = sqlContext.sql("SELECT COUNT(*) FROM ${keyspace}.${table}").toDF("count").first.getAs[Long]("count")
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
  * Full Table Scan Two Columns
  * Performs a full table scan but only retrieves two columns from the underlying
  * table.
  */
class FTSTwoColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val colorCounts = sc.cassandraTable[(String, String)](keyspace, table).select("color", "size").count
    println(colorCounts)
  }
}

/**
  * Full Table Scan Three Columns
  * Performs a full table scan but only retrieves three columns from the underlying
  * table.
  */
class FTSThreeColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val colorCounts = sc.cassandraTable[(String, String, Int)](keyspace, table).select("color", "size", "qty").count
    println(colorCounts)
  }
}

/**
  * Full Table Scan Four Columns
  * Performs a full table scan but only retrieves four columns from the underlying
  * table.
  */
class FTSFourColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val colorCounts = sc.cassandraTable[(String, String, Int, UUID)](keyspace, table).select("color", "size", "qty", "order_number").count
    println(colorCounts)
  }
}

/**
  * SparkSQL Full Table Scan One Column
  */
class SparkSqlFTSOneColumn(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val colorCounts = sqlContext.sql("SELECT color FROM ${keyspace}.${table}").count
    println(colorCounts)
  }
}

/**
 * Full Table Scan All Columns
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
    val count = sqlContext.sql("SELECT * FROM ${keyspace}.${table}").count
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
    val count = sqlContext.sql("SELECT order_number,qty,color,size,order_time FROM ${keyspace}.${table}").count
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
    val count = sqlContext.sql(s"""SELECT * FROM ${keyspace}.${table} WHERE order_time < cast("$timePivot" as timestamp) """).count
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
    val count = sqlContext.sql(s"""SELECT qty,color,size,order_time FROM ${keyspace}.${table} WHERE order_time < cast("$timePivot" as timestamp)""").count
    println(s"Loaded $count rows")
  }
}

/**
 * Join With C* with 1M Partition Key requests
 */
class JWCAllColumns(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val count = sc.parallelize(1 to tenthKeys)
      .map(num => Tuple1(s"Store_$num"))
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
    val count = sqlContext.sql("SELECT COUNT(table_b.store) FROM ${keyspace}.${table} AS table_b JOIN ${keyspace}.${table} AS table_a ON table_b.store = table_a.store")
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
      .map(num => Tuple1(s"Store_$num"))
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
      .map(num => Tuple1(s"Store_$num"))
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
              |FROM ${keyspace}.${table} AS table_b
              |JOIN ${keyspace}.${table} AS table_a
              |ON table_b.store = table_a.store
              |WHERE table_a.order_time < cast("$timePivot" as timeStamp) """.stripMargin).toDF("count").first.getAs[Long]("count")
    println(s"Loaded $count rows")
  }
}

/**
 * A single C* partition is retreivied in an RDD
 */
class RetrieveSinglePartition(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val filterResults = sc.cassandraTable[String](keyspace, table)
      .where("store = ? ", "Store_5")
      .collect
    println(filterResults.length)
  }
}

/**
  * SparkSQL grab a single C* partition
  */
class SparkSqlRetrieveSinglePartition(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  val storenum = 5
  def run(): Unit = {
    val filterResults = sqlContext.sql(f"""SELECT * FROM ${keyspace}.${table} WHERE store = "Store_$storenum%012d" """).count
    println(filterResults)
  }
}

/**
  * Run any user-provided SparkSQL query
  */
class SparkSqlRunUserQuery(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  val defaultQuery =
    s"""SELECT * FROM ${keyspace}.${table}
       |WHERE order_time > cast("$timePivot" as timestamp)
       |AND order_number < "$uuidPivot" """.stripMargin
  def run(): Unit = {
    if (userSqlQuery == null) {
      println(s"No user query detected, use the -u or --userSqlQuery options to specify a custom query.")
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

abstract class SQLReadBase(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  val query: String

  override def run() = {
    println(query)
    val count = sqlContext.sql(query).count
    println(s"Loaded $count rows")
  }

}

/**
  * SparkSql Query doing a Full Table Scan while slicing on a Clustering Column
  */
class SQLFTSClustering(config: Config, sc: SparkContext) extends SQLReadBase(config, sc) {
  val oneTenthPivot = timePivot.minusSeconds(400)
  override val query =
    s"""SELECT * FROM ${keyspace}.${table} WHERE order_time < cast("$timePivot" as timestamp) """.stripMargin
}

/**
  * Full table scan while slicing on a clustering column and selecting for a data column
  */
class SQLFTSClusteringData(config: Config, sc: SparkContext) extends SQLReadBase(config, sc) {
  val oneTenthPivot = timePivot.minusSeconds(400)
  override val query =
    s"""SELECT * FROM ${keyspace}.${table}
       |WHERE order_time < cast("$timePivot" as timestamp)
       |AND size = "XXL" """.stripMargin
}

/**
  * Full Table Scan While Selecting for a data column
  */
class SQLFTSData(config: Config, sc: SparkContext) extends SQLReadBase(config, sc) {
  override val query =
    s"""SELECT * FROM ${keyspace}.${table}
       |WHERE size = "XXL" """.stripMargin
}

/**
  * Full Table Scan while restricting to only a fraction of loaded Cassandra Partitions
  */
class SQLFTSPkRestriction(config: Config, sc: SparkContext) extends SQLReadBase(config, sc) {
  val pivot = math.round(config.numTotalKeys * config.fractionOfData)
  override val query =
    f"""SELECT * FROM ${keyspace}.${table}
       |WHERE store <= "Store_$pivot%012d" """.stripMargin
}

class SQLFTSPkRestrictionSelect(config: Config, sc: SparkContext) extends SQLReadBase(config, sc) {
  val pivot = math.round(config.numTotalKeys * config.fractionOfData)
  override val query =
    f"""SELECT store FROM ${keyspace}.${table}
       |WHERE store <= "Store_$pivot%012d" """.stripMargin
}

/**
  * SparkSQL query to slice on primary key columns
  * Experimental: Temporary until SparkSqlRunUserQuery is finished.
  * The timePivot should be roughly in the middle of the expected values.
  * The uuidPivot is arbitrary (I think), the expected values are random.
  * The store value is just to isolate on one partition key.
  */
class SparkSlicePrimaryKey(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  val storenum = 5
  val query =
    f"""SELECT * FROM ${keyspace}.${table}
      |WHERE order_time < cast("$timePivot" as timestamp)
      |AND store = "Store_$storenum%012d" AND order_number > "$uuidPivot" """.stripMargin
  def run(): Unit = {
    println(query)
    val count = sqlContext.sql(query).count
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
      s"""SELECT * FROM ${keyspace}.${table} WHERE qty < 1000 """.stripMargin).count
    println(s"Loaded $count rows")
  }
}
