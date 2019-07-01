package com.datastax.sparkstress

import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.sparkstress.RowTypes.PerfRowClass
import com.datastax.spark.connector._
import com.datastax.sparkstress.SparkStressImplicits._
import org.joda.time.DateTime
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import SaveMethod._
import DistributedDataType._

import scala.collection.mutable
import scala.util.Random

abstract class ReadTask(config: Config, ss: SparkSession) extends StressTask {

  val sc = ss.sparkContext
  val uuidPivot = UUID.fromString("9b657ca1-bfb1-49c0-85f5-04b127adc6f3")
  val timePivot = new DateTime(2000, 1, 1, 0, 0, 0, 0).plusSeconds(500)
  val keyspace = config.keyspace
  val table = config.table

  val numberNodes = CassandraConnector(sc.getConf).withClusterDo(_.getMetadata.getAllHosts.size)
  val tenthKeys: Int = config.numTotalKeys.toInt / 10

  val cores = sys.env.getOrElse("SPARK_WORKER_CORES", "1").toInt * numberNodes
  val defaultParallelism = math.max(sc.defaultParallelism, cores)
  val coresPerNode: Int = defaultParallelism / numberNodes

  final def run(): Long = {
    val count = performTask()
    println(s"Loaded $count rows")
    count
  }

  def performTask(): Long

  def runTrials(ss: SparkSession): Seq[TestResult] = {
    println("About to Start Trials")
    for (trial <- 1 to config.trials) yield {
      TestResult(time(run()), 0L)
    }
  }

  def getDataFrame(): DataFrame = {
    config.saveMethod match {
      // regular read method from DSE/Cassandra
      case Driver => ss
        .read
        .cassandraFormat(table, keyspace)
        .load()
      // filesystem read methods
      case _ => ss.read.format(config.saveMethod.toString).load(s"dsefs:///${keyspace}.${table}")
    }
  }

  def readColumns(columnNames: Seq[String]): Long = {
    val columns: Seq[org.apache.spark.sql.Column] = columnNames.map(col(_))
    getDataFrame().select(columns:_*).rdd.count
  }
}

/**
  * Full Table Scan Two Columns using DataSets
  * Performs a full table scan retrieving two columns from the underlying
  * table.
  */
@ReadTest
class FTSTwoColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  override def performTask(): Long = {
    config.distributedDataType match {
      case RDD => sc.cassandraTable[String](keyspace, table).select("color", "size").count
      case DataFrame => readColumns(Seq("color", "size"))
    }
  }
}

/**
  * Full Table Scan Three Columns using DataSets
  * Performs a full table scan but only retrieves a single column from the underlying
  * table.
  */
@ReadTest
class FTSThreeColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  override def performTask(): Long = {
    config.distributedDataType match {
      case RDD => sc.cassandraTable[String](keyspace, table)
        .select("color", "size", "qty")
        .count
      case DataFrame => readColumns(Seq("color", "size", "qty"))
    }
  }
}

/**
  * Full Table Scan Four Columns using DataSets
  * Performs a full table scan but only retrieves a single column from the underlying
  * table.
  */
@ReadTest
class FTSFourColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  override def performTask(): Long = {
    config.distributedDataType match {
      case RDD => sc.cassandraTable[String](keyspace, table).select("color", "size", "qty",
        "order_number").count
      case DataFrame => readColumns(Seq("color", "size", "qty", "order_number"))
    }
  }
}

/**
  * Push Down Count
  * Uses our internally cassandra count pushdown, this means all of the aggregation
  * is done on the C* side
  */
@ReadTest
class PDCount(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  override def performTask(): Long = config.distributedDataType match {
    case RDD => sc.cassandraTable(keyspace, table).cassandraCount()
    case DataFrame => getDataFrame().count
  }
}

/**
  * Full Table Scan One Column
  * Performs a full table scan but only retrieves a single column from the underlying
  * table.
  */
@ReadTest
class FTSOneColumn(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  override def performTask() = {
    config.distributedDataType match {
      case RDD => sc.cassandraTable[String](keyspace, table).select("color").count
      case DataFrame => readColumns(Seq("color"))
    }
  }
}

/**
  * Full Table Scan One Column
  * Performs a full table scan but only retrieves all columns from the underlying table.
  */
@ReadTest
class FTSAllColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  override def performTask(): Long = {
    config.distributedDataType match {
      case RDD => sc.cassandraTable[PerfRowClass](keyspace, table).count
      case DataFrame => readColumns(Seq("order_number", "qty", "color", "size", "order_time",
        "store"))
    }
  }
}

/**
  * Full Table Scan Five Columns
  * Performs a full table scan and only retrieves 5 of the columns for each row
  */
@ReadTest
class FTSFiveColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  override def performTask(): Long = {
    config.distributedDataType match {
      case RDD =>
        sc.cassandraTable[(UUID, Int, String, String, org.joda.time.DateTime)](keyspace,
          table)
          .select("order_number", "qty", "color", "size", "order_time")
          .count
      case DataFrame => readColumns(Seq("order_number", "qty", "color", "size", "order_time"))
    }
  }
}

/**
  * Full Table Scan with a Clustering Column Predicate Pushed down to C*
  */
@ReadTest
class FTSPDClusteringAllColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  override def performTask(): Long = {
    config.distributedDataType match {
      case RDD =>
        sc.cassandraTable[PerfRowClass](keyspace, table)
          .where("order_time < ?", timePivot)
          .count
      case DataFrame =>
        getDataFrame()
          .filter(col("order_time") < lit(new Timestamp(timePivot.getMillis)))
          .rdd
          .count
    }
  }
}

/**
  * Full Table Scan with a Clustering Column Predicate Pushed down to C*
  * Only 5 columns retrieved per row
  */
@ReadTest
class FTSPDClusteringFiveColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  override def performTask(): Long = config.distributedDataType match {
      case RDD =>
        sc.cassandraTable[(UUID, Int, String, String, org.joda.time.DateTime)](keyspace, table)
          .where("order_time < ?", timePivot)
          .select("order_number", "qty", "color", "size", "order_time")
          .count
      case DataFrame =>
        getDataFrame()
          .filter(col("order_time") < lit(new Timestamp(timePivot.getMillis)))
          .select("order_number", "qty", "color", "size", "order_time")
          .rdd
          .count
  }
}

/**
  * Join With C* with 1M Partition Key requests
  */
@ReadTest
class JWCAllColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  override def performTask(): Long = config.distributedDataType match {
    case RDD =>
      sc.parallelize(1 to tenthKeys)
        .map(num => Tuple1(s"Store $num"))
        .joinWithCassandraTable[PerfRowClass](keyspace, table)
        .count
    case DataFrame =>
      val joinTarget = getDataFrame()
      ss
        .range(1, tenthKeys)
        .select(concat(lit("Store "), col("id")).as("key"))
        .join(joinTarget, joinTarget("store") === col("key"))
        .rdd
        .count
  }
}

/**
  * Join With C* with 1M Partition Key requests
  * A repartitionByCassandraReplica occurs before retrieving the data
  */
@ReadTest
class JWCRPAllColumns(config: Config, ss: SparkSession) extends
  ReadTask(config, ss) {
  override def performTask(): Long = config.distributedDataType match {
    case RDD =>
      sc.parallelize(1 to tenthKeys)
        .map(num => Tuple1(s"Store $num"))
        .repartitionByCassandraReplica(keyspace, table, coresPerNode)
        .joinWithCassandraTable[PerfRowClass](keyspace, table)
        .count
    case DataFrame => throw new IllegalArgumentException("This test is not supported with the Dataset API")
  }
}

/**
  * Join With C* with 1M Partition Key requests
  * A clustering column predicate is pushed down to limit data retrevial
  */
@ReadTest
class JWCPDClusteringAllColumns(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  def performTask: Long = config.distributedDataType match {
    case RDD =>
      sc.parallelize(1 to tenthKeys)
        .map(num => Tuple1(s"Store $num"))
        .joinWithCassandraTable[PerfRowClass](keyspace, table)
        .where("order_time < ?", timePivot)
        .count
    case DataFrame =>
      val joinTarget = getDataFrame()
        .filter(col("order_time") < lit(new Timestamp(timePivot.getMillis)))
      ss
        .range(1, tenthKeys)
        .select(concat(lit("Store "), col("id")).as("key"))
        .join(joinTarget, joinTarget("store") === col("key"))
        .rdd
        .count

  }
}

/**
  * A single C* partition is retrieved in an RDD
  */
@ReadTest
class RetrieveSinglePartition(config: Config, ss: SparkSession) extends ReadTask(config, ss) {
  override def performTask(): Long = config.distributedDataType match {
    case RDD =>
      sc.cassandraTable[String](keyspace, table)
        .where("store = ? ", "Store 5")
        .count
    case DataFrame =>
      getDataFrame()
        .filter(col("store") === "Store 5")
        .rdd
        .count
  }
}

abstract class AbstractInSelect(config: Config, ss: SparkSession) extends ReadTask(config, ss) {

  val random = new Random(config.seed)
  val clusteringKeysCount = config.totalOps / config.numTotalKeys

  def seqOfUnique(f: () => Long, count: Int) = {
    val result = mutable.Set[Long]()
    while (result.size != count) {
      result.add(f())
    }
    result.toSeq
  }

  def randomPartitionKey(): Long = {
    Math.floorMod(Math.abs(random.nextLong()), config.numTotalKeys)
  }

  def randomClusteringKey(): Long = {
    Math.floorMod(Math.abs(random.nextLong()), clusteringKeysCount)
  }

}

/** Query with "IN" clause with `config.inKeysCount` keys for partition column.
  * Requires writewiderowbypartition table created via invocation with exact same parameters as this run. */
@ReadTest
class ShortInSelect(config: Config, ss: SparkSession)
  extends AbstractInSelect(config, ss) {

  override def performTask(): Long = config.distributedDataType match {
    case _ =>
      assert(config.numTotalKeys > config.inClauseKeys, s"Number of C* partitions " +
        s"${config.numTotalKeys} is to small for this test, it should be greater than " +
        s"${config.inClauseKeys}")

      val partitionKeys = seqOfUnique(randomPartitionKey, config.inClauseKeys).mkString(",")
      val clusteringKey = randomClusteringKey()

      ss.sql(s"select * from $keyspace.$table where key in ($partitionKeys) and col1 = '$clusteringKey'").count
  }
}

/** Query with "IN" clause with `config.inKeysCount`/2 keys for partition column and clustering column.
  * Requires writewiderowbypartition table created via invocation with exact same parameters as this run. */
@ReadTest
class WideInSelect(config: Config, ss: SparkSession)
  extends AbstractInSelect(config, ss) {

  override def performTask(): Long = config.distributedDataType match {
    case _ =>

      val requestedClusteringKeys = config.inClauseKeys / 2
      val requestedPartitionKeys = config.inClauseKeys - requestedClusteringKeys

      assert(config.numTotalKeys > requestedPartitionKeys, s"Number of C* partitions " +
        s"${config.numTotalKeys} is to small for this test, it should be greater than " +
        s"$requestedPartitionKeys")
      assert(clusteringKeysCount > requestedClusteringKeys, s"Number of clustering keys per " +
        s"partition $clusteringKeysCount is to small for this test, it should be greater than " +
        s"$requestedClusteringKeys")

      val partitionKeys = seqOfUnique(randomPartitionKey, requestedPartitionKeys).mkString(",")
      val clusteringKeys = seqOfUnique(randomClusteringKey, requestedClusteringKeys).mkString("'","','", "'")

      ss.sql(s"select * from $keyspace.$table where key in ($partitionKeys) and col1 in ($clusteringKeys)").count
  }
}
