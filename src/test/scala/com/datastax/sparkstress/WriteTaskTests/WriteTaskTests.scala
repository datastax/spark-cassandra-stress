package com.datastax.sparkstress.WriteTaskTests

import java.util.concurrent.TimeoutException

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.datastax.sparkstress._
import org.apache.spark.{ExposeJobListener, SparkConf}
import com.datastax.bdp.fs.client.{DseFsClient, DseFsClientConf}
import com.datastax.bdp.fs.model.HostAndPort
import com.datastax.bdp.fs.model.FilePath

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}

@RunWith(classOf[JUnitRunner])
class WriteTaskTests extends FlatSpec
  with BeforeAndAfterAll
  with Matchers{

  val ss = ConnectHelper.getSparkSession()

  def clearCache(): Unit = CassandraConnector.evictCache()

  // Allow us to rerun tests with a clean slate
  val conn = CassandraConnector(ss.sparkContext.getConf)
  conn.withSessionDo { session =>
    session.execute(s"""DROP KEYSPACE IF EXISTS test1 """)
    session.execute(s"""DROP KEYSPACE IF EXISTS test2 """)
    session.execute(s"""DROP KEYSPACE IF EXISTS test3 """)
    session.execute(s"""DROP KEYSPACE IF EXISTS test4 """)
    session.execute(s"""DROP KEYSPACE IF EXISTS test5 """)
    session.execute(s"""DROP KEYSPACE IF EXISTS test6 """)
    session.execute(s"""DROP KEYSPACE IF EXISTS test7 """)
  }
  Thread.sleep(5000)

  conn.withSessionDo { session =>
    session.execute(s"""CREATE KEYSPACE test1 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE test2 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE test3 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE test4 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE test5 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE test6 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE test7 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
  }

  // Wipe content from DSEFS too
  val dseFsClient = new DseFsClient(new DseFsClientConf(Seq(HostAndPort.defaultPublicDseFsEndpoint("localhost"))))
  val parquetTableName = "parquet_test"
  val textTableName = "text_test"
  val jsonTableName = "json_test"
  val csvTableName = "csv_test"
  val datasetTestKS = "test2"

  try {
    Await.result(Future {dseFsClient.deleteRecursive(FilePath(s"/$datasetTestKS.$parquetTableName"))}, Duration(60, SECONDS))
    Await.result(Future {dseFsClient.deleteRecursive(FilePath(s"/$datasetTestKS.$textTableName"))}, Duration(60, SECONDS))
    Await.result(Future {dseFsClient.deleteRecursive(FilePath(s"/$datasetTestKS.$jsonTableName"))}, Duration(60, SECONDS))
    Await.result(Future {dseFsClient.deleteRecursive(FilePath(s"/$datasetTestKS.$csvTableName"))}, Duration(60, SECONDS))

  } catch {
    case ex: TimeoutException => {
      println(s"We timed out waiting to clear DSEFS before testing.")
    }
  }


  "The RDD" should "have the correct configurations" in {
    val config = new Config(keyspace = "test1", numPartitions = 1, totalOps = 20, numTotalKeys = 1)
    val writer = new WriteShortRow(config, ss)
    val rdd = writer.getRDD
    rdd.partitions.length should be (config.numPartitions)
    rdd.count should be (config.totalOps)
  }

 "WriteShortRow" should "save correctly" in {
    val config = new Config(keyspace = "test3", numPartitions = 1, totalOps = 6, numTotalKeys = 1)
    val writer = new WriteShortRow(config, ss)
    writer.setupCQL
    writer.run
    ss.sparkContext.cassandraTable(config.keyspace,config.table).count should be (6)
  }

  "WriteWideRow" should "save correctly" in {
    val config = new Config(
      testName = "WriteWideRow",
      keyspace = "test4", 
      numPartitions = 1, 
      totalOps = 8, 
      numTotalKeys = 1)
    val writer = new WriteWideRow(config, ss)
    writer.setupCQL
    writer.run
    ss.sparkContext.cassandraTable(config.keyspace,config.table).count should be (8)
  }

  "WriteRandomWideRow" should "save correctly" in {
    val config = new Config(
      testName = "WriteRandomWideRow",
      keyspace = "test5", 
      numPartitions = 10, 
      totalOps = 20, 
      numTotalKeys = 1)
    val writer = new WriteRandomWideRow(config, ss)
    writer.setupCQL
    writer.run
    ss.sparkContext.cassandraTable(config.keyspace,config.table).count should be (20)
  }

  "WriteWideRowByPartition" should "save correctly" in {
    val config = new Config(
      testName = "WriteWideRowByPartition",
      keyspace = "test6", 
      numPartitions = 1, 
      totalOps = 40, 
      numTotalKeys = 5)
    val writer = new WriteWideRowByPartition(config, ss)
    writer.setupCQL
    writer.run
    ss.sparkContext.cassandraTable(config.keyspace,config.table).count should be (40)
  }

  "WritePerfRow" should " generate the correct number of pks" in {
    val config = new Config(
      testName = "WritePerfRow",
      keyspace = "test7",
      numPartitions = 5,
      totalOps = 1000,
      numTotalKeys = 200)
    val writer = new WritePerfRow(config, ss)
    val results = writer.getRDD.map(_.store).countByValue()
    results should have size (200)
  }

  it should "generate the correct number of cks per pk" in {
    val config = new Config(
      testName = "WritePerfRow",
      keyspace = "test7",
      numPartitions = 2,
      totalOps = 40,
      numTotalKeys = 4)
    val writer = new WritePerfRow(config, ss)
    val rowLengths = writer.getRDD.groupBy( u=> u.store).map(row => row._2).collect
    for (row <- rowLengths)
      row should have size 10
  }

  it should " write to C*" in {
     val config = new Config(
      testName = "WritePerfRow",
      keyspace = "test7",
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200)
    val writer = new WritePerfRow(config, ss)
    writer.setupCQL
    writer.run
    ss.sparkContext.cassandraTable(config.keyspace, config.table).count should be (1000)
  }

  it should " save to C* using Dataset API" in {
    val config = new Config(
      testName = "WritePerfRow_DS_Cass",
      keyspace = datasetTestKS,
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = DistributedDataType.DataFrame,
      saveMethod = SaveMethod.Driver)
    val writer = new WritePerfRow(config, ss)
    writer.setupCQL
    writer.run
    ss.sparkContext.cassandraTable(config.keyspace, config.table).count should be (1000)
  }

  it should " copy a table using Dataset API" in {
    val config = new Config(
      testName = "CopyTable",
      table = "copyds",
      keyspace = datasetTestKS,
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = DistributedDataType.DataFrame,
      saveMethod = SaveMethod.Driver)

    val writer = new WritePerfRow(config, ss)
    writer.setupCQL
    writer.run
    val copier = new CopyTable(config, ss)
    copier.setupCQL
    copier.run
    ss.sparkContext.cassandraTable(config.keyspace, s"${config.table}_copy").count should be (1000)
  }

  it should " copy a table using RDD API" in {
    val config = new Config(
      testName = "CopyTableRDD",
      table = "copyrdd",
      keyspace = datasetTestKS,
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = DistributedDataType.RDD,
      saveMethod = SaveMethod.Driver)

    val writer = new WritePerfRow(config, ss)
    writer.setupCQL
    writer.run
    val copier = new CopyTable(config, ss)
    copier.setupCQL
    copier.run

    ss.sparkContext.cassandraTable(config.keyspace, s"${config.table}_copy").count should be (1000)
  }

  it should " save to DSEFS using parquet format" in {
    val config = new Config(
      testName = "WritePerfRow_Parquet",
      keyspace = datasetTestKS,
      table = parquetTableName,
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = DistributedDataType.DataFrame,
      saveMethod = SaveMethod.Parquet)
    val writer = new WritePerfRow(config, ss)
    writer.run
    ss.read.parquet(s"dsefs:///${config.keyspace}.${config.table}").count should be (1000)
  }

  it should " save to DSEFS using text format" in {
    val config = new Config(
      testName = "WritePerfRow_Text",
      keyspace = datasetTestKS,
      table = textTableName,
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = DistributedDataType.DataFrame,
      saveMethod = SaveMethod.Text)
    val writer = new WritePerfRow(config, ss)
    writer.run
    ss.read.text(s"dsefs:///${config.keyspace}.${config.table}").count should be (1000)
  }

  it should " save to DSEFS using json format" in {
    val config = new Config(
      testName = "WritePerfRow_JSON",
      keyspace = datasetTestKS,
      table = jsonTableName,
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = DistributedDataType.DataFrame,
      saveMethod = SaveMethod.Json)
    val writer = new WritePerfRow(config, ss)
    writer.run
    ss.read.json(s"dsefs:///${config.keyspace}.${config.table}").count should be (1000)
  }

  it should " save to DSEFS using csv format" in {
    val config = new Config(
      testName = "WritePerfRow_CSV",
      keyspace = datasetTestKS,
      table = csvTableName,
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = DistributedDataType.DataFrame,
      saveMethod = SaveMethod.Csv)
    val writer = new WritePerfRow(config, ss)
    writer.run
    ss.read.csv(s"dsefs:///${config.keyspace}.${config.table}").count should be (1000)
  }
}