package com.datastax.sparkstress.WriteTaskTests

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{SparkTemplate, EmbeddedCassandra}
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.datastax.sparkstress._
import org.apache.spark.SparkConf

@RunWith(classOf[JUnitRunner])
class WriteTaskTests extends FlatSpec
  with EmbeddedCassandra
  with SparkTemplate
  with BeforeAndAfterAll
  with Matchers{

  val sparkConf =
    new SparkConf()
      .setAppName("SparkStressTest")
      .set("spark.master", "local[*]") // without we get: 'org.apache.spark.SparkException: A master URL must be set in your configuration'
      .set("spark.hadoop.fs.dsefs.impl", "com.datastax.bdp.fs.hadoop.DseFileSystem") // without we get: 'java.io.IOException: No FileSystem for scheme: dsefs'

  def clearCache(): Unit = CassandraConnector.evictCache()
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(sparkConf)

  // Allow us to rerun tests with a clean slate
  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
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

  val ss = ConnectHelper.getSparkSession(sparkConf)

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
      keyspace = "test2",
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = "dataset",
      saveMethod = "driver")
    val writer = new WritePerfRow(config, ss)
    writer.setupCQL
    writer.run
    ss.sparkContext.cassandraTable(config.keyspace, config.table).count should be (1000)
  }

  it should " save to DSEFS using parquet format" in {
    val config = new Config(
      testName = "WritePerfRow_Parquet",
      keyspace = "test2",
      table = "parquet_test",
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = "dataset",
      saveMethod = "parquet")
    val writer = new WritePerfRow(config, ss)
    writer.run
    ss.read.parquet(s"dsefs:///${config.keyspace}.${config.table}").count should be (1000)
  }

  it should " save to DSEFS using text format" in {
    val config = new Config(
      testName = "WritePerfRow_Text",
      keyspace = "test2",
      table = "text_test",
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = "dataset",
      saveMethod = "text")
    val writer = new WritePerfRow(config, ss)
    writer.run
    ss.read.text(s"dsefs:///${config.keyspace}.${config.table}").count should be (1000)
  }

  it should " save to DSEFS using json format" in {
    val config = new Config(
      testName = "WritePerfRow_JSON",
      keyspace = "test2",
      table = "json_test",
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = "dataset",
      saveMethod = "json")
    val writer = new WritePerfRow(config, ss)
    writer.run
    ss.read.json(s"dsefs:///${config.keyspace}.${config.table}").count should be (1000)
  }

  it should " save to DSEFS using csv format" in {
    val config = new Config(
      testName = "WritePerfRow_CSV",
      keyspace = "test2",
      table = "csv_test",
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200,
      distributedDataType = "dataset",
      saveMethod = "csv")
    val writer = new WritePerfRow(config, ss)
    writer.run
    ss.read.csv(s"dsefs:///${config.keyspace}.${config.table}").count should be (1000)
  }
}