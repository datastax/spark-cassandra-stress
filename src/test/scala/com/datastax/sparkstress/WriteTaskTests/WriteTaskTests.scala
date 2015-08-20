package com.datastax.sparkstress.WriteTaskTests

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{SparkTemplate, EmbeddedCassandra}
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.datastax.sparkstress._

@RunWith(classOf[JUnitRunner])
class WriteTaskTests extends FlatSpec
  with EmbeddedCassandra
  with SparkTemplate
  with BeforeAndAfterAll
  with Matchers{

  def clearCache(): Unit = CassandraConnector.evictCache()
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
  conn.withSessionDo { session =>
    session.execute(s"""CREATE KEYSPACE IF NOT EXISTS test1 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE IF NOT EXISTS test2 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE IF NOT EXISTS test3 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE IF NOT EXISTS test4 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE IF NOT EXISTS test5 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE IF NOT EXISTS test6 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE IF NOT EXISTS test7 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
  }

  "The RDD" should "have the correct configurations" in {
    val config = new Config(keyspace = "test1", numPartitions = 1, totalOps = 20, numTotalKeys = 1)
    val writer = new WriteShortRow(config, sc)
    val rdd = writer.getRDD
    rdd.partitions.length should be (config.numPartitions)
    rdd.count should be (config.totalOps)
  }

 "WriteShortRow" should "save correctly" in {
    val config = new Config(keyspace = "test3", numPartitions = 1, totalOps = 6, numTotalKeys = 1)
    val writer = new WriteShortRow(config, sc)
    writer.setupCQL
    writer.run
    sc.cassandraTable(config.keyspace,config.table).count should be (6)
  }

  "WriteWideRow" should "save correctly" in {
    val config = new Config(
      testName = "writewiderow", 
      keyspace = "test4", 
      numPartitions = 1, 
      totalOps = 8, 
      numTotalKeys = 1)
    val writer = new WriteWideRow(config, sc)
    writer.setupCQL
    writer.run
    sc.cassandraTable(config.keyspace,config.table).count should be (8)
  }

  "WriteRandomWideRow" should "save correctly" in {
    val config = new Config(
      testName = "writerandomwiderow", 
      keyspace = "test5", 
      numPartitions = 10, 
      totalOps = 20, 
      numTotalKeys = 1)
    val writer = new WriteRandomWideRow(config, sc)
    writer.setupCQL
    writer.run
    sc.cassandraTable(config.keyspace,config.table).count should be (20)
  }

  "WriteWideRowByPartition" should "save correctly" in {
    val config = new Config(
      testName = "writewiderowbypartition", 
      keyspace = "test6", 
      numPartitions = 1, 
      totalOps = 40, 
      numTotalKeys = 5)
    val writer = new WriteWideRowByPartition(config, sc)
    writer.setupCQL
    writer.run
    sc.cassandraTable(config.keyspace,config.table).count should be (40)
  }

  "WritePerfRow" should " generate the correct number of pks" in {
    val config = new Config(
      testName = "writeperfrow",
      keyspace = "test7",
      numPartitions = 5,
      totalOps = 1000,
      numTotalKeys = 200)
    val writer = new WritePerfRow(config, sc)
    val results = writer.getRDD.map(_.store).countByValue()
    results should have size (200)
  }

  it should "generate the correct number of cks per pk" in {
    val config = new Config(
      testName = "writeperfrow",
      keyspace = "test7",
      numPartitions = 2,
      totalOps = 40,
      numTotalKeys = 4)
    val writer = new WritePerfRow(config, sc)
    val rowLengths = writer.getRDD.groupBy( u=> u.store).map(row => row._2).collect
    for (row <- rowLengths)
      row should have size 10
  }

  it should " write to C*" in {
     val config = new Config(
      testName = "writeperfrow",
      keyspace = "test7",
      numPartitions = 10,
      totalOps = 1000,
      numTotalKeys = 200)
    val writer = new WritePerfRow(config, sc)
    writer.setupCQL
    writer.run
    sc.cassandraTable(config.keyspace, config.table).count should be (1000)
  }

}