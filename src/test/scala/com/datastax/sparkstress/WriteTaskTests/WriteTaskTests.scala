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
    session.execute(s"""CREATE KEYSPACE test1 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE test2 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE test3 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE test4 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE test5 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute(s"""CREATE KEYSPACE test6 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
  }

  "The RDD" should "have the correct configurations" in {
    val config = new Config(keyspace = "test1", numPartitions = 1, totalOps = 20, numTotalKeys = 1)
    val writer = new WriteShortRow(config, sc)
    val rdd = writer.getRDD
    rdd.partitions.length should be (config.numPartitions)
    rdd.count should be (config.totalOps)
  }

  "WriteTask" should "set new config options" in {
    val config = new Config(keyspace = "test2", numPartitions = 1, totalOps = 15, numTotalKeys = 1)
    val writer = new WriteShortRow(config, sc)
    writer.setupCQL
    writer.run
    sc.cassandraTable(config.keyspace,config.table).count should be (15)
    writer.setConfig(new Config(saveMethod = "bulk"))
    writer.config.saveMethod should be ("bulk")
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

}