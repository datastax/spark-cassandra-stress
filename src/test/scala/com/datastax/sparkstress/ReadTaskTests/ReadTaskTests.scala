package com.datastax.sparkstress.ReadTaskTests

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{SparkTemplate, EmbeddedCassandra}
import com.datastax.sparkstress.Config
import org.junit.runner.RunWith
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}
import org.scalatest.junit.JUnitRunner
import com.datastax.sparkstress._

@RunWith(classOf[JUnitRunner])
class ReadTaskTests extends FlatSpec
  with EmbeddedCassandra
  with SparkTemplate
  with BeforeAndAfterAll
  with Matchers{

  def clearCache(): Unit = CassandraConnector.evictCache()

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val config = new Config(
    testName = "readperfks",
    keyspace = "readperfks",
    numPartitions = 10,
    totalOps = 10000,
    numTotalKeys = 200)

  override def beforeAll(): Unit = {
    val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
    conn.withSessionDo { session =>
      session.execute(
        s"""
           |CREATE KEYSPACE readperfks WITH
           |REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """
          .stripMargin)}

    val writer = new WritePerfRow(config, sc)
    writer.setupCQL
    writer.run
  }

  "AggregateColor" should " be able to run" in {
    new AggregateColor(config, sc).run
  }

  "AggregateColorSize" should " be able to run" in {
    new AggregateColorSize(config, sc).run
  }

  "CountAll" should " be able to run" in {
    new CountAll(config, sc).run
  }

  "FilterColorStringMatchCount" should " be able to run " in {
    new FilterColorStringMatchCount(config, sc).run
  }

  "FilterEqualityQtyColorSizeCount" should " be able to run" in {
    new FilterEqualityQtyColorSizeCount(config, sc).run
  }

  "FilterEqualityQtyColorSizeCountFiveCols" should " be able to run" in {
    new FilterEqualityQtyColorSizeCountFiveCols(config, sc).run
  }

  "FilterLessThanQtyEqualityColorSizeCount" should " be able to run" in {
    new FilterLessThanQtyEqualityColorSizeCount(config, sc).run
  }

  "FilterLessThanQtyequalityColorSizeCountFiveCols" should " be able to run " in {
    new FilterLessThanQtyEqualityColorSizeCountFiveCols(config, sc).run
  }

  "FilterQtyMatchCount" should " be able to run " in {
    new FilterQtyMatchCount(config, sc).run
  }

  "RetrieveSinglePartiton" should " be able to run " in {
    new RetrieveSinglePartition(config,sc).run
  }



}
