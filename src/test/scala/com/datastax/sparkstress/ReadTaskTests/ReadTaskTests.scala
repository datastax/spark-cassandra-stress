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
           |CREATE KEYSPACE IF NOT EXISTS readperfks WITH
           |REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """
          .stripMargin)}

    val writer = new WritePerfRow(config, sc)
    writer.setupCQL
    writer.run
  }

  "FTSOneColumn" should " be able to run" in {
    new FTSOneColumn(config, sc).run
  }

  "FTSAllColumns" should " be able to run" in {
    new FTSAllColumns(config, sc).run
  }

  "PDCount" should " be able to run" in {
    new PDCount(config, sc).run
  }

  "FTSFiveColumns" should " be able to run " in {
    new FTSFiveColumns(config, sc).run
  }

  "FTSPDClusteringAllColumns" should " be able to run" in {
    new FTSPDClusteringAllColumns(config, sc).run
  }

  "FTSPDClusteringFiveColumns" should " be able to run" in {
    new FTSPDClusteringFiveColumns(config, sc).run
  }

  "JWCAllColumns" should " be able to run" in {
    new JWCAllColumns(config, sc).run
  }

  "JWCRPAllColumns" should " be able to run " in {
    new JWCRPAllColumns(config, sc).run
  }

  "JWCPDClusteringAllColumns" should " be able to run " in {
    new JWCPDClusteringAllColumns(config, sc).run
  }

  "RetrieveSinglePartiton" should " be able to run " in {
    new RetrieveSinglePartition(config,sc).run
  }



}
