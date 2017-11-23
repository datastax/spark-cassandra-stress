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

  val ss = ConnectHelper.getSparkSession(defaultSparkConf)

  override def beforeAll(): Unit = {
    val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
    conn.withSessionDo { session =>
      session.execute(
        s"""
           |CREATE KEYSPACE IF NOT EXISTS readperfks WITH
           |REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """
          .stripMargin)}

    val writer = new WritePerfRow(config, ss)
    writer.setupCQL
    writer.run
  }

  "FTSOneColumn" should " be able to run" in {
    new FTSOneColumn(config, ss).run
  }

  "FTSAllColumns" should " be able to run" in {
    new FTSAllColumns(config, ss).run
  }

  "PDCount" should " be able to run" in {
    new PDCount(config, ss).run
  }

  "FTSFiveColumns" should " be able to run " in {
    new FTSFiveColumns(config, ss).run
  }

  "FTSPDClusteringAllColumns" should " be able to run" in {
    new FTSPDClusteringAllColumns(config, ss).run
  }

  "FTSPDClusteringFiveColumns" should " be able to run" in {
    new FTSPDClusteringFiveColumns(config, ss).run
  }

  "JWCAllColumns" should " be able to run" in {
    new JWCAllColumns(config, ss).run
  }

  "JWCRPAllColumns" should " be able to run " in {
    new JWCRPAllColumns(config, ss).run
  }

  "JWCPDClusteringAllColumns" should " be able to run " in {
    new JWCPDClusteringAllColumns(config, ss).run
  }

  "RetrieveSinglePartiton" should " be able to run " in {
    new RetrieveSinglePartition(config,ss).run
  }



}
