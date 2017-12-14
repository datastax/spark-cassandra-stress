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

  val rdd_config = new Config(
    testName = "readperfks",
    keyspace = "readperfks",
    numPartitions = 10,
    totalOps = 10000,
    numTotalKeys = 200,
    distributedDataType = "rdd")

  val dataset_config = new Config(
    testName = "readperfks",
    keyspace = "readperfks",
    numPartitions = 10,
    totalOps = 10000,
    numTotalKeys = 200,
    distributedDataType = "dataset")

  val ss = ConnectHelper.getSparkSession(defaultSparkConf)

  override def beforeAll(): Unit = {
    // Allow us to rerun tests with a clean slate
    val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
    conn.withSessionDo { session => session.execute(s"""DROP KEYSPACE IF EXISTS readperfks """)}
    Thread.sleep(5000)

    conn.withSessionDo { session =>
      session.execute(
        s"""
           |CREATE KEYSPACE IF NOT EXISTS readperfks WITH
           |REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """
          .stripMargin)}

    val writer = new WritePerfRow(rdd_config, ss)
    writer.setupCQL
    writer.run
  }

  "FTSOneColumn" should " be able to run" in {
    new FTSOneColumn(rdd_config, ss).run
    new FTSOneColumn(dataset_config, ss).run
  }

  "FTSAllColumns" should " be able to run" in {
    new FTSAllColumns(rdd_config, ss).run
    new FTSAllColumns(dataset_config, ss).run
  }

  "PDCount" should " be able to run" in {
    new PDCount(rdd_config, ss).run
    new PDCount(dataset_config, ss).run
  }

  "FTSFiveColumns" should " be able to run " in {
    new FTSFiveColumns(rdd_config, ss).run
    new FTSFiveColumns(dataset_config, ss).run
  }

  "FTSPDClusteringAllColumns" should " be able to run" in {
    new FTSPDClusteringAllColumns(rdd_config, ss).run
    new FTSPDClusteringAllColumns(dataset_config, ss).run
  }

  "FTSPDClusteringFiveColumns" should " be able to run" in {
    new FTSPDClusteringFiveColumns(rdd_config, ss).run
    new FTSPDClusteringFiveColumns(dataset_config, ss).run
  }

  "JWCAllColumns" should " be able to run" in {
    new JWCAllColumns(rdd_config, ss).run
    new JWCAllColumns(dataset_config, ss).run
  }

  "JWCRPAllColumns" should " be able to run " in {
    new JWCRPAllColumns(rdd_config, ss).run
    new JWCRPAllColumns(dataset_config, ss).run
  }

  "JWCPDClusteringAllColumns" should " be able to run " in {
    new JWCPDClusteringAllColumns(rdd_config, ss).run
    new JWCPDClusteringAllColumns(dataset_config, ss).run
  }

  "RetrieveSinglePartiton" should " be able to run " in {
    new RetrieveSinglePartition(rdd_config,ss).run
    new RetrieveSinglePartition(dataset_config,ss).run
  }
}
