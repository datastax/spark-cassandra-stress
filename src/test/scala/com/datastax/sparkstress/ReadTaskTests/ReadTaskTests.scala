package com.datastax.sparkstress.ReadTaskTests

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.sparkstress.Config
import org.junit.runner.RunWith
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}
import org.scalatest.junit.JUnitRunner
import com.datastax.sparkstress._

@RunWith(classOf[JUnitRunner])
class ReadTaskTests extends FlatSpec
  with BeforeAndAfterAll
  with Matchers{

  def clearCache(): Unit = CassandraConnector.evictCache()

  val rdd_config = new Config(
    testName = "readperfks",
    keyspace = "readperfks",
    numPartitions = 10,
    totalOps = 10000,
    numTotalKeys = 200,
    distributedDataType = DistributedDataType.RDD)

  val dataset_config = new Config(
    testName = "readperfks",
    keyspace = "readperfks",
    numPartitions = 10,
    totalOps = 10000,
    numTotalKeys = 200,
    distributedDataType = DistributedDataType.DataFrame)

  val ss = ConnectHelper.getSparkSession()

  override def beforeAll(): Unit = {
    // Allow us to rerun tests with a clean slate
    val conn = CassandraConnector(ss.sparkContext.getConf)
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

  val testMatrix = Seq(
    (rdd_config, DistributedDataType.RDD),
    (dataset_config, DistributedDataType.DataFrame)
  )

  for ((conf, confName) <- testMatrix) {

    val testDescription = s" be able to run with $confName"

    "FTSOneColumn" should s" be able to run with $confName" in {
      new FTSOneColumn(conf, ss).run should be > 0L
    }

    "FTSAllColumns" should s" be able to run with $confName" in {
      new FTSAllColumns(conf, ss).run should be > 0L
    }

    "PDCount" should s" be able to run with $confName" in {
      new PDCount(conf, ss).run should be > 0L
    }

    "FTSFiveColumns" should s" be able to run with $confName" in {
      new FTSFiveColumns(conf, ss).run should be > 0L
    }

    "FTSPDClusteringAllColumns" should s" be able to run with $confName" in {
      new FTSPDClusteringAllColumns(conf, ss).run should be > 0L
    }

    "FTSPDClusteringFiveColumns" should s" be able to run with $confName" in {
      new FTSPDClusteringFiveColumns(conf, ss).run should be > 0L
    }

    "JWCAllColumns" should s" be able to run with $confName" in {
      new JWCAllColumns(conf, ss).run should be > 0L
    }

    "JWCRPAllColumns" should s" be able to run with $confName" in {
      confName match {
        case DistributedDataType.RDD => new JWCRPAllColumns(conf, ss).run should be > 0L
        case DistributedDataType.DataFrame =>
          intercept[IllegalArgumentException] { new JWCRPAllColumns(conf, ss).run }
      }
    }

    "JWCPDClusteringAllColumns" should s" be able to run with $confName" in {
      new JWCPDClusteringAllColumns(conf, ss).run should be > 0L
    }

    "RetrieveSinglePartiton" should s" be able to run with $confName" in {
      new RetrieveSinglePartition(conf, ss).run should be > 0L
    }
  }
}
