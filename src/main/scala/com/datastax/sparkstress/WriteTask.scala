package com.datastax.sparkstress

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{ExposeJobListener, SparkContext}
import com.datastax.sparkstress.RowTypes._
import com.datastax.spark.connector._
import com.datastax.spark.connector.RDDFunctions
import com.datastax.bdp.spark.writer.BulkTableWriter._
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import java.util.concurrent.TimeoutException
import scala.concurrent.{Await,Future}
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.commons.io.IOUtils
import scala.util.parsing.json.{JSON, JSONType, JSONArray, JSONObject}
import java.net.URL

object WriteTask {
  val ValidTasks = Set(
    "writeshortrow",
    "writeperfrow",
    "writewiderow",
    "writerandomwiderow",
    "writewiderowbypartition"
  )
}

abstract class WriteTask[rowType](
  val config: Config,
  val sc: SparkContext)
  (implicit rwf: RowWriterFactory[rowType]) extends StressTask {

  def setupCQL() = {
    CassandraConnector(sc.getConf).withSessionDo{ session =>
      if (config.deleteKeyspace){
        println(s"Destroying Keyspace")
        session.execute(s"DROP KEYSPACE IF EXISTS ${config.keyspace}")
      }
      val kscql = getKeyspaceCql(config.keyspace)
      val tbcql = getTableCql(config.table)
      println(s"""Running the following create statements\n$kscql\n${tbcql.mkString("\n")})""")
      session.execute(kscql)
      session.execute(s"USE ${config.keyspace}")
      for (cql <- tbcql)
       session.execute(cql)
    }
    printf("Done Setting up CQL Keyspace/Table\n")
  }

  def getKeyspaceCql(ksName: String): String = s"CREATE KEYSPACE IF NOT EXISTS $ksName WITH replication = {'class': '${config.replicationStrategy}', '${config.replicationDC}': ${config.replicationFactor} }"

  def getTableCql(tbName: String): Seq[String]

  def getRDD: RDD[rowType]

  /**
   * Runs the write workload, returns when terminationTimeMinutes is reached or when the job completes, which ever is first.
   * @return a tuple containing (runtime, totalCompletedOps)
   */
  def run(): TestResult = {
    var totalCompletedOps: Long = 0L
    val runtime = time({
      val fs = Future {
        config.saveMethod match {
          case "bulk" => getRDD.bulkSaveToCassandra(config.keyspace, config.table)
          case _ => new RDDFunctions(getRDD).saveToCassandra(config.keyspace, config.table)
          //For Some reason the implicit doesn't work here in Connector 1.[1,0].X
        }
      }
      try {
        if (config.terminationTimeMinutes > 0) {
          Await.result(fs, Duration(TimeUnit.MINUTES.toMillis(config.terminationTimeMinutes), MILLISECONDS))
        } else {
          Await.result(fs, Duration(Long.MaxValue, NANOSECONDS)) // max allowed duration 292 years :)
        }
      } catch {
        case ex: TimeoutException => {
          println(s"We hit our timeout limit for this test (${config.terminationTimeMinutes} min), shutting down.")
          val host: String = sys.props.get("spark.master").getOrElse(sc.getConf.getOption("spark.master")).toString.split("://|:")(1)
          val appId: String = sc.getConf.getAppId

          /* Follow-up: For now we're only pulling stage id 0, if/when we want to support multiple trials we may want to
          pass in the trial number to run() and use that as the stage id. */
          val stageId = 0
          val stageAttemptId = 0
          val stageDataOption = ExposeJobListener.getjobListener(sc).stageIdToData.get((stageId, stageAttemptId))
          val stageData = stageDataOption.get
          val outputRecords = stageData.outputRecords
          println(s"\n\noutputRecords: ${outputRecords}\n\n")
          totalCompletedOps = outputRecords
        }
      }
    })
    if (totalCompletedOps == 0L) TestResult(runtime, config.totalOps) else TestResult(TimeUnit.MINUTES.toNanos(config.terminationTimeMinutes), totalCompletedOps)
  }

  def runTrials(sc: SparkContext): Seq[TestResult] = {
    println("About to Start Trials")
    for (trial <- 1 to config.trials) yield {setupCQL(); Thread.sleep(10000); run()}
  }

}

/**
 * Writes data to a schema which contains no clustering keys, no partitions will be
 * overwritten.
 */
class WriteShortRow(config: Config, sc: SparkContext) extends
  WriteTask[ShortRowClass](config, sc)(implicitly[RowWriterFactory[ShortRowClass]]) {

  def getTableCql(tbName: String): Seq[String] =
    Seq(s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key bigint, col1 text, col2 text, col3 text, PRIMARY KEY(key))""".stripMargin)

  def getRDD: RDD[ShortRowClass] = {
    println(
      s"""Generating RDD for short rows:
         |${config.totalOps} Total Writes
         |${config.numPartitions} Num Partitions""".stripMargin
    )
    RowGenerator.getShortRowRDD(sc, config.numPartitions, config.totalOps)
  }

}

/**
 * Writes data in a format similar to the DataStax legacy 'tshirt' schema
 */
class WritePerfRow(config: Config, sc: SparkContext) extends
  WriteTask[PerfRowClass](config, sc)(implicitly[RowWriterFactory[PerfRowClass]]) {

  def getTableCql(tbName: String): Seq[String] =
    Seq(
      s"""CREATE TABLE IF NOT EXISTS $tbName
       |(store text,
       |order_time timestamp,
       |order_number uuid,
       |color text,
       |size text,
       |qty int,
       |PRIMARY KEY (store, order_time, order_number))
     """.stripMargin) ++
      ( if (config.secondaryIndex)
        Seq(
          s"""CREATE INDEX IF NOT EXISTS color ON ${config.keyspace}.$tbName (color) """,
          s"""CREATE INDEX IF NOT EXISTS size ON ${config.keyspace}.$tbName (size)""")
      else Seq.empty )

  def getRDD: RDD[PerfRowClass] =
    RowGenerator.getPerfRowRdd(sc, config.numPartitions, config.totalOps, config.numTotalKeys)

}

/**
 * Runs inserts to partitions in a round robin fashion. This means partition key 1 will not
 * be written to twice until partition key n is written to once.
 */
class WriteWideRow(config: Config, sc: SparkContext) extends
  WriteTask[WideRowClass](config, sc)(implicitly[RowWriterFactory[WideRowClass]]) {

  def getTableCql(tbName: String): Seq[String] =
    Seq(s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key bigint, col1 text, col2 text, col3 text,
       |PRIMARY KEY (key, col1))
     """.stripMargin)

  def getRDD: RDD[WideRowClass] = {
    println(
      s"""Generating RDD for wide rows with round robin partitions:
         |${config.totalOps} Total Writes,
         |${config.numTotalKeys} Cassandra Partitions""".stripMargin
    )
    RowGenerator
      .getWideRowRdd(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
  }

}

/**
 * Runs inserts to partitions in a random fashion. The chance that any particular partition
 * key will be written to at a time is 1/N. (flat distribution)
 */
class WriteRandomWideRow(config: Config, sc: SparkContext) extends
  WriteTask[WideRowClass](config, sc)(implicitly[RowWriterFactory[WideRowClass]]) {

  def getTableCql(tbName: String): Seq[String] =
    Seq(s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key bigint, col1 text, col2 text, col3 text,
       |PRIMARY KEY (key, col1))
     """.stripMargin)

  def getRDD: RDD[WideRowClass] = {
    println(
      s"""Generating RDD for random wide rows:
         |${config.totalOps} Total Writes,
         |${config.numTotalKeys} Cassandra Partitions""".stripMargin
    )
    RowGenerator.getRandomWideRow(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
  }

}

/**
 *  Runs inserts to partitions in an ordered fashion. All writes to partition 1 occur before any
 *  writes to partition 2.
 */
class WriteWideRowByPartition(config: Config, sc: SparkContext) extends
  WriteTask[WideRowClass](config, sc)(implicitly[RowWriterFactory[WideRowClass]]) {

  def getTableCql(tbName: String): Seq[String] =
    Seq(s"""CREATE TABLE IF NOT EXISTS $tbName
       |(key int, col1 text, col2 text, col3 text,
       |PRIMARY KEY (key, col1))
     """.stripMargin)

  def getRDD: RDD[WideRowClass] = {
    println(
      s"""Generating RDD for wide rows in ordered by partition:
         |${config.totalOps} Total Writes,
         |${config.numTotalKeys} Cassandra Partitions""".stripMargin
    )
    RowGenerator.getWideRowByPartition(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
  }



}
