package com.datastax.sparkstress

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.writer.RowWriterFactory
import com.datastax.sparkstress.RowGenerator.PerfRowGenerator
import com.datastax.sparkstress.RowTypes._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, _}
import org.apache.spark.streaming.dstream.DStream
import java.util.concurrent.TimeUnit
import scala.reflect.ClassTag

object StreamingTask {
  val ValidTasks = Set(
    "streamingwrite"
  )
}

abstract class StreamingTask[rowType](
  val config: Config,
  val ss: SparkSession)
(implicit ct:ClassTag[rowType]) extends StressTask {

  val ssc = new StreamingContext(ss.sparkContext, Seconds(config.streamingBatchIntervalSeconds))
  val opsPerBatch = (config.numReceivers * config.receiverThroughputPerBatch)
  val estimatedReqRuntime: Long = ((config.totalOps / opsPerBatch) * config.streamingBatchIntervalSeconds) + 10
  val terminationTime: Long = {
    if (config.terminationTimeMinutes == 0) {
      estimatedReqRuntime
    } else {
      val newTerminationTime: Long = TimeUnit.MINUTES.toSeconds(config.terminationTimeMinutes)
      if (estimatedReqRuntime <= newTerminationTime) {
        println(s"Using the estimated runtime (${estimatedReqRuntime} secs}) required to stream ${config.totalOps} since it is <= the requested runtime (${newTerminationTime} secs).")
        estimatedReqRuntime
      } else {
        println(s"Converting requested runtime of ${config.terminationTimeMinutes} min to ${newTerminationTime} secs.")
        newTerminationTime
      }
    }
  }

  def setupCQL() = {
    CassandraConnector(ss.sparkContext.getConf).withSessionDo { session =>
      if (config.deleteKeyspace) {
        println(s"Destroying Keyspace")
        session.execute(s"DROP KEYSPACE IF EXISTS ${config.keyspace}")
      }
      val kscql = getKeyspaceCql(config.keyspace)
      val tbcql = getTableCql(config.table)
      println( s"""Running the following create statements\n$kscql\n${tbcql.mkString("\n")}""")
      session.execute(kscql)
      session.execute(s"USE ${config.keyspace}")
      for (cql <- tbcql)
        session.execute(cql)
    }
    printf("Done Setting up CQL Keyspace/Table\n")
  }

  def getKeyspaceCql(ksName: String): String = s"CREATE KEYSPACE IF NOT EXISTS $ksName WITH replication = {'class': 'NetworkTopologyStrategy', 'Analytics': ${config.replicationFactor} }"

  def getTableCql(tbName: String): Seq[String]

  /** Should generate unique data based on the int passed **/
  def getGenerator: RowGenerator[rowType]

  def run() = {
    val dstreams = for (i <- 1 to config.numReceivers) yield {
      println(s"Making Custom Stream $i")
      ssc.receiverStream(
        new StressReceiver[rowType](
          i,
          getGenerator,
          config,
          ss.sparkContext.getConf.getInt("spark.streaming.blockInterval", 200),
          StorageLevel.MEMORY_AND_DISK_2)
      )
    }
    dstreamOps(ssc.union(dstreams))
    ssc.start()
  }

  def dstreamOps(dstream: DStream[rowType]): Unit

  def runTrials(ss: SparkSession): Seq[TestResult] = {
    println("About to Start Trials")
    for (trial <- 1 to config.trials) yield {setupCQL(); Thread.sleep(10000); TestResult(time(run()),0L)}
  }
}

class StreamingWrite(config: Config, ss: SparkSession) extends
  StreamingTask[PerfRowClass](config,ss)(implicitly[ClassTag[PerfRowClass]]){

  /** Use the write task for gettingTableCql : TODO make this more elegent, part of the Generator?**/
  @transient val _writeTask = new WritePerfRow(config, ss)
  val generator = new PerfRowGenerator(config.numReceivers, config.totalOps, config.numTotalKeys)

  override def getTableCql(tbName: String): Seq[String] = _writeTask.getTableCql(tbName)

  /** Should generate unique data based on the int passed **/
  override def getGenerator: RowGenerator[PerfRowClass] = generator

  override def dstreamOps(dstream: DStream[PerfRowClass]): Unit = dstream.saveToCassandra(config.keyspace, config.table)
}

