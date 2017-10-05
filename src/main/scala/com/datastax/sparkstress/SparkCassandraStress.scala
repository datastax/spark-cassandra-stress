package com.datastax.sparkstress

import java.io.{FileWriter, Writer}
import java.lang.Math.round
import org.apache.spark.SparkConf


case class Config(
  //Test Options
  seed: Long = System.currentTimeMillis(),
  testName: String ="writeshortrow",
  keyspace: String = "ks",
  table: String = "tab",
  trials: Int = 1,
  verboseOutput: Boolean = false,
  //Write Options
  replicationFactor: Int = 1,
  numPartitions: Int = 400,
  totalOps: Long = 20 * 1000000,
  numTotalKeys: Long =  1 * 1000000,
  deleteKeyspace: Boolean = false,
  secondaryIndex: Boolean = false,
  // csv file to append results
  file: Option[Writer] = Option.empty,
  saveMethod: String = "driver",
  distributedDataType: String = "rdd",
  //Spark Options
  sparkOps: Map[String,String] = Map.empty,
  //Streaming Params
  numReceivers: Int = 1,
  receiverThroughputPerBatch: Long = 100000,
  terminationTimeMinutes: Long = 0,
  streamingBatchIntervalSeconds:  Int = 5
)

case class TestResult ( time: Long, ops: Long )

object SparkCassandraStress {
  val VALID_TESTS =
    WriteTask.ValidTasks ++ ReadTask.ValidTasks ++ StreamingTask.ValidTasks

  val KeyGroupings = Seq("none", "replica_set", "partition")

  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[Config]("SparkCassandraStress") {
      head("SparkCassandraStress", "1.0")

      arg[String]("testName") optional() action { (arg,config) =>
        config.copy(testName = arg.toLowerCase())
      } text {  s"""Tests :
              |Write Tests:  ${WriteTask.ValidTasks.mkString(" , ")}
              |Read Tests: ${ReadTask.ValidTasks.mkString(" , ")}
              |Streaming Tests: ${StreamingTask.ValidTasks.mkString(" , ")}""".stripMargin}
      arg[String]("master") optional() action { (arg,config) =>
        config.copy(sparkOps = config.sparkOps + ("spark.master" -> arg))
      } text {"Spark Address of Master Node"}

      opt[String]('f', "file") optional() action { (arg,config) =>
        config.copy(file = Option(new FileWriter(arg, true)))
      } text {"Name of the file to append results"}

      opt[Unit]('d',"deleteKeyspace") optional() action { (_,config) =>
        config.copy(deleteKeyspace = true)
      } text {"Delete Keyspace before running"}

      opt[Unit]('i',"secondaryIndex") optional() action { (_,config) =>
        config.copy(secondaryIndex = true)
      } text {"Adds Secondary Indexes to PerfRow DDL"}

      opt[String]('k',"keyspace") optional() action { (arg,config) =>
        config.copy(keyspace = arg)
      } text {"Name of the keyspace to use/create"}

      opt[String]('e',"distributedDataType") optional() action { (arg,config) =>
        config.copy(distributedDataType = arg)
      } text {
        """See 'saveMethod'.
          |                           rdd: Resilient Distributed Dataset, the basic abstraction in Spark.
          |                           dataset: A DataSet is a strongly typed collection of domain-specific objects.""".stripMargin}

      opt[String]('s',"saveMethod") optional() action { (arg,config) =>
        config.copy(saveMethod = arg)
      } text {
        """See 'distributedDataType'.
          |                           rdd save methods:
          |                           bulk: bulkSaveToCassandra, driver: saveToCassandra
          |                           dataset save methods:
          |                             driver: ds.write.cassandraFormat(..).mode(..).save()
          |                             parquet: format to save in dsefs
          |                             text: format to save in dsefs
          |                             json: format to save in dsefs
          |                             csv: format to save in dsefs""".stripMargin}

      opt[Int]('n',"trials")optional() action { (arg,config) =>
        config.copy(trials = arg)
      } text {"Trials to run"}

      opt[Long]('o',"totalOps") optional() action { (arg,config) =>
        config.copy(totalOps = arg)
      } text {"Total number of operations to execute"}

      opt[Int]('p',"numPartitions") optional() action { (arg,config) =>
        config.copy(numPartitions = arg)
      } text {"Number of Spark Partitions To Create"}

      opt[Long]('c',"seed") optional() action { (arg,config) =>
        config.copy(seed = arg)
      } text {"Seed used for randomly generating cell data, reuse a previous seed for 100% repeatability between runs. Min: -9223372036854775808, Max: 9223372036854775807"}

      opt[Int]('r',"replication") optional() action { (arg,config) =>
        config.copy(replicationFactor = arg)
      } text {"Replication Factor to set on new keyspace, will not change existing keyspaces"}

      opt[String]('t', "table") optional() action { (arg,config) =>
        config.copy(table = arg)
      } text {"Name of the table to use/create"}

      opt[Unit]('v',"verbose") optional() action { (_,config) =>
        config.copy(verboseOutput = true)
      } text {"Display verbose output for debugging."}

      opt[Long]('y',"numTotalKeys") optional() action { (arg,config) =>
        config.copy(numTotalKeys = arg)
      } text {"Total Number of CQL Partition Key Values"}

      opt[Int]('w',"numReceivers") optional() action { (arg,config) =>
        config.copy(numReceivers = arg)
      } text {"Changes the number of receivers to make in Streaming Tests"}

      opt[Long]('x',"receiverThroughputPerBatch") optional() action { (arg,config) =>
        config.copy(receiverThroughputPerBatch = arg)
      } text {"Changes the number of rows to emit per receiver per batch timing"}

      opt[Int]('z',"streamingBatchLength") optional() action { (arg,config) =>
        config.copy(streamingBatchIntervalSeconds = arg)
      } text {"Batch interval in seconds used for defining a StreamingContext."}

      opt[Int]('m',"terminationTimeMinutes") optional() action { (arg,config) =>
        config.copy(terminationTimeMinutes = arg)
      } text { "The desired runtime (in minutes) for a given workload. WARNING: Not supported with multiple trials or read workloads."}

      arg[String]("connectorOpts") optional() text { """spark-cassandra-connector configs, Ex: --conf "conf1=val1" --conf "conf2=val2" """}

      help("help") text {"CLI Help"}
      checkConfig{ c => if (VALID_TESTS.contains(c.testName)) success else failure(
        s"""${c.testName} is not a valid test :
           |Write Tests:  ${WriteTask.ValidTasks.mkString(" , ")}
           |Read Tests: ${ReadTask.ValidTasks.mkString(" , ")}""".stripMargin)}
    }

    parser.parse(args, Config()) map { config =>
      if (config.trials > 1 && config.terminationTimeMinutes > 0) {
        println("\nERROR: A termination time was specified with multiple trials, this is not supported yet.\n")
      } else if (ReadTask.ValidTasks(config.testName) && config.terminationTimeMinutes > 0) {
        println(s"\nERROR: A termination time was specified with '${config.testName} which is a Read test, this is not supported yet.\n")
      } else {
        runTask(config)
      }
    } getOrElse {
      System.exit(1)
    }
  }

  def csvResults(config: Config, time: Seq[Long]) : String = {
    time.zipWithIndex.map {case (time,count) => {
      val timeSeconds :Double = time / 1000000000.0
      val opsPerSecond = config.totalOps/ timeSeconds
      Seq(config.testName, config.saveMethod, config.totalOps, config.totalOps/config.numTotalKeys, count, timeSeconds, opsPerSecond, config).mkString("\t")
    }}.mkString("\n") + "\n"
  }

  def runTask(config:Config)
  {
    val sparkConf =
      new SparkConf()
        .setAppName("SparkStress_"+config.testName)
        //Make sure for streaming that the keep_alive is sufficently large
        .set("spark.cassandra.connection.keep_alive_ms", (config.streamingBatchIntervalSeconds*1000*5).toString)
        .set("spark.cassandra.input.metrics", "true")
        .set("spark.cassandra.output.metrics", "true")
        .setAll(config.sparkOps)

    val ss = ConnectHelper.getSparkSession(sparkConf)


    if (config.verboseOutput) { 
      println("\nDumping debugging output")
      println(ss.sparkContext.getConf.toDebugString+"\n")
    }

    val test: StressTask =
      config.testName.toLowerCase match {
          /** Write Tasks **/
        case "writeshortrow" => new WriteShortRow(config, ss)
        case "writewiderow" => new WriteWideRow(config, ss)
        case "writeperfrow" => new WritePerfRow(config, ss)
        case "writerandomwiderow" => new WriteRandomWideRow(config, ss)
        case "writewiderowbypartition" => new WriteWideRowByPartition(config, ss)

          /** RDD Read Tasks **/
        case "pdcount" => new PDCount(config, ss)
        case "ftsallcolumns" => new FTSAllColumns(config, ss)
        case "ftsfivecolumns" => new FTSFiveColumns(config, ss)
        case "ftsonecolumn" => new FTSOneColumn(config, ss)
        case "ftspdclusteringallcolumns" => new FTSPDClusteringAllColumns(config, ss)
        case "ftspdclusteringfivecolumns" => new FTSPDClusteringFiveColumns(config, ss)
        case "jwcallcolumns" => new JWCAllColumns(config, ss)
        case "jwcpdclusteringallcolumns" => new JWCPDClusteringAllColumns(config, ss)
        case "jwcrpallcolumns" => new JWCRPAllColumns(config, ss)
        case "retrievesinglepartition" => new RetrieveSinglePartition(config, ss)

          /** Dataset Read Tasks **/
        case "ftsallcolumns_ds" => new FTSAllColumns_DS(config, ss)
        case "ftsfivecolumns_ds" => new FTSFiveColumns_DS(config, ss)
        case "ftsfourcolumns_ds" => new FTSFourColumns_DS(config, ss)
        case "ftsthreecolumns_ds" => new FTSThreeColumns_DS(config, ss)
        case "ftstwocolumns_ds" => new FTSTwoColumns_DS(config, ss)
        case "ftsonecolumn_ds" => new FTSOneColumn_DS(config, ss)

        case "dsefsreadparquet_ds" => new DSEFSReadParquet_DS(config, ss)
        case "dsefsreadtext_ds" => new DSEFSReadText_DS(config, ss)
        case "dsefsreadjson_ds" => new DSEFSReadJson_DS(config, ss)
        case "dsefsreadcsv_ds" => new DSEFSReadCsv_DS(config, ss)

        /** Streaming Tasks **/
        case "streamingwrite" => new StreamingWrite(config, ss)
      }

    val wallClockStartTime = System.nanoTime()

    val timesAndOps: Seq[TestResult]= test.runTrials(ss)
    val time = for (x <- timesAndOps) yield {x.time}
    val totalCompletedOps = for (x <- timesAndOps) yield {x.ops}

    val wallClockStopTime = System.nanoTime() 
    val wallClockTimeDiff = wallClockStopTime - wallClockStartTime
    val wallClockTimeSeconds = (wallClockTimeDiff / 1000000000.0) 
    val timeSeconds = time.map{ x => round( x / 1000000000.0 ) }
    val opsPerSecond = for (i <- 0 to timeSeconds.size-1) yield {round((totalCompletedOps(i)).toDouble/timeSeconds(i))}

    test match {
      case x: WriteTask[_] =>  {
        println(s"TimeInSeconds : ${timeSeconds.mkString(",")}\n")
        println(s"OpsPerSecond : ${opsPerSecond.mkString(",")}\n")
        config.file.map(f => {f.write(csvResults(config, time));f.flush })
        ss.stop()
      }
      case x: ReadTask => {
        println(s"TimeInSeconds : ${timeSeconds.mkString(",")}\n")
        config.file.map(f => {f.write(csvResults(config, time));f.flush })
        ss.stop()
      }
      case y: StreamingTask[_] => {
        println("Streaming Begun")
        println(s"Running for ${y.terminationTime} Seconds")
        Thread.sleep(y.terminationTime * 1000)
        println("Times up, shutting down")
        y.ssc.stop(true, true)
      }
    }
 }

}
