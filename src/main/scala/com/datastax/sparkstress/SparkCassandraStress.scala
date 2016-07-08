package com.datastax.sparkstress

import java.io.{FileWriter, Writer}

import org.apache.spark.SparkConf


case class Config(
  //Test Options
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
  //Spark Options
  sparkOps: Map[String,String] = Map.empty,
  //Streaming Params
  numReceivers: Int = 1,
  receiverThroughputPerBatch: Long = 100000,
  streamingBatchIntervalSeconds:  Int = 5
)


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

      opt[String]('S', "save") optional() action { (arg,config) =>
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

      opt[String]('m',"saveMethod") optional() action { (arg,config) =>
        config.copy(saveMethod = arg)
      } text {"rdd save method. bulk: bulkSaveToCassandra, driver: saveToCassandra"}

      opt[Int]('n',"trials")optional() action { (arg,config) =>
        config.copy(trials = arg)
      } text {"Trials to run"}

      opt[Long]('o',"totalOps") optional() action { (arg,config) =>
        config.copy(totalOps = arg)
      } text {"Total number of operations to execute"}

      opt[Int]('p',"numPartitions") optional() action { (arg,config) =>
        config.copy(numPartitions = arg)
      } text {"Number of Spark Partitions To Create"}

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
      
      arg[String]("connectorOpts") optional() text {"spark-cassandra-connector configs, Ex: --conf \"conf1=val1\" --conf \"conf2=val2\""}
      
      
      help("help") text {"CLI Help"}
      checkConfig{ c => if (VALID_TESTS.contains(c.testName)) success else failure(
        s"""${c.testName} is not a valid test :
           |Write Tests:  ${WriteTask.ValidTasks.mkString(" , ")}
           |Read Tests: ${ReadTask.ValidTasks.mkString(" , ")}""".stripMargin)}
    }

    parser.parse(args, Config()) map { config =>
      runTask(config)
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
        .setAll(config.sparkOps)

    val sc = ConnectHelper.getContext(sparkConf)
    
    if (config.verboseOutput) { 
      println("\nDumping debugging output")
      println(sc.getConf.toDebugString+"\n")
    }

    val test: StressTask =
      config.testName.toLowerCase match {
          /** Write Tasks **/
        case "writeshortrow" => new WriteShortRow(config, sc)
        case "writewiderow" => new WriteWideRow(config, sc)
        case "writeperfrow" => new WritePerfRow(config, sc)
        case "writerandomwiderow" => new WriteRandomWideRow(config, sc)
        case "writewiderowbypartition" => new WriteWideRowByPartition(config, sc)

          /** Read Tasks **/
        case "pdcount" => new PDCount(config, sc)
        case "ftsallcolumns" => new FTSAllColumns(config, sc)
        case "ftsfivecolumns" => new FTSFiveColumns(config, sc)
        case "ftsonecolumn" => new FTSOneColumn(config, sc)
        case "ftspdclusteringallcolumns" => new FTSPDClusteringAllColumns(config, sc)
        case "ftspdclusteringfivecolumns" => new FTSPDClusteringFiveColumns(config, sc)
        case "jwcallcolumns" => new JWCAllColumns(config, sc)
        case "jwcpdclusteringallcolumns" => new JWCPDClusteringAllColumns(config, sc)
        case "jwcrpallcolumns" => new JWCRPAllColumns(config, sc)
        case "retrievesinglepartition" => new RetrieveSinglePartition(config, sc)

          /** Streaming Tasks **/
        case "streamingwrite" => new StreamingWrite(config, sc)
      }

    val wallClockStartTime = System.nanoTime()
    val time = test.runTrials(sc)

    val wallClockStopTime = System.nanoTime() 
    val wallClockTimeDiff = wallClockStopTime - wallClockStartTime
    val wallClockTimeSeconds = (wallClockTimeDiff / 1000000000.0) 
    val timeSeconds = time.map{ _ / 1000000000.0}
    val opsPerSecond = timeSeconds.map{ config.totalOps/_}
    test match {
      case x: WriteTask[_] =>  {
        println(s"TimeInSeconds : ${(timeSeconds.map{math.round(_)}).mkString(",")}\n")
        println(s"OpsPerSecond : ${(opsPerSecond.map{math.round(_)}).mkString(",")}\n")
        config.file.map(f => {f.write(csvResults(config, time));f.flush })
        sc.stop()
      }
      case x: ReadTask => {
        println(s"TimeInSeconds : ${(timeSeconds.map{math.round(_)}).mkString(",")}\n")
        config.file.map(f => {f.write(csvResults(config, time));f.flush })
        sc.stop()
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
