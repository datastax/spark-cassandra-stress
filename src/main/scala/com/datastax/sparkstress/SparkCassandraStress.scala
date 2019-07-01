package com.datastax.sparkstress

import java.io.{FileWriter, Writer}
import java.lang.Math.round
import org.apache.spark.SparkConf
import org.reflections.Reflections
import org.apache.spark.sql.{SparkSession, SaveMode}
import collection.JavaConversions._
import scala.math._

object DistributedDataType extends Enumeration {
  val RDD  = Value("rdd")
  val DataFrame = Value("dataframe")
}

object SaveMethod extends Enumeration {
  val Driver = Value("driver")
  val Bulk = Value("bulk")
  val Parquet = Value("parquet")
  val Json = Value("json")
  val Csv = Value("csv")
  val Text = Value("text")
}

object ValidNumericAnnotations extends Enumeration {
  val k = Value("k")
  val m = Value("m")
  val b = Value("b")
  val t = Value("t")
  val q = Value("q")
}

case class TableLocation(keyspace: String, table: String)

case class Config(
  //Test Options
  seed: Long = 4L, // lock in a default for better repeatability, lucky number 4
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
  @transient file: Option[Writer] = Option.empty,
  saveMethod: SaveMethod.Value = SaveMethod.Driver,
  dataframeSaveMode: SaveMode = SaveMode.Append,
  distributedDataType: DistributedDataType.Value = DistributedDataType.RDD,
  //Spark Options
  sparkOps: Map[String,String] = Map.empty,
  //Streaming Params
  numReceivers: Int = 1,
  receiverThroughputPerBatch: Long = 100000,
  terminationTimeMinutes: Long = 0,
  streamingBatchIntervalSeconds:  Int = 5,
  inClauseKeys: Int = 2000
)

case class TestResult ( time: Long, ops: Long )

object SparkCassandraStress {
  val reflections = new Reflections("com.datastax.sparkstress")
  val VALID_TESTS = getValidTestNames()

  val KeyGroupings = Seq("none", "replica_set", "partition")
  val supportedAnnotationsMsg = s"Ex. 1000, 1k (thousand), 2m (million), 3B (billion), 4q (quadrillion)"

  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[Config]("SparkCassandraStress") {
      head("SparkCassandraStress", "1.0")

      arg[String]("testName") optional() action { (arg,config) =>
        config.copy(testName = arg.toLowerCase)
      } text {  s"""Tests :
              |Write Tests:  ${getWriteTestNames.mkString(" , ")}
              |Read Tests: ${getReadTestNames.mkString(" , ")}
              |Streaming Tests: ${getStreamingTestNames.mkString(" , ")}""".stripMargin}
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
        config.copy(distributedDataType = DistributedDataType.withName(arg.toLowerCase))
      } text {
        """See 'saveMethod'. **Note**: Use for write workloads.
          |                           rdd: Resilient Distributed Dataset, the basic abstraction in Spark.
          |                           dataframe: A Dataframe is a catalyst backed optimized container.""".stripMargin}

      opt[String]('s',"saveMethod") optional() action { (arg,config) =>
        config.copy(saveMethod = SaveMethod.withName(arg.toLowerCase))
      } text {
        """See 'distributedDataType'. **Note**: Use for write workloads.
          |                           rdd save methods:
          |                             bulk: bulkSaveToCassandra
          |                             driver: saveToCassandra
          |                           **Note**: Folder for DSEFS writes will be named: keyspace.tablename (Default: ks.tab)
          |                           dataframe save methods:
          |                             driver: ds.write...save()
          |                             parquet: data format in DSEFS
          |                             text: data format in DSEFS
          |                             json: data format in DSEFS
          |                             csv: data format in DSEFS""".stripMargin}

      opt[String]('u',"dataframeSaveMode") optional() action { (arg,config) =>
        config.copy(dataframeSaveMode = SaveMode.valueOf(arg.toLowerCase.capitalize))
      } text {
        """See 'distributedDataType'. Specifies the behavior when data or table already exists. Options include:
          |                             overwrite: overwrite the existing data
          |                             append: (default) append the data
          |                             ignore: ignore the operation (i.e. no-op)
          |                             error: throw an exception at runtime""".stripMargin}

      opt[Int]('n',"trials")optional() action { (arg,config) =>
        config.copy(trials = arg)
      } text {"Trials to run"}

      opt[String]('o',"totalOps") optional() action { (arg,config) =>
        config.copy(totalOps = unpackAnnotatedNumeric(arg))
      } text {s"Total number of operations to execute. ${supportedAnnotationsMsg}"}

      opt[Int]('p',"numPartitions") optional() action { (arg,config) =>
        config.copy(numPartitions = arg)
      } text {s"Number of Spark Partitions To Create."}

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

      opt[String]('y',"numTotalKeys") optional() action { (arg,config) =>
        config.copy(numTotalKeys = unpackAnnotatedNumeric(arg))
      } text {s"Total Number of CQL Partition Key Values. ${supportedAnnotationsMsg}"}

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

      opt[Int]("inClauseKeys") optional() action { (arg,config) =>
        config.copy(inClauseKeys = arg)
      } text {s"Number of keys in 'IN' clause, applicable only for tests that execute select queries " +
        s"with 'IN' clause."}

      arg[String]("connectorOpts") optional() text { """spark-cassandra-connector configs, Ex: --conf "conf1=val1" --conf "conf2=val2" """}

      help("help") text {"CLI Help"}
      checkConfig{ c => if (VALID_TESTS.contains(c.testName)) success else failure(
        s"""${c.testName} is not a valid test :
           |Streaming Tests:  ${getStreamingTestNames.mkString(" , ")}
           |Write Tests:  ${getWriteTestNames.mkString(" , ")}
           |Read Tests: ${getReadTestNames.mkString(" , ")}""".stripMargin)}
    }

    parser.parse(args, Config()) map { config =>
      if (config.trials > 1 && config.terminationTimeMinutes > 0) {
        println("\nERROR: A termination time was specified with multiple trials, this is not supported yet.\n")
      } else if (getReadTestNames.contains(config.testName) && config.terminationTimeMinutes > 0) {
        println(s"\nERROR: A termination time was specified with '${config.testName} which is a Read test, this is not supported yet.\n")
      }else {
        runTask(config)
      }
    } getOrElse {
      System.exit(1)
    }
  }

  def unpackAnnotatedNumeric(num: String): Long = {

    if (num forall Character.isDigit) {
      num.toLong
    } else {
      val numericPortion = num.init
      assert(numericPortion forall Character.isDigit)

      val numericAnnotation = num.toLowerCase.last
      assert(Character.isLetter(numericAnnotation))

      try {
        ValidNumericAnnotations.withName(numericAnnotation.toString) match {
          case ValidNumericAnnotations.k => numericPortion.toLong * pow(10, 3).toLong // thousand
          case ValidNumericAnnotations.m => numericPortion.toLong * pow(10, 6).toLong // million
          case ValidNumericAnnotations.b => numericPortion.toLong * pow(10, 9).toLong // billion
          case ValidNumericAnnotations.t => numericPortion.toLong * pow(10, 12).toLong // trillion
          case ValidNumericAnnotations.q => numericPortion.toLong * pow(10, 15).toLong // quadrillion
        }
      } catch {
        case ex: NoSuchElementException => throw new UnsupportedOperationException(s"${ex}: ${supportedAnnotationsMsg}")
      }
    }
  }

  def getReadTests() = {
    reflections.getTypesAnnotatedWith(classOf[ReadTest]).toSet
  }

  def getWriteTests() = {
    reflections.getTypesAnnotatedWith(classOf[WriteTest]).toSet
  }

  def getStreamingTests() = {
    reflections.getTypesAnnotatedWith(classOf[StreamingTest]).toSet
  }

  def getValidTests() = {
    Set.empty ++ getReadTests() ++ getWriteTests() ++ getStreamingTests()
  }

  def getReadTestNames(): Set[String] = {
    getReadTests.map(_.getSimpleName.toLowerCase)
  }

  def getWriteTestNames(): Set[String] = {
    getWriteTests.map(_.getSimpleName.toLowerCase)
  }

  def getStreamingTestNames(): Set[String] = {
    getStreamingTests.map(_.getSimpleName.toLowerCase)
  }

  def getValidTestNames(): Set[String] = {
    getReadTestNames() ++ getWriteTestNames() ++ getStreamingTestNames()
  }

  def getStressTest(config: Config, ss: SparkSession) : StressTask = {
    val subClasses = getValidTests.toList
    val classMap = subClasses.map(_.getSimpleName.toLowerCase).zip(subClasses).toMap
    classMap(config.testName)
      .getConstructors
      .maxBy(_.getParameterTypes.length)
      .newInstance(config, ss)
      .asInstanceOf[StressTask]
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

    val test: StressTask = getStressTest(config, ss)
    val timesAndOps: Seq[TestResult]= test.runTrials(ss)
    val time = for (x <- timesAndOps) yield {x.time}
    val totalCompletedOps = for (x <- timesAndOps) yield {x.ops}

    val timeSeconds = time.map{ x => round( x / 1000000000.0 ) }
    val timeMillis = time.map{ x => round( x / 1000000.0 ) }
    val opsPerSecond = for (i <- timeSeconds.indices) yield {round(totalCompletedOps(i).toDouble/timeSeconds(i))}

    test match {
      case _: WriteTask[_] =>  {
        println(s"TimeInSeconds : ${timeSeconds.mkString(",")}\n")
        println(s"OpsPerSecond : ${opsPerSecond.mkString(",")}\n")
        config.file.map(f => {f.write(csvResults(config, time));f.flush })
        ss.stop()
      }
      case _: ReadTask => {
        println(s"TimeInSeconds : ${timeSeconds.mkString(",")}\n")
        println(s"TimeInMillis : ${timeMillis.mkString(",")}\n")
        println(s"Average [ms]: ${timeMillis.sum.toDouble / timeSeconds.size}\n")
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
