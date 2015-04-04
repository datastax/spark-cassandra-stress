package com.datastax.sparkstress

import org.apache.spark.{SparkConf, SparkContext}


case class Config(
  //Test Options
  testName: String ="writeshortrow",
  keyspace: String = "ks",
  table: String = "tab",
  replicationFactor: Int = 1,
  numPartitions: Int = 400,
  totalOps: Long = 20 * 1000000,
  numTotalKeys: Long =  1 * 1000000,
  trials: Int = 1,
  deleteKeyspace: Boolean = false,
  //Spark Options
  sparkOps: Map[String,String] = Map.empty
)


object SparkCassandraStress {
  val VALID_TESTS =
    WriteTask.ValidTasks ++
    Set("readall")

  val KeyGroupings = Seq("none", "replica_set", "partition")

  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[Config]("SparkCassandraStress") {
      head("SparkCassandraStress", "1.0")

      arg[String]("testName") optional() action { (arg,config) =>
        config.copy(testName = arg.toLowerCase())
      } text {"Name of the test to be run: "+VALID_TESTS.mkString(" , ")}

      arg[String]("master") optional() action { (arg,config) =>
        config.copy(sparkOps = config.sparkOps + ("spark.master" -> arg))
      } text {"Spark Address of Master Node"}

      arg [String]("cassandra") optional() action { (arg, config) =>
        config.copy(sparkOps = config.sparkOps + ("spark.cassandra.connection.host" -> arg))
      } text {"Ip Address to Connect To Cassandra On"}

      opt[Long]('o',"totalOps") optional() action { (arg,config) =>
        config.copy(totalOps = arg)
      } text {"Total number of operations to execute"}

      opt[Int]('p',"numPartitions") optional() action { (arg,config) =>
        config.copy(numPartitions = arg)
      } text {"Number of Spark Partitions To Create"}

      opt[Long]('y',"numTotalKeys") optional() action { (arg,config) =>
        config.copy(numTotalKeys = arg)
      } text {"Total Number of CQL Partition Key Values"}

      opt[Int]('n',"trials")optional() action { (arg,config) =>
        config.copy(trials = arg)
      } text {"Trials to run"}

      opt[Unit]('d',"deleteKeyspace") optional() action { (_,config) =>
        config.copy(deleteKeyspace = true)
      } text {"Delete Keyspace before running"}

      opt[String]('k',"keyspace") optional() action { (arg,config) =>
        config.copy(keyspace = arg)
      } text {"Name of the keyspace to use/create"}

      opt[Int]('r',"replication") optional() action { (arg,config) =>
        config.copy(replicationFactor = arg)
      } text {"Replication Factor to set on new keyspace, will not change existing keyspaces"}

      opt[String]('t', "table") optional() action { (arg,config) =>
        config.copy(table = arg)
      } text {"Name of the table to use/create"}

      opt[Int]('c',"maxConcurrentWrites") optional() action { (arg,config) =>
        config.copy(sparkOps = config.sparkOps +
          ("spark.cassandra.output.concurrent.writes" -> arg.toString))
      } text {"Connector Write Paralellism"}

      opt[Int]('b',"batchSize") optional() action { (arg,config) =>
        config.copy(sparkOps = config.sparkOps +
          ("spark.cassandra.output.batch.size.bytes" -> arg.toString))
      } text {"Write Batch Size in bytes"}

      opt[Int]('w',"rowSize") optional() action { (arg,config) =>
        config.copy(sparkOps = config.sparkOps +
          ("spark.cassandra.output.batch.size.rows" -> arg.toString))
      } text {"This setting will override batch size in bytes and instead just do a static number of rows per batch"}

      opt[Int]('f',"fetchSize") optional() action { (arg,config) =>
        config.copy(sparkOps = config.sparkOps +
          ("spark.cassandra.input.page.row.size" -> arg.toString))
      } text {"Read fetch size"}

      opt[Int]('s',"splitSize") optional() action { (arg,config) =>
        config.copy(sparkOps = config.sparkOps +
          ("spark.cassandra.input.split.size" -> arg.toString))
      } text {"Read input size"}

      opt[String]('g', "groupingKey") optional() action { (arg, config) =>
        config.copy(sparkOps = config.sparkOps +
          ("spark.cassandra.output.batch.grouping.key" -> arg.toString))
      } validate { (arg) =>
        if (KeyGroupings.contains(arg))
          success
        else
          failure(s"groupingKey ($arg) must be be part of ${KeyGroupings.mkString(" ")}")
      } text {s"The method by which the Spark Connector groups rows into partitions. Options: [ ${KeyGroupings.mkString(" ")} ]"}
      opt[Int]('q', "batchBufferSize") optional() action { (arg, config) =>
        config.copy(sparkOps = config.sparkOps +
          ("spark.cassandra.output.batch.grouping.buffer.size" -> arg.toString))
      } text {"The amount of batches the connector keeps alive before forcing the largest to be executed"}

      help("help") text {"CLI Help"}
     checkConfig{ c => if (VALID_TESTS.contains(c.testName)) success else failure(c.testName+" is not a valid test : "+VALID_TESTS.mkString(" , ")) }
    }

    parser.parse(args, Config()) map { config =>
      runTask(config)
    } getOrElse {
      System.exit(1)
    }
  }

  def runTask(config:Config)
  {

    val sparkConf =
      new SparkConf()
        .setAppName("SparkStress: "+config.testName)
        .setAll(config.sparkOps)

    val sc = ConnectHelper.getContext(sparkConf)

    val test: StressTask =
      config.testName.toLowerCase match {
        case "writeshortrow" => new WriteShortRow(config, sc)
        case "writewiderow" => new WriteWideRow(config, sc)
        case "writeperfrow" => new WritePerfRow(config, sc)
        case "writerandomwiderow" => new WriteRandomWideRow(config, sc)
        case "writewiderowbypartition" => new WriteWideRowByPartition(config, sc)
        case "readall" => new ReadAll()
      }

    test.setConfig(config)

    val time = test.runTrials(sc)
    sc.stop()
    val timeSeconds = time.map{ _ / 1000000000.0}
    val opsPerSecond = timeSeconds.map{ config.totalOps/_}
    printf(s"\n\nTimeInSeconds : %s\n",timeSeconds.mkString(","))
    printf(s"OpsPerSecond : %s\n",opsPerSecond.mkString(","))
 }



}
