package com.datastax.sparkstress

import org.apache.spark.{SparkConf, SparkContext}

case class Config(testName: String ="writeshortrow",
master: String = "spark://127.0.0.1:7077",
numPartitions: Int = 400,
totalOps: Long = 20 * 1000000,
numTotalKeys: Long =  1 * 1000000,
trials: Int = 1,
deleteKeyspace: Boolean = false,
maxParallelWrites: Int = 5,
writeBatchSize: Int = 64*1024,
inputFetchSize: Int = 1000,
inputSplitSize: Int = 100000,
keyspace: String = "ks",
table: String = "tab",
writeRowSize: String = "auto"
)



object TestRunner {
  val VALID_TESTS = List("writeshortrow", "writewiderow", "writerandomwiderow", "writeperfrow", "readall")
  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[Config]("sparkstress") {
     head("SparkStress", "0.1")
     arg[String]("testName") optional() action { (arg,config) => config.copy(testName = arg.toLowerCase())} text {"Name of the test to be run: "+VALID_TESTS.mkString(" , ")}
     arg[String]("master") optional() action { (arg,config) => config.copy(master = arg)} text {"Spark Address of Master Node"}
     opt[Long]('n',"totalOps") optional() action { (arg,config) => config.copy(totalOps = arg)} text {"Total number of operations to execute"}
     opt[Int]('x',"numPartitons") optional() action {(arg,config) => config.copy(numPartitions = arg)} text {"Number of Spark Partitions To Create"}
     opt[Long]('k',"numTotalKeys") optional() action {(arg,config) => config.copy(numTotalKeys = arg)} text {"Total Number of CQL Partition Key Values"}
     opt[Int]('t',"trials")optional() action {(arg,config) => config.copy(trials = arg)} text {"Trials to run"}
     opt[Unit]('d',"deleteKeyspace") optional() action { (_,config) => config.copy(deleteKeyspace = true) } text {"Delete Keyspace before running"}
     opt[Int]('p',"maxParaWrites") optional() action { (arg,config) => config.copy(maxParallelWrites = arg) } text {"Write Paralellism"}
     opt[Int]('b',"batchSize") optional() action { (arg,config) => config.copy(writeBatchSize = arg) } text {"Write Batch Size in bytes"}
     opt[String]('r',"rowSize") optional() action { (arg,config) => config.copy(writeRowSize = arg) } text {"This setting will override batch size in bytes and instead just do a static number of rows per batch"}
     opt[Int]('f',"fetchSize") optional() action { (arg,config) => config.copy(inputFetchSize = arg) } text {"Read fetch size"}
     opt[Int]('s',"splitSize") optional() action { (arg,config) => config.copy(inputSplitSize = arg) } text {"Read input size"}
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
    val ipReg = """\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}""".r
    val cassandraIp =  ipReg findFirstIn (config.master) match {
      case Some(ipReg) => ipReg
      case None => "127.0.0.1"
    }

    val sparkconf = new SparkConf()
                          .setMaster(config.master)
                          .setAppName("SparkStress: "+config.testName)
                          .setSparkHome(System.getenv("SPARK_HOME"))
                          .setJars(Seq(System.getProperty("user.dir") + "/target/spark-stress.jar"))
                          .set("spark.cassandra.output.batch.size.bytes", config.writeBatchSize.toString)
                          .set("spark.cassandra.output.concurrent.writes",config.maxParallelWrites.toString)
                          .set("spark.cassandra.input.page.row.size", config.inputFetchSize.toString)
                          .set("spark.cassandra.input.split.size", config.inputSplitSize.toString)
                          .set("spark.cassandra.output.batch.size.rows", config.writeRowSize)
                          .set("spark.cassandra.connection.host", cassandraIp)


    val sc = new SparkContext(sparkconf)

    val test: StressTask =
      config.testName.toLowerCase match {
        case "writeshortrow" => new WriteShortRow()
        case "writewiderow" => new WriteWideRow()
        case "writeperfrow" => new WritePerfRow()
        case "writerandomwiderow" => new WriteRandomWideRow()
        case "readall" => new ReadAll()
    }
    test.setConfig(config)
    val time = test.runTrials(sc)
    sc.stop()
    val timeSeconds = time.map{ _ / 1000000000.0}
    val opsPerSecond = timeSeconds.map{ config.totalOps/_}
    printf("\n\nTrials Ran %s\n",timeSeconds.mkString(","))
    printf("\n\nOpsPerSecond %s\n",opsPerSecond.mkString(","))
 }



}
