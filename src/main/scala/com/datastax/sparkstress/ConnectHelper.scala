package com.datastax.sparkstress

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ConnectHelper {
  def getSparkSession(conf: SparkConf = DefaultConfig): SparkSession = {
    SparkSession.builder().config(conf).getOrCreate()
  }

  val DefaultConfig = new SparkConf()
    .setAppName("Spark Stress Test")
    .setMaster("local[*]")
    .set("spark.hadoop.fs.dsefs.impl", "com.datastax.bdp.fs.hadoop.DseFileSystem") // without we get: 'java.io.IOException: No FileSystem for scheme: dsefs'

}
