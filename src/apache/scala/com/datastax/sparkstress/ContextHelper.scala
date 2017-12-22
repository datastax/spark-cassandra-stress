package com.datastax.sparkstress

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

object ConnectHelper {

  /*
  Get a normal SparkContext Object
   */
  def getContext(conf: SparkConf): SparkContext =
    this.getSparkSession(conf).sparkContext

  /*
  Get a normal SparkContext Object
 */
  def getSparkSession(conf: SparkConf = DefaultConf): SparkSession =
    SparkSession.builder().config(conf).getOrCreate()


  val DefaultConf =
    new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Cassandra Stress")
        .set("spark.hadoop.fs.dsefs.impl", "com.datastax.bdp.fs.hadoop.DseFileSystem") // without we get: 'java.io.IOException: No FileSystem for scheme: dsefs'
}
