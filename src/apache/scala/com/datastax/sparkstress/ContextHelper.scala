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
  def getSparkSession(conf: SparkConf): SparkSession =
    SparkSession.builder().config(conf).getOrCreate()
}
