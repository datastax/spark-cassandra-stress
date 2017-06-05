package com.datastax.sparkstress

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.bdp.spark.DseSparkConfHelper._

object ConnectHelper {
  /**
   * Uses forDse method to enrich SparkConf before making spark context
   * @param conf
   */
  def getContext(conf: SparkConf): SparkContext =
    this.getSparkSession(conf).sparkContext

  def getSparkSession(conf: SparkConf): SparkSession =
    SparkSession.builder().config(conf.forDse).getOrCreate()
}
