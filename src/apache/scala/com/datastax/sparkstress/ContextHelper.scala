package com.datastax.sparkstress

import org.apache.spark.{SparkContext, SparkConf}

object ConnectHelper {

  /*
  Get a normal SparkContext Object
   */
  def getContext(conf: SparkConf): SparkContext =
    new SparkContext(conf)
}
