package com.datastax.sparkstress

import com.datastax.spark.connector.rdd.{CassandraTableScanRDD, CassandraRDD}

/* In these version of the connector cassandraCount did not exist and was invoked
by calling .count */
object SparkStressImplicits {

  implicit def toCassandraCountable[T](rdd: CassandraTableScanRDD[T]): CassandraCountable[T] =
    new CassandraCountable(rdd)
}

class CassandraCountable[T](rdd: CassandraTableScanRDD[T]) {

  def cassandraCount(): Long = {
    rdd.count()
  }
}
