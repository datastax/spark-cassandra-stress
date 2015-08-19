package com.datastax.sparkstress

import java.nio.file.Path

import com.datastax.bdp.spark.writer.BulkWriteConf
import com.datastax.spark.connector.{AllColumns, ColumnSelector}
import com.datastax.spark.connector.rdd.{CassandraTableScanRDD, CassandraRDD}
import org.apache.spark.rdd.RDD

/* In these version of the connector cassandraCount did not exist and was invoked
by calling .count */
object SparkStressImplicits {

  implicit def toCassandraCountable[T](rdd: CassandraTableScanRDD[T]): CassandraCountable[T] =
    new CassandraCountable(rdd)

  implicit def toBulkTableWriter[T](rdd: RDD[T]): BulkTableWriter[T] =
    new BulkTableWriter(rdd)
}

class BulkTableWriter[T](rdd: RDD[T]) {

  def bulkSaveToCassandra(keyspaceName: String,
                          tableName: String,
                          columns: ColumnSelector = AllColumns,
                          writeConf: BulkWriteConf = BulkWriteConf()): Unit = {
    throw new UnsupportedOperationException
  }
}

case class BulkWriteConf(outputDirectory: Option[Path] = None,
                         deleteSource: Boolean = true,
                         bufferSizeInMB: Int = 64)

class CassandraCountable[T](rdd: CassandraTableScanRDD[T]) {

  def cassandraCount(): Long = {
    throw new UnsupportedOperationException("There are no push down Cassandra counts in Connector < 1.2")
  }
}
