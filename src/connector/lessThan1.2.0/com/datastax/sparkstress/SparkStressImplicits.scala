package com.datastax.sparkstress

import java.nio.file.Path

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.writer.{RowWriterFactory, WriteConf}
import com.datastax.spark.connector.{AllColumns, ColumnSelector}
import org.apache.spark.rdd.RDD

/* In these version of the connector cassandraCount did not exist and was invoked
by calling .count */
object SparkStressImplicits {

  implicit def toNotAvailaibleFunctions[T](rdd: RDD[T]): NotAvailableFunctions[T] =
    new NotAvailableFunctions(rdd)

  implicit def toBulkTableWriter[T](rdd: RDD[T]): BulkTableWriter[T] =
    new BulkTableWriter(rdd)
}

case class BulkWriteConf(outputDirectory: Option[Path] = None,
                         deleteSource: Boolean = true,
                         bufferSizeInMB: Int = 64)

class BulkTableWriter[T](rdd: RDD[T]) {

  def bulkSaveToCassandra(keyspaceName: String,
                          tableName: String,
                          columns: ColumnSelector = AllColumns,
                          writeConf: BulkWriteConf = BulkWriteConf()): Unit = {
    throw new UnsupportedOperationException
  }
}


class NotAvailableFunctions[T](rdd: RDD[T]) {

  def cassandraCount(): Long = {
    throw new UnsupportedOperationException("There are no push down Cassandra counts in Connector < 1.2")
  }

  def joinWithCassandraTable[K](args: Any*):CassandraRDD[(T,K)] = {
    throw new UnsupportedOperationException("Join With Cassandra Table doesn't exist in Connector < 1.2")
  }

  def repartitionByCassandraReplica(args: Any*): CassandraRDD[T] = {
    throw new UnsupportedOperationException("Repartition by Cassandra Table Doesn't exist in Connector < 1.2")
  }

  def spanBy[K](args: Any*):RDD[K] = {
    throw new UnsupportedOperationException("SpanBy Doesn't exist in the connector < 1.2")
  }

}
