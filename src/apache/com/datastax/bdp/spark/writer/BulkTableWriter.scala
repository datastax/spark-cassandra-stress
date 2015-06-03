package com.datastax.bdp.spark.writer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer._

import java.nio.file.{Path, Files}

import scala.language.implicitConversions

object BulkTableWriter{

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