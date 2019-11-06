package com.datastax.sparkstress

import org.apache.spark.rdd.RDD
import com.datastax.bdp.spark.writer.BulkTableWriter._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.RowWriterFactory

import scala.collection.JavaConverters._

object SparkStressImplicits {

  def bulkSaveToCassandra[T: RowWriterFactory](rdd: RDD[T], keyspace: String, table: String): Unit = {
    rdd.bulkSaveToCassandra(keyspace, table)
  }

  def clusterSize(connector: CassandraConnector): Int = {
    connector.withClusterDo(_.getMetadata.getAllHosts.size)
  }

  def getLocalDC(cc: CassandraConnector): String = {
      val hostsInProvidedDC = cc.hosts
      cc.withClusterDo(cluster =>
        cluster
          .getMetadata
          .getAllHosts.asScala
          .find( node => hostsInProvidedDC.contains(node.getAddress))
          .map(_.getDatacenter)
          .getOrElse("Analytics")
      )
  }
}
