package com.datastax.sparkstress

import java.net.InetSocketAddress

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.util.DriverUtil
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object SparkStressImplicits {

  def bulkSaveToCassandra[T: RowWriterFactory](rdd: RDD[T], keyspace: String, table: String): Unit = {
    // bulk save was removed in 6.9
    throw new UnsupportedOperationException
  }

  def clusterSize(connector: CassandraConnector): Int = {
    connector.withSessionDo(_.getMetadata.getNodes.size())
  }

  def getLocalDC(connector: CassandraConnector): String = {
    val hostsInProvidedDC = connector.hosts
    connector.withSessionDo { session =>
      val nodes = session.getMetadata
        .getNodes
        .values()
        .asScala

      nodes
        .find(node => DriverUtil.toAddress(node).exists(hostsInProvidedDC.contains))
        .orElse(nodes.headOption)
        .map(_.getDatacenter)
        .getOrElse("Analytics")
    }
  }
}
