package com.datastax.sparkstress

import collection.JavaConverters._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession

trait StressTask {
    def runTrials(ss:SparkSession): Seq[TestResult]

    def time(f: => Any): (Long) = {
      val t0 = System.nanoTime()
      f
      System.nanoTime() - t0
    }

}

object StressTask {

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

  def getKeyspaceCql(ksName: String, localDC: String, replicationFactor: Int): String =
    s"""CREATE KEYSPACE IF NOT EXISTS $ksName
       |WITH replication = {'class': 'NetworkTopologyStrategy', '$localDC': $replicationFactor }""".stripMargin

}
