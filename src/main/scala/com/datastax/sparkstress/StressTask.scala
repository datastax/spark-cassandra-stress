package com.datastax.sparkstress

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

  def getKeyspaceCql(ksName: String, localDC: String, replicationFactor: Int): String =
    s"""CREATE KEYSPACE IF NOT EXISTS $ksName
       |WITH replication = {'class': 'NetworkTopologyStrategy', '$localDC': $replicationFactor }""".stripMargin

}
