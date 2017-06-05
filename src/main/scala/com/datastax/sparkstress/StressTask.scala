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
