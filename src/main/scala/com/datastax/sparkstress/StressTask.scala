package com.datastax.sparkstress

import org.apache.spark.SparkContext

trait StressTask {
    def runTrials(sc:SparkContext): Seq[TestResult]

    def time(f: => Any): (Long) = {
      val t0 = System.nanoTime()
      f
      System.nanoTime() - t0
    }

}
