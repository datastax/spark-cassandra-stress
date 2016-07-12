package org.apache.spark
import org.apache.spark.ui.jobs.JobProgressListener

object ExposeJobListener {
  def getjobListener(sparkContext: SparkContext): JobProgressListener = {
    sparkContext.jobProgressListener
  }
}

