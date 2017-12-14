package com.datastax.sparkstress

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.log4j.Logger

class StressReceiver[T](
  index: Int,
  rowGenerator: RowGenerator[T],
  config: Config,
  blockIntervalInMs: Int,
  storageLevel: StorageLevel)
  extends Receiver[T](storageLevel) {


  class EmitterThread(receiver: StressReceiver[_]) extends Thread(s"Emitter$index") {
    override def run(): Unit = {
      val rowIterator = rowGenerator.generatePartition(config.seed, index)
      val throughPutPerBlockInterval = (blockIntervalInMs / (config.streamingBatchIntervalSeconds * 1000.0) * config.receiverThroughputPerBatch).toLong
      while (rowIterator.hasNext) {
        val batchBegin = System.currentTimeMillis()
        for (x <- 1l to throughPutPerBlockInterval if rowIterator.hasNext) {
          store(rowIterator.next())
        }
        val batchEnd = System.currentTimeMillis()

        val napTime = blockIntervalInMs - (batchEnd - batchBegin)
        if (napTime > 0)
          Thread.sleep(napTime)
      }
      receiver.stop("Iterator Empty")
    }
  }


  def onStart() = {
    new EmitterThread(this).start()
  }



  def onStop() = {
  }
}
