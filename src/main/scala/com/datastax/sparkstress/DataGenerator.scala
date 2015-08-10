package com.datastax.sparkstress

import java.util.UUID

import org.apache.spark.SparkContext
import scala.util.{Random, Failure, Success, Try}
import org.apache.spark.rdd.RDD
import com.datastax.sparkstress.RowTypes._
import org.joda.time.DateTime


object RowGenerator {


  def getShortRowRDD(sc: SparkContext, numPartitions: Int, numTotalRows: Long):
  RDD[ShortRowClass] = {
    val opsPerPartition = numTotalRows / numPartitions

    def generatePartition(index: Int) = {
      val r = new scala.util.Random(index * System.currentTimeMillis())
      val start = opsPerPartition*index
      (0L until opsPerPartition).map { i =>
        new ShortRowClass(i + start, r.nextString(20), r.nextString(20), r.nextString(20))
      }.iterator
    }

    sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generatePartition(index)
      }
    }

  }

  def getWideRowByPartition(sc: SparkContext, numPartitions: Int, numTotalOps: Long, numTotalKeys: Long):
  RDD[WideRowClass] = {
    val opsPerPartition = numTotalOps /numPartitions

    def generatePartition(index: Int) = {
      val r = new scala.util.Random(index * System.currentTimeMillis())
      val keysPerPartition = numTotalKeys / numPartitions
      val start = keysPerPartition * numPartitions
      val ckeysPerPkey = numTotalOps / numTotalKeys

      for ( pk <- (0L until keysPerPartition).iterator; ck <- (0L until ckeysPerPkey).iterator) yield
        new WideRowClass((start + pk), (ck).toString, r.nextString(20), r.nextString(20)) 
    }

    sc.parallelize(Seq[Int](), numPartitions)
      .mapPartitionsWithIndex { case (index, n) => generatePartition(index) }
  }

  def getWideRowRdd(sc: SparkContext, numPartitions: Int, numTotalOps: Long, numTotalKeys: Long):
  RDD[WideRowClass] = {
    val opsPerPartition = numTotalOps /numPartitions

    def generatePartition(index: Int) = {
      val r = new scala.util.Random(index * System.currentTimeMillis())
      val start = opsPerPartition*index:Long
      (0L until opsPerPartition).map { i =>
        new WideRowClass((i + start) % numTotalKeys, (i + start).toString, r.nextString(20), r.nextString(20)) 
      }.iterator
    }
     sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generatePartition(index)
      }
    }
  }

  def getRandomWideRow(sc: SparkContext, numPartitions: Int, numTotalOps: Long, numTotalKeys:
  Long): RDD[WideRowClass] = {
    val opsPerPartition = numTotalOps / numPartitions

    def generatePartition(index: Int) = {
      val r = new scala.util.Random(index * System.currentTimeMillis())
      (0L until opsPerPartition).map { i =>
        new WideRowClass(math.abs(r.nextLong()) % numTotalKeys, r.nextInt.toString, r.nextString(20), r.nextString(20)) 
      }.iterator
    }
    sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generatePartition(index)
      }
    }
  }


  /**
   * This code mimics an internal DataStax perf row format. Since we are manily using this to test
   * Read speeds we will generate by C* partition.
   */
  val colors = List("red", "green", "blue", "yellow", "purple", "pink", "grey", "black", "white", "brown").view
  val sizes = List("P", "S", "M", "L", "XL", "XXL", "XXXL").view
  val qtys = (5 to 10000 by 5).view
  val perftime = new DateTime(2000,1,1,0,0)
  val perfRandom = 42

  def getPerfRowRdd(sc: SparkContext, numPartitions: Int, numTotalRows: Long, numTotalKeys: Long): RDD[PerfRowClass] = {
    val clusteringKeysPerPartitionKey = numTotalRows / numTotalKeys
    val partitionKeysPerSparkPartition = numTotalKeys / numPartitions

    def generatePartition(index: Int): Iterator[PerfRowClass] = {
      val offset = partitionKeysPerSparkPartition * index;
      val r = new scala.util.Random(index * perfRandom)

      for ( pk <- (1L to partitionKeysPerSparkPartition).iterator; ck <- (1L to clusteringKeysPerPartitionKey).iterator) yield {
        val color = colors(r.nextInt(colors.size))
        val size = sizes(r.nextInt(sizes.size))
        val qty = qtys(r.nextInt(qtys.size))
        val store = s"Store ${pk + offset}"
        val order_number = UUID.randomUUID()
        val order_time = perftime.plusSeconds(r.nextInt(1000))
        PerfRowClass(store, order_time, order_number, color, size, qty)
      }
    }

    sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generatePartition(index)
      }
    }
  }


}

