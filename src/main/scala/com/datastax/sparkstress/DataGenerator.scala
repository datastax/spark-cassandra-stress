package com.datastax.sparkstress

import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Random, Success, Try}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import com.datastax.sparkstress.RowTypes._
import org.joda.time.DateTime

abstract class RowGenerator[T] extends Serializable{
  def generatePartition(seed: Long, index: Int) : Iterator[T]
}

object RowGenerator {


  def generateShortRowPartition(seed: Long, index: Int, opsPerPartition: Long) = {
    val r = new scala.util.Random(index * seed)
    val start = opsPerPartition*index
    (0L until opsPerPartition).map { i =>
      new ShortRowClass(i + start, r.nextString(20), r.nextString(20), r.nextString(20))
    }.iterator
  }

  def getShortRowRDD(ss: SparkSession, seed: Long, numPartitions: Int, numTotalRows: Long):
  RDD[ShortRowClass] = {

    val opsPerPartition = numTotalRows / numPartitions

    ss.sparkContext.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generateShortRowPartition(seed, index, opsPerPartition)
      }
    }
  }

  def getShortRowDataset(ss: SparkSession, seed: Long, numPartitions: Int, numTotalRows: Long): org.apache.spark.sql.Dataset[ShortRowClass] = {
    import ss.implicits._
    getShortRowRDD(ss, seed, numPartitions, numTotalRows).toDS()
  }

  def generateWideRowByPartitionPartition(seed: Long, index: Int, numPartitions: Int, numTotalKeys: Long, numTotalOps: Long) = {
    val r = new scala.util.Random(index * seed)
    val keysPerPartition = numTotalKeys / numPartitions
    val start = keysPerPartition * numPartitions
    val ckeysPerPkey = numTotalOps / numTotalKeys

    for ( pk <- (0L until keysPerPartition).iterator; ck <- (0L until ckeysPerPkey).iterator) yield
      new WideRowClass((start + pk), (ck).toString, r.nextString(20), r.nextString(20))
  }

  def getWideRowByPartition(ss: SparkSession, seed: Long, numPartitions: Int, numTotalOps: Long, numTotalKeys: Long):
  RDD[WideRowClass] = {

    ss.sparkContext.parallelize(Seq[Int](), numPartitions)
      .mapPartitionsWithIndex { case (index, n) => generateWideRowByPartitionPartition(seed, index, numPartitions, numTotalKeys, numTotalOps) }
  }

  def getWideRowByPartitionDataset(ss: SparkSession, seed: Long, numPartitions: Int, numTotalOps: Long, numTotalKeys: Long): org.apache.spark.sql.Dataset[WideRowClass] = {
    import ss.implicits._
    getWideRowByPartition(ss, seed, numPartitions, numTotalOps, numTotalKeys).toDS()
  }

  def generateWideRowPartition(seed: Long, index: Int, numTotalKeys: Long, opsPerPartition: Long) = {
    val r = new scala.util.Random(index * seed)
    val start = opsPerPartition*index:Long
    (0L until opsPerPartition).map { i =>
      new WideRowClass((i + start) % numTotalKeys, (i + start).toString, r.nextString(20), r.nextString(20))
    }.iterator
  }

  def getWideRowRdd(ss: SparkSession, seed: Long, numPartitions: Int, numTotalOps: Long, numTotalKeys: Long):
  RDD[WideRowClass] = {
    val opsPerPartition = numTotalOps /numPartitions

     ss.sparkContext.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generateWideRowPartition(seed, index, numTotalKeys, opsPerPartition)
      }
    }
  }

  def getWideRowDataset(ss: SparkSession, seed: Long, numPartitions: Int, numTotalOps: Long, numTotalKeys: Long): org.apache.spark.sql.Dataset[WideRowClass] = {
    import ss.implicits._
    getWideRowRdd(ss, seed, numPartitions, numTotalOps, numTotalKeys).toDS()
  }

  def generateRandomWideRowPartition(seed: Long, index: Int, numTotalKeys: Long, opsPerPartition: Long) = {
    val r = new scala.util.Random(index * seed)
    (0L until opsPerPartition).map { i =>
      new WideRowClass(math.abs(r.nextLong()) % numTotalKeys, r.nextInt.toString, r.nextString(20), r.nextString(20))
    }.iterator
  }

  def getRandomWideRow(ss: SparkSession, seed: Long, numPartitions: Int, numTotalOps: Long, numTotalKeys:
  Long): RDD[WideRowClass] = {
    val opsPerPartition = numTotalOps / numPartitions

    ss.sparkContext.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generateRandomWideRowPartition(seed, index, numTotalKeys, opsPerPartition)
      }
    }
  }

  def getRandomWideRowDataset(ss: SparkSession, seed: Long, numPartitions: Int, numTotalOps: Long, numTotalKeys: Long): org.apache.spark.sql.Dataset[WideRowClass] = {
    import ss.implicits._
    getRandomWideRow(ss, seed, numPartitions, numTotalOps, numTotalKeys).toDS()
  }

  /**
   * This code mimics an internal DataStax perf row format. Since we are mainly using this to test
   * Read speeds we will generate by C* partition.
   */
  val colors = List("red", "green", "blue", "yellow", "purple", "pink", "grey", "black", "white", "brown").view
  val sizes = List("P", "S", "M", "L", "XL", "XXL", "XXXL").view
  val qtys = (5 to 10000 by 5).view
  val perftime = new DateTime(2000,1,1,0,0,0,0)

  class PerfRowGenerator(numPartitions: Int, numTotalRows: Long, numTotalKeys: Long)
    extends RowGenerator[PerfRowClass]() {

    val clusteringKeysPerPartitionKey = numTotalRows / numTotalKeys
    val partitionKeysPerSparkPartition = numTotalKeys / numPartitions

    override def generatePartition(seed: Long, index: Int): Iterator[PerfRowClass] = {
      val offset = partitionKeysPerSparkPartition * index;
      val r = new scala.util.Random(index * seed)

      for ( pk <- (1L to partitionKeysPerSparkPartition).iterator; ck <- (1L to clusteringKeysPerPartitionKey).iterator) yield {
        val color = colors(r.nextInt(colors.size))
        val size = sizes(r.nextInt(sizes.size))
        val qty = qtys(r.nextInt(qtys.size))
        val store = s"Store ${pk + offset}"
        val order_number = UUID.randomUUID().toString()
        val order_time = new Timestamp(perftime.plusSeconds(r.nextInt(1000)).getMillis)
        PerfRowClass(store, order_time, order_number, color, size, qty)
      }
    }
  }

  def getPerfRowRdd(ss: SparkSession, seed: Long, numPartitions: Int, numTotalRows: Long, numTotalKeys: Long): RDD[PerfRowClass] = {
    val perfRowGenerator = new PerfRowGenerator(numPartitions, numTotalRows, numTotalKeys)
    ss.sparkContext.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        perfRowGenerator.generatePartition(seed, index)
      }
    }
  }

  def getPerfRowDataset(ss: SparkSession, seed: Long, numPartitions: Int, numTotalRows: Long, numTotalKeys: Long): org.apache.spark.sql.Dataset[PerfRowClass] = {
    import ss.implicits._
    getPerfRowRdd(ss, seed, numPartitions, numTotalRows, numTotalKeys).toDS()
  }

}

