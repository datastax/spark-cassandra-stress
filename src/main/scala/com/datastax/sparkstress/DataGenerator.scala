package com.datastax.sparkstress

import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import com.datastax.sparkstress.RowTypes._
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

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

  def getShortRowDataFrame(ss: SparkSession, seed: Long, numPartitions: Int, numTotalRows: Long): DataFrame = {
    import ss.implicits._
    getShortRowRDD(ss, seed, numPartitions, numTotalRows).toDF()
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

  def getWideRowByPartitionDataFrame(ss: SparkSession, seed: Long, numPartitions: Int, numTotalOps: Long, numTotalKeys: Long): DataFrame = {
    import ss.implicits._
    getWideRowByPartition(ss, seed, numPartitions, numTotalOps, numTotalKeys).toDF()
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

  def getWideRowDataFrame(ss: SparkSession, seed: Long, numPartitions: Int, numTotalOps: Long, numTotalKeys: Long): DataFrame = {
    import ss.implicits._
    getWideRowRdd(ss, seed, numPartitions, numTotalOps, numTotalKeys).toDF()
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

  def getRandomWideRowDataFrame(ss: SparkSession, seed: Long, numPartitions: Int, numTotalOps: Long, numTotalKeys: Long): DataFrame = {
    import ss.implicits._
    getRandomWideRow(ss, seed, numPartitions, numTotalOps, numTotalKeys).toDF()
  }

  /**
   * This code mimics an internal DataStax perf row format. Since we are mainly using this to test
   * Read speeds we will generate by C* partition.
   */
  val colors = ArrayBuffer("red", "green", "blue", "yellow", "purple", "pink", "grey", "black", "white", "brown")
  val sizes = ArrayBuffer("P", "S", "M", "L", "XL", "XXL", "XXXL")
  val qtys = 5 to 10000 by 5
  val perftime = new DateTime(2000,1,1,0,0,0,0)

  val color = colors(3)
  val size = sizes(3)
  val qty = qtys(4)
  val store = s"Store 23523"
  val order_number = new UUID(5325235,23523443)
  val order_time = perftime.plusSeconds(4)
  val row = PerfRowClass(store, order_time, order_number, color, size, qty)
  val tuple = (row.store, new Timestamp(row.order_time.getMillis), row.order_number.toString, row.color, row.size, row.qty)

  class PerfRowGenerator(numPartitions: Int, numTotalRows: Long, numTotalKeys: Long)
    extends RowGenerator[PerfRowClass]() {

    val clusteringKeysPerPartitionKey = numTotalRows / numTotalKeys
    val partitionKeysPerSparkPartition = numTotalKeys / numPartitions

    override def generatePartition(seed: Long, index: Int): Iterator[PerfRowClass] = {
      val offset = partitionKeysPerSparkPartition * index;
      val r = new scala.util.Random(index * seed)

      (for ( pk <- 1L to partitionKeysPerSparkPartition; ck <- 1L to clusteringKeysPerPartitionKey) yield {
        row
      }).iterator
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

  def getPerfRowDataFrame(ss: SparkSession, seed: Long, numPartitions: Int, numTotalRows: Long, numTotalKeys: Long): DataFrame = {
    import ss.implicits._
    // There exists no encoder for Joda DateTimeObjects so let's build a tuple that the encoders can handle
    getPerfRowRdd(ss, seed, numPartitions, numTotalRows, numTotalKeys).mapPartitions(
      it => it.map( p => tuple)
    ).toDF("store", "order_time", "order_number", "color", "size", "qty")
  }

}

