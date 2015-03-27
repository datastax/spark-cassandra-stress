package com.datastax.sparkstress

import java.lang.reflect.Constructor
import org.apache.spark.SparkContext
import scala.util.{Random, Failure, Success, Try}
import org.apache.spark.rdd.RDD
import java.util.Date


object RowGenerator {

  case class shortrowClass(key: Long, col1: String, col2: String, col3: String)

  def getShortRowRDD(sc: SparkContext, numPartitions: Int, numTotalRows: Long): RDD[shortrowClass] = {
    val opsPerPartition = numTotalRows / numPartitions

    def generatePartition(index: Int) = {
      val r = new scala.util.Random(index * System.currentTimeMillis())
      val start = opsPerPartition*index
      (0L until opsPerPartition).map { i =>
        new shortrowClass(i + start, r.nextString(20), r.nextString(20), r.nextString(20))
      }.iterator
    }

    sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generatePartition(index)
      }
    }

  }

  def getWideRowRdd(sc: SparkContext, numPartitions: Int, numTotalOps: Long, numTotalKeys:Long): RDD[shortrowClass] = {
    val opsPerPartition = numTotalOps /numPartitions

    def generatePartition(index: Int) = {
      val r = new scala.util.Random(index * System.currentTimeMillis())
      val start = opsPerPartition*index:Long
      (0L until opsPerPartition).map { i =>
        new shortrowClass((i + start)%numTotalKeys,(i+start).toString, r.nextString(20), r.nextString(20))
      }.iterator
    }
     sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generatePartition(index)
      }
    }
  }

  def getRandomWideRow(sc: SparkContext, numPartitions: Int, numTotalOps: Long, numTotalKeys: Long): RDD[shortrowClass] = {
    val opsPerPartition = numTotalOps / numPartitions

    def generatePartition(index: Int) = {
      val r = new scala.util.Random(index * System.currentTimeMillis())
      (0L until opsPerPartition).map { i =>
        new shortrowClass(math.abs(r.nextLong()) % numTotalKeys, r.nextInt.toString, r.nextString(20), r.nextString(20))
      }.iterator
    }
    sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generatePartition(index)
      }
    }
  }


  case class perfrowClass(key: String, color: String, size: String, qty: Int, time: Date,
                          col1: String, col2: String, col3: String, col4: String, col5: String,
                          col6: String, col7: String, col8: String, col9: String, col10: String)


  val colors = List("red", "green", "blue", "yellow", "purple", "pink", "grey", "black", "white", "brown").view
  val sizes = List("P", "S", "M", "L", "XL", "XXL", "XXXL").view
  val qtys = (1 to 500).view


  def getPerfRowRdd(sc: SparkContext, numPartitions: Int, numTotalRows: Long): RDD[perfrowClass] ={
    val opsPerPartition = numTotalRows / numPartitions

    def generatePartition(index: Int) = {
      val r = new scala.util.Random(index * System.currentTimeMillis())
      val start = opsPerPartition*index
      var csqIt = (for (color <- colors; size <- sizes; qty <- qtys) yield (color,size,qty)).iterator
      (0L until opsPerPartition).map { i =>
        if (!csqIt.hasNext){ csqIt = (for (color <- colors; size <- sizes; qty <- qtys) yield (color,size,qty)).iterator }
        val (color,size,qty) = csqIt.next()
        val extraString = "Operation_"+(i+start)
        new perfrowClass("Key_"+(i + start), color,size,qty,new java.util.Date,
          extraString,extraString,extraString,extraString,extraString,
          extraString,extraString,extraString,extraString,extraString)
      }.iterator
    }

    sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generatePartition(index)
      }
    }
  }


}

