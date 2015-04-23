package com.datastax.sparkstress

import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.SparkContext
import scala.util.{Random, Failure, Success, Try}
import org.apache.spark.rdd.RDD
import com.datastax.sparkstress.RowTypes._
import java.net.URI
import java.util.UUID

object RowGenerator {

  val METHODS = Seq("POST", "GET", "DELETE", "PUT")
  val TOPLEVELDOMAINS = Seq("home", "about", "island", "mirror", "store", "images", "gallery", "products", "streams")
  val WORDS = Seq("apple", "banana", "orange", "kiwi", "lemon")
  val HEADERS = Seq("gzip", "deflate", "tar", "untar", "crunch", "smash", "smooth", "blend")
  class HttpUrlGenerator(queue: LinkedBlockingQueue[String], ips: Seq[String], port: Int) extends Runnable {
    def run() {
      println("Generator Starting")
      val r = new Random()
      val toplevelDomains = Seq("home", "about", "island", "mirror", "store", "images", "gallery", "products", "streams")
      val words = Seq("apple", "banana", "orange", "kiwi", "lemon")
      while (true) {
        val ip = ips(r.nextInt(ips.length))
        val tld = toplevelDomains(r.nextInt(toplevelDomains.length))
        val sub1 = words(r.nextInt(words.length))
        val sub2 = words(r.nextInt(words.length))
        val uri = s"/$tld/$sub1/$sub2"
        queue.put(s"http://$ip:$port$uri")
      }
    }
  }

  def getTimelineRowRDD(sc: SparkContext, numPartitions: Int, numTotalRows: Long):
  RDD[TimelineRowClass] = {
    val opsPerPartition = numTotalRows / numPartitions

    def generatePartition(index: Int) = {
      val r = new scala.util.Random(index * System.currentTimeMillis())
      val start = opsPerPartition*index
      (0L until opsPerPartition).map { i =>
        val url = "/"+TOPLEVELDOMAINS(r.nextInt(TOPLEVELDOMAINS.length))+
                  "/"+WORDS(r.nextInt(WORDS.length))+"/"+WORDS(r.nextInt(WORDS.length))
        val h1 = HEADERS(r.nextInt(HEADERS.length))
        val h2 = HEADERS(r.nextInt(HEADERS.length))
        new TimelineRowClass(
          UUIDs.unixTimestamp(UUIDs.timeBased()) / 10000L,
          url,
          UUIDs.timeBased(),
          METHODS(r.nextInt(METHODS.length)),
          Map("Accept-encoding" -> List(h1, h2)).map { case (k, v) => (k, v.mkString("#"))},
          "NO BODY"
        )
      }.iterator
    }

    sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generatePartition(index)
      }
    }

  }

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

      for ( pk <- (0L until keysPerPartition); ck <- (0L until ckeysPerPkey)) yield
        new WideRowClass((start + pk), (ck).toString, r.nextString(20), r.nextString(20)) 
    }.iterator

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




  val colors = List("red", "green", "blue", "yellow", "purple", "pink", "grey", "black", "white", "brown").view
  val sizes = List("P", "S", "M", "L", "XL", "XXL", "XXXL").view
  val qtys = (1 to 500).view


  def getPerfRowRdd(sc: SparkContext, numPartitions: Int, numTotalRows: Long): RDD[PerfRowClass] = {
    val opsPerPartition = numTotalRows / numPartitions

    def generatePartition(index: Int) = {
      val r = new scala.util.Random(index * System.currentTimeMillis())
      val start = opsPerPartition*index
      var csqIt = (for (color <- colors; size <- sizes; qty <- qtys) yield (color,size,qty)).iterator
      (0L until opsPerPartition).map { i =>
        if (!csqIt.hasNext){ csqIt = (for (color <- colors; size <- sizes; qty <- qtys) yield (color,size,qty)).iterator }
        val (color,size,qty) = csqIt.next()
        val extraString = "Operation_"+(i+start)
        new PerfRowClass("Key_" + (i + start), color, size, qty, new java.util.Date,
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

