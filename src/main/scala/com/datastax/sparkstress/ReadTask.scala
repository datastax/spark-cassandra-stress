package com.datastax.sparkstress

import java.util.UUID

import com.datastax.sparkstress.RowTypes.PerfRowClass
import org.apache.spark.SparkContext
import com.datastax.spark.connector._

object ReadTask {
  val ValidTasks = Set(
    "CountAll",
    "AggregateColor",
    "AggregateColorSize",
    "FilterColorStringMatchCount",
    "FilterEqualityQtyColorSizeCount",
    "FilterEqualityQtyColorSizeCountFiveCols",
    "FilterLessThanQtyEqualityColorSizeCount",
    "FilterLessThanQtyEqualityColorSizeFiveCols",
    "FilterQtyMatchCount",
    "RetrieveSinglePartition"
  )
}

abstract class ReadTask(config: Config, sc: SparkContext) extends StressTask {

  val keyspace = config.keyspace
  val table = config.table

  def run()

  def runTrials(sc: SparkContext): Seq[Long] = {
    println("About to Start Trials")
    for (trial <- 1 to config.trials) yield {
      time(run())
    }
  }
}


class CountAll(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val count = sc.cassandraTable(keyspace, table).count
    if (config.totalOps != count) {
      println(s"Read verification failed! Expected ${config.totalOps}, returned $count");
    }
  }
}

class AggregateColor(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val colorCounts = sc.cassandraTable[String](keyspace, table).select("color").countByValue
    colorCounts.foreach(println)
  }
}

class AggregateColorSize(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val colorCounts = sc.cassandraTable[(String, String)](keyspace, table).select("color",
      "size").countByValue
    colorCounts.foreach(println)
  }
}

class FilterColorStringMatchCount(config: Config, sc: SparkContext) extends ReadTask(config, sc) {

  def run(): Unit = {
    val greenCount = sc.cassandraTable[PerfRowClass](keyspace, table).where("color = ?", "green")
      .count
    println(greenCount)
  }
}

class FilterEqualityQtyColorSizeCount(config: Config, sc: SparkContext) extends ReadTask(config,
  sc) {
  def run(): Unit = {
    val filterCount = sc.cassandraTable[PerfRowClass](keyspace, table)
      .where("color = ? AND size = ?", "red", "P")
      .filter(_.qty == 500)
      .count
    println(filterCount)
  }
}

class FilterEqualityQtyColorSizeCountFiveCols(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val filterResults = sc
      .cassandraTable[(UUID, Int, String, String, org.joda.time.DateTime)] (keyspace, table)
      .where("color = ? AND size = ?", "red", "P")
      .select("order_number", "qty", "color", "size", "order_time")
      .filter(_._2 == 500)
      .collect
    println(filterResults.size)
    filterResults.slice(0, 5).foreach(println)
  }
}

class FilterLessThanQtyEqualityColorSizeCount(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val filterResults = sc.cassandraTable[PerfRowClass](keyspace, table)
      .where("color = ? AND size = ?", "red", "P")
      .filter(_.qty <= 498)
      .count
    println(filterResults)
  }
}

class FilterLessThanQtyEqualityColorSizeCountFiveCols(config: Config, sc: SparkContext) extends
ReadTask(config, sc) {
  def run(): Unit = {
    val filterResults = sc
      .cassandraTable[(UUID, Int, String, String, org.joda.time.DateTime)] (keyspace, table)
      .where("color = ? AND size = ?", "red", "P")
      .select("order_number", "qty", "color", "size", "order_time")
      .filter(_._2 <= 498)
      .collect
    println(filterResults.size)
    filterResults.slice(0, 5).foreach(println)
  }
}

class FilterQtyMatchCount(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val filterResults = sc.cassandraTable[Int](keyspace, table)
      .select("qty")
      .filter(_ == 5)
      .count
    println(filterResults)
  }
}


class RetrieveSinglePartition(config: Config, sc: SparkContext) extends ReadTask(config, sc) {
  def run(): Unit = {
    val filterResults = sc.cassandraTable[Int](keyspace, table)
      .where("store = ? ", "store 5")
      .collect
    println(filterResults)
  }
}

