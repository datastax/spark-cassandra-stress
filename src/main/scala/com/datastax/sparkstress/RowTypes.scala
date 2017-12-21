package com.datastax.sparkstress

object RowTypes {

  sealed trait StressRow

  case class ShortRowClass(key: Long, col1: String, col2: String, col3: String) extends StressRow
  
  case class WideRowClass(key: Long, col1: String, col2: String, col3: String) extends StressRow

  case class PerfRowClass(
   store: String,
   order_time: java.sql.Timestamp,
   order_number:String, // java.util.UUID format
   color: String,
   size: String,
   qty: Int) extends StressRow

}