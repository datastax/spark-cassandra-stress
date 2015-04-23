package com.datastax.sparkstress

import java.util.Date
import java.util.UUID
object RowTypes {

  sealed trait StressRow

  case class ShortRowClass(key: Long, col1: String, col2: String, col3: String) extends StressRow
  
  //case class TimelineRowClass(key: Long, col1: String, col2: Map[String, String], col3: UUID) extends StressRow
  case class TimelineRowClass( timesegment: Long, url: String, t_uuid: UUID, method: String, 
                               headers: Map[String, String], body: String) extends StressRow 
  
  case class WideRowClass(key: Long, col1: String, col2: String, col3: String) extends StressRow

  case class MethodAggRowClass( time: Long, url: String, method: String, count: Long) extends StressRow 

  case class SortedUrlRowClass( time: Long, url: String, count: Long) extends StressRow 

  case class PerfRowClass(key: String, color: String, size: String, qty: Int, time: Date,
                          col1: String, col2: String, col3: String, col4: String, col5: String,
                          col6: String, col7: String, col8: String, col9: String, col10: String) extends StressRow
}
