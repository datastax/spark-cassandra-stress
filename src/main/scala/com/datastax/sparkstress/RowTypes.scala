package com.datastax.sparkstress

import java.util.UUID

object RowTypes {

  sealed trait StressRow

  case class ShortRowClass(key: Long, col1: String, col2: String, col3: String) extends StressRow
  
  case class WideRowClass(key: Long, col1: String, col2: String, col3: String) extends StressRow

  case class PerfRowClass(
    store: String,
    order_time: org.joda.time.DateTime,
    order_number: UUID,
    color: String,
    size: String,
    qty: Int) extends StressRow

}