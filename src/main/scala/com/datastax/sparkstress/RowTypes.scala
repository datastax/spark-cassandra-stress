package com.datastax.sparkstress

import java.util.Date

object RowTypes {

  sealed trait StressRow

  case class ShortRowClass(key: Long, col1: String, col2: String, col3: String) extends StressRow
  
  case class WideRowClass(key: Long, col1: String, col2: String, col3: String, col4: String, col5: String, col6: String, col7: String, col8: String, col9: String) extends StressRow

  case class WideRowClass(key: Long, col1: String, col2: String, col3: String, col4: String, 
                          col5: String, col6: String, col7: String, col8: String, col9: String) extends StressRow

  case class PerfRowClass(key: String, color: String, size: String, qty: Int, time: Date,
                          col1: String, col2: String, col3: String, col4: String, col5: String,
                          col6: String, col7: String, col8: String, col9: String, col10: String) extends StressRow
}
