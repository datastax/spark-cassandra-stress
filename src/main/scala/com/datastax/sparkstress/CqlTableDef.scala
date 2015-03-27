package com.datastax.sparkstress

/**
 * Representation of a CQL table
 */
class CqlTableDef(var keyspace: String = "ks",
                  var table: String = "tab",
                  var columns: Seq[(String, String)] = Seq(("key", "int"), ("value", "int")),
                  var prikey: Seq[String] = Seq("key"),
                  var clskeys: Seq[String] = Seq()) {

  def getCqlCreateRepresentation(): String = {
    val TEMPLATE = "CREATE TABLE IF NOT EXISTS %s.%s ( %s , Primary Key %s)"
    val columnString = columns.map(x => "%s %s".format(x._1, x._2)).mkString(" , ")
    val pKeyString = prikey.mkString(" , ")
    val cKeyString = if (clskeys.length != 0) "," + clskeys.mkString(" , ") else ""
    val keyString = if (prikey.size > 1) "((%s)%s)".format(pKeyString, cKeyString) else "(%s%s)".format(pKeyString, cKeyString)

    TEMPLATE.format(keyspace, table, columnString, keyString)
  }

}

/**
 * Representation of a CQL Keyspace
 */
class KeyspaceDef(var keyspace: String = "ks",
                  var replication: Seq[(String, Any)] = Seq(("class", "SimpleStrategy"), ("replication_factor", 1))) {


  def getCqlCreateRepresentation(): String = {
    val TEMPLATE = "CREATE KEYSPACE IF NOT EXISTS %s with replication = {%s}"
    val replicationString = replication.map {
      case (x: String, y: String) => "'%s': '%s'".format(x, y)
      case (x: String, y: Int) => "'%s': %d".format(x, y)
    }.mkString(",")
    TEMPLATE.format(keyspace, replicationString)
  }


}
