package com.datastax.sparkstress

import com.datastax.driver.core.Cluster
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import com.datastax.spark.connector._
import java.util.Random
import java.util

/**
 * Created by russellspitzer on 5/19/14.
 * For great justice and Datastax
 */
abstract class WriteTask extends StressTask {


  var config = Config()

  def setupCQL() = {
    val ipReg = """\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}""".r
    val ip:String = ipReg findFirstIn(config.master) match {
      case Some(ipReg) => ipReg
      case None => "127.0.0.1"
    }
    val cluster = Cluster.builder().addContactPoint(ip).build()
    val session = cluster.connect()
    printf("Connected to Cassandra at %s\n", ip)
    val ksCreate = getKeyspaceDef()
    val tabCreate = getTableDef()
    if (config.deleteKeyspace){
      println("Destroying Keyspace")
      session.execute("DROP KEYSPACE "+ksCreate.keyspace)
    }
    printf("Running the following create statements\n%s\n%s\n",
      ksCreate.getCqlCreateRepresentation(),
      tabCreate.getCqlCreateRepresentation()
    )
    session.execute(ksCreate.getCqlCreateRepresentation())
    session.execute(tabCreate.getCqlCreateRepresentation())
    session.close()
    cluster.close()
    printf("Done Setting up CQL Keyspace/Table\n")
  }

  def getKeyspaceDef(): KeyspaceDef

  def getTableDef(): CqlTableDef

  def run(sc: SparkContext)

  def setConfig(c:Config){
    config=c
  }



  def runTrials(sc: SparkContext): Seq[Long] = {
    println("About to Start Trials")
    for (trial <- 1 to config.trials) yield {setupCQL(); time(run(sc))}
  }


}


class WriteShortRow extends WriteTask {

  var keyspaceDef = new KeyspaceDef()
  var tableDef = new CqlTableDef(columns = Seq(("key", "bigint"), ("col1", "text"), ("col2", "text"), ("col3", "text")))

  override def run(sc: SparkContext) = {
    setupCQL()
    val rdd = RowGenerator.getShortRowRDD(sc, config.numPartitions, config.totalOps)
    rdd.saveToCassandra(tableDef.keyspace, tableDef.table)
  }

  override def getKeyspaceDef(): KeyspaceDef = {
    keyspaceDef
  }

  override def getTableDef(): CqlTableDef = {
    tableDef
  }
}

class WritePerfRow extends WriteTask {

  var keyspaceDef = new KeyspaceDef()
  var tableDef = new CqlTableDef(columns =
    Seq(("key", "text"), ("size","text"), ("qty","int"), ("time","timestamp"),("color","text"),
    ("col1", "text"), ("col2", "text"), ("col3", "text"), ("col4","text"),("col5","text"),
    ("col6", "text"), ("col7", "text"), ("col8", "text"), ("col9","text"),("col10","text")
    )
  )

  override def run(sc: SparkContext) = {
    val rdd = RowGenerator.getPerfRowRdd(sc, config.numPartitions, config.totalOps)
    rdd.saveToCassandra(tableDef.keyspace, tableDef.table)
  }

  override def getKeyspaceDef(): KeyspaceDef = {
    keyspaceDef
  }

  override def getTableDef(): CqlTableDef = {
    tableDef
  }
}

class WriteWideRow extends WriteTask {

  var keyspaceDef = new KeyspaceDef()
  var tableDef = new CqlTableDef(columns = Seq(("key", "int"), ("col1", "text"), ("col2", "text"), ("col3", "text")), clskeys = Seq("col1"))

  override def run(sc: SparkContext) = {
    val rdd = RowGenerator.getWideRowRdd(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
    rdd.saveToCassandra(tableDef.keyspace, tableDef.table)
  }

  override def getKeyspaceDef(): KeyspaceDef = {
    keyspaceDef
  }

  override def getTableDef(): CqlTableDef = {
    tableDef
  }
}

class WriteRandomWideRow extends WriteTask {

  val keyspaceDef = new KeyspaceDef()
  val tableDef = new CqlTableDef(columns = Seq(("key", "int"), ("col1", "text"), ("col2", "text"), ("col3", "text")), clskeys = Seq("col1"))

  override def run(sc: SparkContext) = {
    val rdd = RowGenerator.getRandomWideRow(sc, config.numPartitions, config.totalOps, config.numTotalKeys)
    rdd.saveToCassandra(tableDef.keyspace, tableDef.table)
  }

  override def getKeyspaceDef(): KeyspaceDef = {
    keyspaceDef
  }

  override def getTableDef(): CqlTableDef = {
    tableDef
  }
}