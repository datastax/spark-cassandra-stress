package com.datastax.sparkstress.WriteTaskTests.NonDseWriteTaskTests

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.datastax.sparkstress._

@RunWith(classOf[JUnitRunner])
class NonDseWriteTaskTests extends FlatSpec
  with BeforeAndAfterAll
  with Matchers{

  def clearCache(): Unit = CassandraConnector.evictCache()
/**
  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
  conn.withSessionDo { session =>
    session.execute(s"""CREATE KEYSPACE test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
  }

  //This test should run only when not building against DSE
  "WriteTask" should "throw an exception when using bulkSave" in {
    val config = new Config(saveMethod = "bulk")
    val writer = new WriteShortRow(config, sc)
    an [UnsupportedOperationException] should be thrownBy writer.run
  }
  **/
}