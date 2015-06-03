package com.datastax.sparkstress.WriteTaskTests

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{SparkTemplate, EmbeddedCassandra}
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class WriteTaskTests extends FlatSpec
  with EmbeddedCassandra
  with SparkTemplate
  with BeforeAndAfterAll
  with Matchers{

  def clearCache(): Unit = CassandraConnector.evictCache()
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
  conn.withSessionDo { session =>
    session.execute("""CREATE KEYSPACE test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    session.execute("""CREATE TABLE test.kv (k int PRIMARY KEY, v int)""")
    session.execute("""INSERT INTO test.kv (k,v) VALUES (1,1)""")

  }

  //TODO Replace this with real tests :)
  "This setup" should " let me access an emedded spark context " in {
    sc.parallelize(1 to 100).count should be (100)
  }

  it should " work with an embedded C* server " in {
    sc.cassandraTable("test","kv").count should be (1)

  }


}
