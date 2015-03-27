package com.datastax.sparkstress
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import scala.util.{Random, Failure, Success, Try}
import org.apache.spark.rdd.RDD
import java.util.Date
import com.datastax.spark.connector.cql.CassandraConnector

/**
 * @author Russell Spitzer
 */
class Test {


  case class shortrowClass(key: Long, col1: String, col2: String, col3: String)

  CassandraConnector(sc.getConf).withSessionDo { session =>
    session.execute("DROP KEYSPACE IF EXISTS test")
    session.execute("CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE test.words (key bigint PRIMARY KEY, col1 text, col2 text, col3 text)")
  }

  def getShortRowRDD(sc: SparkContext, numPartitions: Int, numTotalRows: Long): RDD[shortrowClass] = {
    val opsPerPartition = numTotalRows / numPartitions

    def generatePartition(index: Int) = {
      val r = new scala.util.Random(index * System.currentTimeMillis())
      val start = opsPerPartition*index
      (0L until opsPerPartition).map { i =>
        new shortrowClass(i + start, r.nextString(20), r.nextString(20), r.nextString(20))
      }.iterator
    }

    sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex {
      case (index, n) => {
        generatePartition(index)
      }
    }
  }


  getShortRowRDD(sc, 50, 1000000).foreachPartition { it =>
    CassandraConnector(sc.getConf).withSessionDo { session =>
      val ps = session.prepare("INSERT INTO test.words (key, col1, col2, col3) VALUES (?,?,?,?)")
      val futures = for (row <- it) yield session.executeAsync(ps.bind(row.key: java.lang.Long, row.col1, row.col2, row.col3))
      futures.foreach( future => future.getUninterruptibly)
    }
  }

  getShortRowRDD(sc, 50, 1000000).saveToCassandra("test","words")


}

CassandraConnector(sc.getConf).withSessionDo { session =>
val ps = session.prepare("INSERT INTO test.words (key, col1, col2, col3) VALUES (?,?,?,?)")
val futures = g.map(row => session.executeAsync(ps.bind(row.key: java.lang.Long, row.col1, row.col2, row.col3)))
futures.foreach( future => future.getUninterruptibly)
}
