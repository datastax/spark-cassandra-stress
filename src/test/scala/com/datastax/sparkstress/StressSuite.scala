package com.datastax.sparkstress

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.matchers.ShouldMatchers
import com.google.common.io.Files
import com.google.common.hash.Hashing
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import scala.util.Random
//To run continous on tests you need ~ ;assembly;test

class StressSuite extends FunSuite with BeforeAndAfterAll with ShouldMatchers {
  private var _sc: SparkContext = _
  private var _workingDir: String = _
  private var _session: Session = _
  private var _cluster: Cluster = _

  def sc: SparkContext = _sc

  def workingDir: String = _workingDir

  def session: Session = _session

  def cluster: Cluster = _cluster


  override def beforeAll() {
    _sc = new SparkContext("spark://127.0.0.1:7077", "test")
    _sc.addJar(System.getProperty("user.dir")+"/target/spark-stress.jar")
    _cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    _session = _cluster.connect()
    _workingDir = Files.createTempDir().getAbsolutePath
    super.beforeAll()
  }

  override def afterAll() {
    if (_sc != null) {
      _sc.stop()
      _sc = null
      _workingDir = null
    }
    if (_session != null) {
      _session.close()
      _session = null
    }
    if (_cluster != null) {
      _cluster.close()
      _cluster = null
    }

  }

  test("Test Keyspace Cql String Generation") {
    val x = new CqlTableDef()
    var result = x.getCqlCreateRepresentation()
    result should include("CREATE TABLE IF NOT EXISTS ks.tab ( key int , value int , Primary Key (key))")
    val y = new CqlTableDef(keyspace = "funkyspace")
    result = y.getCqlCreateRepresentation()
    result should include("funkyspace")
    val z = new CqlTableDef(columns = Seq(("a", "int"), ("e", "int"), ("d", "int"), ("c", "int")), prikey = Seq("e", "a"))
    result = z.getCqlCreateRepresentation()
    result should include("a int , e int , d int , c int")
    result should include("Primary Key ((e , a))")

  }

  test("Test Cql Keyspace String Generation") {
    val x = new KeyspaceDef()
    var result = x.getCqlCreateRepresentation()
    result should include("CREATE KEYSPACE IF NOT EXISTS ks with replication = {'class': 'SimpleStrategy','replication_factor': 1}")
  }


  test("Setup CQL") {
    val wt = new WriteShortRow()
    session.execute("DROP KEYSPACE IF EXISTS ks")
    wt.setupCQL()
    println(wt.tableDef.getCqlCreateRepresentation())
    session.execute("use ks;")
    session.execute("INSERT INTO ks.tab (key,col1, col2,col3) VALUES (0,'1','1','1')")
    val result = session.execute("SELECT * FROM ks.tab")
    val row = result.one()
    row.getLong("key") should equal(0)
    row.getString("col1") should equal("1")
    row.getString("col2") should equal("1")
    row.getString("col3") should equal("1")
  }


  test("Small Shortrow Generator") {
    val srtRdd = RowGenerator.getShortRowRDD(sc,10,100)
    val arr = srtRdd.toArray()
    arr.length should equal (100)
    arr.groupBy(x=>x.key).size should equal(100)
  }

  test("WideRow Generator"){
    val wdRdd = RowGenerator.getWideRowRdd(sc,10,100,5)
    val arr = wdRdd.toArray()
    arr.length should equal (100)
    arr.groupBy( x => x.key).size should equal (5)
    arr.groupBy( x => x.col1).size should equal (100)
  }

  test("PerfRow Generator"){
    val perfRdd = RowGenerator.getPerfRowRdd(sc,10,1000)
    val arr = perfRdd.toArray()
    arr.length should equal (1000)
    arr.filter(_.qty == 2).size should equal (10)
    arr.groupBy(_.key).size should equal (1000)
    println( arr(1))
  }



}
