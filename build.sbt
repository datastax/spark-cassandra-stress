import AssemblyKeys._

name := "spark-stress"

version := "0.1"

organization := "com.datastax"

scalaVersion := "2.10.4"

libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "4.5"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.2" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.1" % "provided"

resolvers += Resolver.sonatypeRepo("public")

//  Using DSE Paths for libraries
unmanagedClasspath in Compile <++= baseDirectory map  { base =>
    val DSE_HOME = System.getenv.get("DSE_HOME")
    val finder: PathFinder = (
        file(DSE_HOME+"/resources/hadoop/conf") ** "*"  +++
        file(DSE_HOME+"/resources/spark/lib") ** "*.jar" +++
        file(DSE_HOME+"/resources/driver/lib") ** "*.jar"  +++
        file(DSE_HOME+"/resources/cassandra/lib") ** "*.jar" +++
        file(DSE_HOME+"/resources/hadoop/") ** "*.jar" +++
        file(DSE_HOME+"/resources/dse/lib") ** "*.jar" +++
        file(DSE_HOME+"/lib") ** "*.jar"
      )
    finder.classpath
}

//  Using DSE Paths for libraries
unmanagedClasspath in Test <++= baseDirectory map  { base =>
  val DSE_HOME = System.getenv.get("DSE_HOME")
  val finder: PathFinder = (
    file(DSE_HOME+"/resources/hadoop/conf") ** "*"  +++
      file(DSE_HOME+"/resources/spark/lib") ** "*.jar" +++
      file(DSE_HOME+"/resources/driver/lib") ** "*.jar"  +++
      file(DSE_HOME+"/resources/cassandra/lib") ** "*.jar" +++
      file(DSE_HOME+"/resources/hadoop/") ** "*.jar" +++
      file(DSE_HOME+"/resources/dse/lib") ** "*.jar" +++
      file(DSE_HOME+"/lib") ** "*.jar"
    )
  finder.classpath
}

assemblySettings

test in assembly := {}

outputPath in assembly := file("target/spark-stress.jar")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
    {
        case PathList("META-INF", xs @ _*) =>
            (xs.map(_.toLowerCase)) match {
                case ("manifest.mf" :: Nil) => MergeStrategy.discard
                    // Note(harvey): this to get Shark perf test assembly working.
                case ("license" :: _) => MergeStrategy.discard
                case ps @ (x :: xs) if ps.last.endsWith(".sf") => MergeStrategy.discard
                    case _ => MergeStrategy.first
            }
        case PathList("reference.conf", xs @ _*) => MergeStrategy.concat
        case PathList("application.conf", xs @ _*) => MergeStrategy.concat
        case PathList("core-default.xml") => MergeStrategy.discard
        case PathList("hbase-default.xml") => MergeStrategy.discard
        case PathList("hdfs-default.xml") =>  MergeStrategy.discard
        case PathList("mapred-default.xml") => MergeStrategy.discard
            case _ => MergeStrategy.first
    }
}
