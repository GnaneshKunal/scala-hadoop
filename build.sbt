name := "scala-hadoop"

version := "0.1"

scalaVersion := "2.11.8"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"

libraryDependencies += "org.apache.parquet" % "parquet" % "1.8.2"

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.13"

resolvers += "Apache HBase" at "https://repository.apache.org/content/repositories/releases"


resolvers += "Apache ZooKeeper" at "https://repository.apache.org/content/repositories/releases"


resolvers += "Thrift" at "http://people.apache.org/~rawson/repo/"