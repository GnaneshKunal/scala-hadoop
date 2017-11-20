package gk.hadoop

import java.net.URI

import org.apache.spark.sql._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

object Basics extends App {

  //List Files

  val fs = FileSystem.get(new Configuration())
  val status = fs.listStatus(new Path("file:///"))
  status.foreach(x => println {
    x.getPath
  })

  val hdfs = FileSystem.get(new URI("hdfs://localhost:9000/"), new Configuration())
  val path = new Path("/data/test.txt")
  val stream = hdfs.open(path)
  def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine()))

  val arr = new Array[Byte](1000)

  stream.readFully(0, arr)

  arr.foreach(x => print(x.toChar))

}