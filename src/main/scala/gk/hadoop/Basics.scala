package gk.hadoop

import java.net.URI

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
  val path1 = new Path("/data/test.txt")
  val stream1 = hdfs.open(path1)

  val arr1 = new Array[Byte](1000)

  stream1.readFully(0, arr1)

  arr1.foreach(x => print(x.toChar))

  println {
    "\n==============================="
  }

  val path2 = new Path("/data/test2.txt")
  val stream2 = hdfs.open(path2)

  val buff2 = new Array[Byte](20)
  stream2.read(0, buff2, 0, 20)

  buff2.foreach(x => print{x.toChar})

}