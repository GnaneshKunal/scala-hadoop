package gk.hadoop.book

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import java.net.URI

// Showing the file statuses for a collection of paths in a hadoop file system.
object Example3_6 {

  val uri: String = "hdfs://localhost:9000/"
  val conf = new Configuration()
  val fs: FileSystem = FileSystem.get(URI.create(uri), conf)

  def main(args: Array[String]): Unit = {

    val path = new Array[Path](2)
    path(0) = new Path(uri)
    path(1) = new Path(uri + "data/")

    val status = fs.listStatus(path)
    val listedPaths = FileUtil.stat2Paths(status)

    listedPaths.foreach(println)

  }

}
