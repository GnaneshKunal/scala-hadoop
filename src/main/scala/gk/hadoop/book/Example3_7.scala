package gk.hadoop.book

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import java.net.URI

// Implement a PathFilter for excluding paths that match a regex
object Example3_7 {

  val uri: String = "hdfs://localhost:9000/"
  val conf = new Configuration()
  val fs: FileSystem = FileSystem.get(URI.create(uri), conf)

  def main(args: Array[String]): Unit = {

    val path = new Path(uri + "data/*")

    val listStatuses = fs
      .globStatus(path, new RegexExcludeFilter("^.*2.txt$"))

    val listPaths = FileUtil.stat2Paths(listStatuses)

    listPaths.foreach(println)

  }

}

class RegexExcludeFilter(private final val regex: String)
  extends PathFilter {

  def accept(path: Path): Boolean = !path.toString.matches(regex)
}