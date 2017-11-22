package gk.hadoop.book

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import java.net.URI

// Delete a file on hdfs
object Example3_8 {

  val uri: String = "hdfs://localhost:9000/"
  val conf = new Configuration()
  val fs: FileSystem = FileSystem.get(URI.create(uri), conf)

  def main(args: Array[String]): Unit = {
    val deleteFilePath = new Path(uri + "data/test4.txt")

    val status: String =
      if (fs.delete(deleteFilePath, false))
        "File has been deleted"
      else
        "File can't be deleted"

    println {
      status
    }

  }

}
