package gk.hadoop.book.hdfs

import java.io.InputStream
import java.net.URI

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils

// Display files from a Hadoop fs using FileSystem directly
object FileSystemCat {

  def main(args: Array[String]): Unit = {
    val uri: String = "hdfs://localhost:9000/data/test.txt"
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(uri), conf)
    var in: InputStream = null

    in = fs.open(new Path(uri))
    IOUtils.copyBytes(in, System.out, 4096, false)
    IOUtils.closeStream(in)
  }

}
