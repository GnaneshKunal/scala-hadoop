package gk.hadoop.book.hdfs

import java.io._
import java.net.URI

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.util.Progressable

// Copy a local file to Hadoop fs
object FileCopyWithProgress {

  def main(args: Array[String]): Unit = {
    val localStr: String = "/home/ub/spark/README.md"
    val strDestination: String = "hdfs://localhost:9000/data/test3.txt"

    val in = new BufferedInputStream(new FileInputStream(localStr))

    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(strDestination), conf)
    val out = fs.create(new Path(strDestination), new Progressable() {
      override def progress(): Unit = {
        print(".")
      }
    })

    IOUtils.copyBytes(in, out, 4096, true)
  }
}
