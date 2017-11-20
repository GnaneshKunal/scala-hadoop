package gk.hadoop.book

import java.net.URI

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io.IOUtils

// Displaying files from a Hadoop fs on stdout twice using seek()
object Example3_3 {

  def main(args: Array[String]): Unit = {
    val uri = "hdfs://localhost:9000/data/test.txt"
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(uri), conf)

    var in: FSDataInputStream = null

    in = fs.open(new Path(uri))
    IOUtils.copyBytes(in, System.out, 4096, false)
    in.seek(0)
    println{
      "======================================================================="
    }
    IOUtils.copyBytes(in, System.out, 4096, false)

    IOUtils.closeStream(in)
  }

}
