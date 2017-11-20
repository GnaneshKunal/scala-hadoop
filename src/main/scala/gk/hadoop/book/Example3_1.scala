package gk.hadoop.book

import java.io.InputStream
import java.net.URL

import org.apache.hadoop.fs._
import org.apache.hadoop.io._


// Display files from Hadoop fs using URLStreamHandler
object Example3_1 {

  URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())

  def main(args: Array[String]): Unit = {
    val in: InputStream = new URL("hdfs://localhost:9000/data/test.txt").openStream()
    IOUtils.copyBytes(in, System.out, 4096, false)
    IOUtils.closeStream(in)

  }

}
