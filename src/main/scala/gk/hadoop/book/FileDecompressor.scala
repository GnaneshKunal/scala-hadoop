package gk.hadoop.book

import java.io.{InputStream, OutputStream}
import java.net.URI

import org.apache.hadoop.conf._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils

// A program to decompress a compressed file using a codec inferred from the file's extension
object FileDecompressor {

  val uri = "hdfs://localhost:9000/data/text.gz"
  val conf = new Configuration()
  val fs = FileSystem.get(URI.create(uri), conf)

  def main(args: Array[String]): Unit = {
    val input = new Path(uri)

    val factory = new CompressionCodecFactory(conf)
    val codec = factory.getCodec(input)

    if (codec == null) {
      System.err.println("No coec found for " + uri)
      System.exit(1)
    }

    val outputURI = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension())

    var in: InputStream = null
    var out: OutputStream = null

    try {
      in = codec.createInputStream(fs.open(input))
      out = fs.create(new Path(outputURI))
      IOUtils.copyBytes(in, out, conf)
    } finally {
      IOUtils.closeStream(in)
      IOUtils.closeStream(out)
    }
  }

}
