package gk.hadoop.book.parquet

import java.net.URI

import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.schema._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.parquet.hadoop.example.{GroupReadSupport, GroupWriteSupport}

object ParquetInit extends App {

  val conf = new Configuration()
  val uri = "hdfs://localhost:9000/"

  val dataFile = "data/data.parquet"

  val fs = FileSystem.get(URI.create(uri), conf)

  if (fs.exists(new Path(uri + dataFile))) {
    if (!fs.delete(new Path(uri + dataFile), false)) {
      System.err.println {
        "Can't remove existing parquet file"
      }
    }
  }


  val schema: MessageType =
    MessageTypeParser.parseMessageType(
      "message Pair {\n" +
      "  required binary left (UTF8);\n" +
      "  required binary right (UTF8);\n" +
      "}"
    )

  val groupFactory = new SimpleGroupFactory(schema)
  val group = groupFactory.newGroup()
    .append("left", "L")
    .append("right", "R")



  val path = new Path(uri + "data/data.parquet")

  val writeSupport =
    new GroupWriteSupport()
  GroupWriteSupport.setSchema(schema, conf)

  val writer: ParquetWriter[Group] =
    new ParquetWriter[Group](
      path,
      writeSupport,
      ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
      ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
      ParquetProperties.WriterVersion.PARQUET_1_0,
      conf
    )

  writer.write(group)
  writer.close()

  val readSupport =
    new GroupReadSupport()
  val reader: ParquetReader[Group] =
    new ParquetReader[Group](path, readSupport)

  val result = reader.read()

  println {
    result.toString
  }

}
