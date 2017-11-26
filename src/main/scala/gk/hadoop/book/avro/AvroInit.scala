package gk.hadoop.book.avro

import java.io.{ByteArrayOutputStream, File}

import org.apache.avro._
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic._
import org.apache.avro.io._


// Basic Avro functions
object AvroInit extends App {

  val parser = new Schema.Parser()
  val schema = parser.parse(
    this.getClass().getResourceAsStream(
      "./StringPair.avsc"
    )
  )

  val datum: GenericRecord = new GenericData.Record(schema)
  datum.put("left", "L")
  datum.put("right", "R")

  val out = new ByteArrayOutputStream()
  val writer =
    new GenericDatumWriter[GenericRecord](schema)

  val encoder = EncoderFactory.get().binaryEncoder(out, null)
  writer.write(datum, encoder)
  encoder.flush()
  out.close()

  val reader =
    new GenericDatumReader[GenericRecord](schema)

  val decoder =
    DecoderFactory.get().binaryDecoder(out.toByteArray, null)

  val result =
    reader.read(null, decoder)

  println {
    result.toString
  }

// file writer
  val file = new File("./src/main/scala/gk/hadoop/book/avro/data.avro")
  val dataFileWriter: DataFileWriter[GenericRecord] =
    new DataFileWriter[GenericRecord](writer)
  dataFileWriter.create(schema, file)
  dataFileWriter.append(datum)
  dataFileWriter.close()

  // file reader
  val dataFileReader: DataFileReader[GenericRecord] =
    new DataFileReader[GenericRecord](file, reader)
  println {
    dataFileReader.getSchema
  }


}
