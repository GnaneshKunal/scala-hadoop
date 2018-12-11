package gk.hadoop.book.kafka

import java.util.{Properties, Collections}
import java.io.ByteArrayOutputStream

import org.apache.kafka.clients.consumer.{KafkaConsumer}
import org.apache.kafka.common.serialization.StringSerializer

import org.apache.avro._
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic._
import org.apache.avro.io._

import scala.collection.JavaConversions._

object ConsumerAvroExample extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "ConsumerAvroExample")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

  val parser = new Schema.Parser()

  val schema = parser.parse(
    """{
	"namespace": "customerManagement.avro",
	"type": "record",
	"name": "Customer",
	"doc": "Customer Record",
	"fields": [
		  { "name": "id", "type": "int" },
		  { "name": "name", "type": "string" },
		  { "name": "email", "type": ["null", "string"], "default": "null"}
	]
}
""")

  case class Customer(id: Int, name: String, email: String)

  val reader = new GenericDatumReader[GenericRecord](schema)

  val consumer = new KafkaConsumer[String, Array[Byte]](props)

  consumer.subscribe(Collections.singletonList("test"))

  try {
    while (true) {
      val records = consumer.poll(20)

      for (record <- records) {
        println(record)
        val decoder =
          DecoderFactory.get().binaryDecoder(record.value(), null)

        val result =
          reader.read(null, decoder)

        println {
          "ID: " + result.get("id") + ", name: " + result.get("name") + ", email: " + result.get("email")
        }

        println {
          result.toString
        }

        // val dataFileReader =
        //   new DataFileReader[GenericRecord](schema, reader)

      }
    }
  } catch {
    case e: Exception =>
      e.printStackTrace()
  } finally {
    consumer.close()
  }

}
