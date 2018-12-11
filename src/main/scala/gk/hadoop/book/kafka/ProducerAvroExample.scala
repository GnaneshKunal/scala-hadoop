package gk.hadoop.book.kafka

import java.util.Properties
import java.io.ByteArrayOutputStream

// import io.confluent.kafka.serializers.KafkaAvroSerializer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import org.apache.avro.Schema.Parser
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.specific.{SpecificDatumWriter}
import org.apache.avro.io._

object ProducerAvroExample extends App {

  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "ProducerAvroExample")
  // props.put("serializer.class", "kafka.serializer.DefaultEncoder")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

  val producer = new KafkaProducer[String, Array[Byte]](props)

  val parser = new Parser()

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
"""
  )

  val datum = new GenericData.Record(schema)
  datum.put("id", 123)
  datum.put("name", "gnanesh")
  datum.put("email", "me@gnanesh.com")

  val writer = new SpecificDatumWriter[GenericRecord](schema)
  val out = new ByteArrayOutputStream()
  val encoder = EncoderFactory.get().binaryEncoder(out, null)
  writer.write(datum, encoder)
  encoder.flush()
  out.close()

  val serializedBytes = out.toByteArray()

  val message = new ProducerRecord[String, Array[Byte]]("test", "RECORD1", serializedBytes)
  producer.send(message).get()

}
