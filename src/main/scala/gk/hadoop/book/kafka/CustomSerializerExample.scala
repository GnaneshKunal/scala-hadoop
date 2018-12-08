package gk.hadoop.book.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.Properties

object CustomSerializerExample extends App {

  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  kafkaProps.put("client.id", "CustomSerializerExample")
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "gk.hadoop.book.kafka.CustomSerializer")

  val producer = new KafkaProducer[String, Customer](kafkaProps)

  val customer1 = Customer(1, "gnanesh")
  val customer2 = Customer(2, "kunal")

  val data1 = new ProducerRecord[String, Customer]("test", "Data1", customer1)
  producer.send(data1).get()

  val data2 = new ProducerRecord[String, Customer]("test", "Data2", customer2)
  producer.send(data2).get()

}

