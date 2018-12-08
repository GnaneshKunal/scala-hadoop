package gk.hadoop.book.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object ProducerCustomPartitioner extends App {

  val kafkaProps = new Properties()

  kafkaProps.put("bootstrap.servers", "localhost:9002")
  kafkaProps.put("client.id", "ProducerCustomPartitioner")
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("partitioner.class", "gk.hadoop.book.kafka.BananaPartitioner")

  val producer = new KafkaProducer[String, String](kafkaProps)
  val data1 = new ProducerRecord[String, String]("test", "Banana", "This is not a test 1")
  producer.send(data1).get()

  val data2 = new ProducerRecord[String, String]("test", "Banana", "This is not a test 2")
  producer.send(data2).get()

}
