package gk.hadoop.book.kafka

import java.util.{Properties, Collections}

import scala.collection.JavaConversions._

import org.apache.kafka.clients.consumer.KafkaConsumer

object ConsumerExample extends App {

  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  kafkaProps.put("group.id", "CountryCounter")
  kafkaProps.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProps.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](kafkaProps)

  consumer.subscribe(Collections.singletonList("test"))

  try {
    while (true) {
      val records = consumer.poll(20)
      // println {
      //   records
      // }
      for (record <- records) {
        println("Topic: " + record.topic() + " partition: " + record.partition() + "offset: " + record.offset() + " key: " + record.key() + " value: " + record.value())
      }
    }
  } catch {
    case e: Exception =>
      e.printStackTrace()
  } finally {
    consumer.close()
  }

}
