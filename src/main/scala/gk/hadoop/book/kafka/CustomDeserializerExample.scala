package gk.hadoop.book.kafka

import org.apache.kafka.clients.consumer.{KafkaConsumer}

import scala.collection.JavaConversions._

import java.util.{Properties, Collections}

object CustomDeserializerExample extends App {

  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  kafkaProps.put("group.id", "CustomDeserializerExample")
  kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProps.put("value.deserializer", "gk.hadoop.book.kafka.CustomDeserializer")

  val consumer = new KafkaConsumer[String, Customer](kafkaProps)

  consumer.subscribe(Collections.singletonList("test"))

  try {
    while (true) {
      
      val records = consumer.poll(100)

      for (record <- records) {
        println {
          "Current Customer ID: " + record.value().customerID + " and current customer name: " + record.value().customerName
        }
      }
    }
  } catch {
    case e: Exception =>
      e.printStackTrace()
  } finally {
    consumer.close()
  }
}
