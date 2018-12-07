package gk.hadoop.book.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, Callback, RecordMetadata}

object ProducerSendExample extends App {

  val kafkaProps = new Properties()

  kafkaProps.put("bootstrap.servers", "localhost:9092")
  kafkaProps.put("client.id", "ProducerSendExample")
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](kafkaProps)

  for (x <- Range(0, 20)) {
    val data = new ProducerRecord[String, String]("test", "TINAT" + x, "This is some shit " + x + " test")

    producer.send(data).get()
  }

  val data = new ProducerRecord[String, String]("test", "TINAT", "This is definitly not a random test")
  producer.send(data).get()

  // class DemoProducerCallback extends Callback {
  //   override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
  //     if (e != null)
  //       e.printStackTrace()
  //     else {
  //       println {
  //         recordMetadata
  //       }
  //     }
  //   }
  // }

  val data2 = new ProducerRecord[String, String]("test", "TINAT2", "This is also not a test")
  producer.send(data2).get()


}
