package gk.hadoop.book.kafka

import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object ProducerExample extends App {
  // println("Hello, World")

  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "ScalaProduerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val events = 25
  val topic = "test"
  val rnd = new Random()

  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()
  for (nEvents <- Range(0, events)) {
    val runtime = new Date().getTime()
    val ip = "192.168.2." + rnd.nextInt(255)
    Thread.sleep(1000)
    val msg = runtime + ", " + nEvents + ", www.example.com, " + ip
    val data = new ProducerRecord[String, String](topic, ip, msg)

    producer.send(data).get()
  }

  System.out.println("Sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()

}
