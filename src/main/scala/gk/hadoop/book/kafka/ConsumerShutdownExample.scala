package gk.hadoop.book.kafka

import java.util.{Properties, Collections}

import scala.collection.JavaConversions._

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException

object ConsumerShutdownExample extends App {
  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  kafkaProps.put("group.id", "ShutdownExample")
  kafkaProps.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProps.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](kafkaProps)

  consumer.subscribe(Collections.singletonList("test"))

  val mainThread = Thread.currentThread()


  Runtime.getRuntime().addShutdownHook(new Thread() {
    override def run() = {
      println {
        "Starting exit..."
      }
      consumer.wakeup()
      try {
        mainThread.join()
      } catch {
        case e: WakeupException =>
          println("Got wakeup Exception")
      }
    }
  })

  try {
    while (true) {
      val records = consumer.poll(20)

      for (record <- records) {
        println {
          "Topic: " + record.topic() + " partition: " + record.partition() + " offset: " + record.offset() + " key: " + record.key() + " value: " + record.value()
        }
      }
      consumer.commitSync()
    }
  } catch {
    case e: WakeupException =>
      println("Got wakeupException")
    case e: Exception =>
      e.printStackTrace()
  } finally {
    consumer.close()
  }

}
