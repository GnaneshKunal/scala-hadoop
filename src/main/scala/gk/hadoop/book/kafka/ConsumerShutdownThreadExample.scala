package gk.hadoop.book.kafka

import java.util.concurrent.Executors
import java.util.{Properties, Collections, Collection}

import scala.concurrent._
import ExecutionContext.Implicits.global
import JavaConversions.asExecutionContext
import scala.collection.JavaConversions._

import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.common.errors.WakeupException

class ConsumerShutdownThreadExample(
  val brokers: String,
  val groupID: String,
  val topic: String) {

  val props = createConsumerConfig(brokers, groupID)
  val consumer = new KafkaConsumer[String, String](props)

  def shutdown() = {
    if (consumer != null)
      consumer.close()
  }

  def createConsumerConfig(
    brokers: String,
    groupID: String): Properties = {
    val kafkaProps = new Properties()
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "TestGroup")
    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    kafkaProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    kafkaProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    kafkaProps.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.put("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps
  }

  def run() = {
    consumer.subscribe(Collections.singletonList(this.topic))

    val mainThread = Thread.currentThread()

    Runtime.getRuntime().addShutdownHook(new Thread(){
      override def run() = {
        println {
          "Starting exit..."
        }
        consumer.wakeup()
        try {
          mainThread.join()
        } catch {
          case e: WakeupException =>
            println("Got WakeupException")
        }
      }
    })

    class ApplicationThread {
      protected implicit val context =
        asExecutionContext(Executors.newSingleThreadExecutor())

      def run(code: => Unit) = Future(code)
    }

    (new ApplicationThread).run({
      try {
        while (true) {
          val records = consumer.poll(100)
          for (record <- records) {
            println {
              "topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset() + ", key = " + record.key() + ", value = " + record.value()
            }
          }
        }
      } catch {
        case e: WakeupException =>
          println("Got wakeup Exception")
        case e: Exception =>
          e.printStackTrace()
      } finally {
        consumer.close()
      }
    })
  }

}

object ConsumerShutdownThreadExample extends App {
  val example = new ConsumerShutdownThreadExample(
    "localhost:9092",
    "TestGroup",
    "test")

  example.run()
}
