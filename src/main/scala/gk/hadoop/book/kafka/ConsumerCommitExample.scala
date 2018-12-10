package gk.hadoop.book.kafka

import java.util.concurrent.Executors
import java.util.{Properties, Collections}


import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.JavaConversions.asExecutionContext
import scala.collection.JavaConversions._

import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}


class ConsumerCommitExample(
  val brokers: String,
  val groupID: String,
  val topic: String) {

  val props = createConsumerConfig(brokers, groupID)
  val consumer = new KafkaConsumer[String, String](props)

  // // var executor: ExecutorService = null

  // def shutdown() = {
  //   if (consumer != null)
  //     consumer.close()
  //   if (executor != null)
  //     executor.shutdown()
  // }

  def shutdown() = {
    if (consumer != null)
      consumer.close()
  }

  def createConsumerConfig(
    brokers: String,
    groupID: String): Properties = {
    val kafkaProps = new Properties()
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)
    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
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

    class ApplicationThread {
      protected implicit val context =
        asExecutionContext(Executors.newSingleThreadExecutor())

      def run(code: => Unit) = Future(code)
    }

    (new ApplicationThread).run({
      try {
        while (true) {
          val records = consumer.poll(1000)

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
      // // async commit
      //consumer.commitAsync()
      try {
        consumer.commitSync()
      } catch {
        case e: Exception =>
          println("commit failed ", e)
      }

    })
  }

}

object ConsumerCommitExample extends App {

  val example = new ConsumerCommitExample(
    "localhost:9092",
    "TestGroup",
    "test")

  example.run()

}
