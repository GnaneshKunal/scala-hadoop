package gk.hadoop.book.kafka

import java.util.concurrent.Executors
import java.util.{Properties, Collections, Collection}

import scala.concurrent._
import ExecutionContext.Implicits.global
import JavaConversions.asExecutionContext
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig, ConsumerRebalanceListener, OffsetAndMetadata}
import org.apache.kafka.common.{TopicPartition}

class ConsumerRebalanceListenerExample(
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
    /*
     * 1) Commit specified Offset (commiting for every 10 messages)
     * 2) We are also running custom rebalance events(HandleRebalance)
     */
    val currentOffsets =
      new HashMap[TopicPartition, OffsetAndMetadata]()
    var count = 0

    class HandleRebalance extends ConsumerRebalanceListener {

      def onPartitionsAssigned(
        partitions: Collection[TopicPartition]
      ) {
        println {
          "Got a new Partition"
        }
      }

      def onPartitionsRevoked(
        partitions: Collection[TopicPartition]
      ) {
        println {
          "Lost partitions in rebalance. Committing current offsets: " + currentOffsets
        }
        consumer.commitSync(currentOffsets)
      }
    }

    consumer.subscribe(Collections.singletonList(this.topic),
      new HandleRebalance())

    class ApplicationThread {
      protected implicit val context =
        asExecutionContext(Executors.newSingleThreadExecutor())

      def run(code: => Unit) = Future (code)
    }

    (new ApplicationThread).run({
      try {
        while (true) {
          val records = consumer.poll(100)
          for (record <- records) {
            println {
              "topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset() + ", key = " + record.key() + ", value = " + record.value()
            }
            currentOffsets.put(
              new TopicPartition(
                record.topic(),
                record.partition()
              ),
              new OffsetAndMetadata(
                record.offset() + 1,
                "no metadata"
              ))
            if (count % 10 == 0)
              consumer.commitAsync(currentOffsets, null)
            count += 1
          }
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally {
        try {
          consumer.commitSync(currentOffsets)
        } finally {
          consumer.close()
          println {
            "Closed consumer and we are done"
          }
        }
      }
    })
  }
}

object ConsumerRebalanceListenerExample extends App {

  val example = new ConsumerCommitExample(
    "localhost:9092",
    "TestGroup",
    "test")

  example.run()

}
