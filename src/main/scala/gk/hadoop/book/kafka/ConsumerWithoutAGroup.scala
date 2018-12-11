package gk.hadoop.book.kafka

import java.util.{Properties, Collections}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import java.util.List

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

object ConsumerWithoutAGroup extends App {

  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  // kafkaProps.put("group.id", "NoGroup")
  kafkaProps.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProps.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](kafkaProps)

  var partitionInfos: List[PartitionInfo] = null
  partitionInfos = consumer.partitionsFor(
    "test"// Collections.singletonList("test")
  )
  val partitions = new ListBuffer[TopicPartition]()

  if (partitionInfos != null) {
    for (partition <- partitionInfos)
      partitions += (new TopicPartition(
        partition.topic(),
        partition.partition()
      ))
    consumer.assign(partitions.toList)
  }

  try {
    while (true) {
      val records = consumer.poll(1000)

      for (record <- records) {
        println("Topic: " + record.topic() + " partition: " + record.partition() + "offset: " + record.offset() + " key: " + record.key() + " value: " + record.value())
      }
      consumer.commitSync()
    }
  } catch {
    case e: Exception =>
      e.printStackTrace()
  } finally {
    consumer.close()
  }

}
