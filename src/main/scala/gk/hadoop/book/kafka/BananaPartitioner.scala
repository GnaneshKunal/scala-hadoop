package gk.hadoop.book.kafka

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.record.InvalidRecordException
import org.apache.kafka.common.utils.Utils

import scala.math

import java.util.Map

class BananaPartitioner extends Partitioner {

  def configure(configs: Map[String, _]): Unit = {}

  def partition(topic: String, key: Object, keyBytes: Array[Byte],
    value: Object, valueBytes: Array[Byte], cluster: Cluster): Int = {

    val partitions = cluster.partitionsForTopic(topic)
    val numPartitions = partitions.size()

    if ((keyBytes == null) || (!(key.isInstanceOf[String])))
      throw new InvalidRecordException("We expect all messages to have customer name as key")

    if (key.asInstanceOf[String].equals("Banana"))
      numPartitions

    (math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1))
  }

  def close(): Unit = {}
}
