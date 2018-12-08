package gk.hadoop.book.kafka

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

import java.nio.ByteBuffer
import java.util.Map

class CustomSerializer extends Serializer[Customer] {

  override def configure(configs: Map[String, _], key: Boolean): Unit = {
    // nothing to configure
  }

  override def serialize(topic: String, data: Customer): Array[Byte] = {
    try {
      if (data == null)
        null
      else {
        val serializedName = data.customerName.getBytes("UTF-8")
        val stringSize = serializedName.length

        val buffer = ByteBuffer.allocate(4 + 4 + stringSize)
        buffer.putInt(data.customerID)
        buffer.putInt(stringSize)
        buffer.put(serializedName)

        buffer.array()
      }
    } catch {
      case e: Exception => throw new SerializationException("Error when serializing Customer to byte[] " + e)
    }
  }

  override def close(): Unit = {
    // nothing to close
  }
}
