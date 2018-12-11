package gk.hadoop.book.kafka

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

import java.nio.ByteBuffer
import java.util.Map

class CustomDeserializer extends Deserializer[Customer] {
  override def configure(configs: Map[String, _], key: Boolean): Unit = {

  }

  override def deserialize(topic: String, data: Array[Byte]) = {
    // var id: Int
    // var nameSize: Int
    // var name: String

    try {
      if (data == null)
        null
      if (data.length < 8)
        throw new SerializationException("Size of data received by IntegerDeserializer is shorter than expected")
      val buffer = ByteBuffer.wrap(data)
      val id = buffer.getInt()
      val nameSize = buffer.getInt()

      val nameBytes = new Array[Byte](nameSize)
      buffer.get(nameBytes)
      val name = new String(nameBytes, "UTF-8")

      new Customer(id, name)
    } catch {
      case e: Exception =>
        throw new SerializationException("Error when serializing Customer to byte[] " + e)
    }
  }
  override def close() = {

  }
}
