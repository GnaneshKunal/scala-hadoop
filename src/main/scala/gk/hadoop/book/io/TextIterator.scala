package gk.hadoop.book.io

import java.nio.ByteBuffer

import org.apache.hadoop.io.Text


// Iterating over the characters in a TextObject
object TextIterator extends App {

  val t = new Text("\u0041\u00DF\u6771\uD801\uDC00")

  val buf: ByteBuffer = ByteBuffer.wrap(t.getBytes, 0, t.getLength)

  var cp = 0

  while(buf.hasRemaining && (cp = Text.bytesToCodePoint(buf)) != -1) {
    println {
      Integer.toHexString(cp)
    }
  }

}
