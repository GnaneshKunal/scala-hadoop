package gk.hadoop.book.io

import org.apache.hadoop.io.Text


// Showing the differences between the string and textClasses
object StringTextComparision extends App {

  val s: String = "\u0041\u00DF\u6771\uD801\uDC00"

  println {
    "String\n" +
    "sindexOf: " + s.indexOf("\u00DF") + "\n" +
    "s.charAt: " + s.charAt(0) + "\n" +
    "s.codePointAt: " + s.codePointAt(0) + "\n"
  }

  val t = new Text("\u0041\u00DF\u6771\uD801\uDC00")

  println {
    "Text\n" +
    "find: " + t.find("\u0041") + "\n" +
    "charAt: " + t.charAt(0) + "\n"
  }

}
