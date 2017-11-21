package gk.hadoop.book

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import java.net.URI

// Demonstration File status Information
object Example3_5 {

  val conf = new Configuration()
  val hdfsURI = "hdfs://localhost:9000/"
  val fs = FileSystem.get(URI.create(hdfsURI), conf)

  def main(args: Array[String]): Unit = {

    fileStatusForFile()
    fileStatusForDirectory()
  }

  def fileStatusForFile(): Unit = {
    val file = new Path("/data/test3.txt")
    val stat = fs.getFileStatus(file)
    println{
      "getPath()\t" + stat.getPath() + "\n" +
      "isDirectory()\t" + stat.isDirectory() + "\n" +
      "getLen()\t" + stat.getLen() + "\n" +
      "getModificationTime()\t" + stat.getModificationTime() + "\n" +
      "getReplication()\t" + stat.getReplication() + "\n" +
      "getBlockSize()\t" + stat.getBlockSize() + "\n" +
      "getOwner()\t" + stat.getOwner() + "\n" +
      "getGroup()\t" + stat.getGroup() + "\n" +
      "getPermission().toString\t" + stat.getPermission().toString + "\n"
    }
  }

  def fileStatusForDirectory(): Unit = {
    val dir = new Path("/data")
    val stat = fs.getFileStatus(dir)
    println{
      "getPath()\t" + stat.getPath().toUri().getPath() + "\n" +
      "isDirectory()\t" + stat.isDirectory() + "\n" +
      "getLen()\t" + stat.getLen() + "\n" +
      "getModificationTime()\t" + stat.getModificationTime() + "\n" +
      "getReplication()\t" + stat.getReplication() + "\n" +
      "getBlockSize()\t" + stat.getBlockSize() + "\n" +
      "getOwner()\t" + stat.getOwner() + "\n" +
      "getGroup()\t" + stat.getGroup() + "\n" +
      "getPermission().toString\t" + stat.getPermission().toString + "\n"
    }
  }

}
