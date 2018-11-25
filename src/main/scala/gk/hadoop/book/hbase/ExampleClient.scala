package gk.hadoop.book.hbase

//import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import java.io.IOException

object ExampleClient extends App {
  val config = HBaseConfiguration.create()

  val admin = new AsyncHBaseAdmin(config)

//  val tableName = TableName.valueOf("test")
//  val htd = new HTableDescriptor(tableName)
//  val hcd = new HColumnDescriptor("data")
//  htd.addFamily(hcd);
//
//  admin.createTable(htd);
//
//  val tables = admin.listTables()
//
//  if (tables.length != 1 &&
//    Bytes.equals(tableName.getName, tables(0).getTableName.getName)) {
//    throw new IOException("Failed create of table")
//  }
//
//  val table = new HTable(config, tableName)
//
//  for (i <- 0 to 2) {
//    val row = Bytes.toBytes("row" + i)
//    val put = new Put(row)
//    val columnFamily = Bytes.toBytes("data")
//    val qualifier = Bytes.toBytes(String.valueOf(i))
//    val value = Bytes.toBytes("value" + i)
//    put.addImmutable(columnFamily, qualifier, value)
//    table.put(put)
//  }
//
//  val get = new Get(Bytes.toBytes("row1"))
//  val result = table.get(get)
//  println {
//    "Get: " + result
//  }
//  val scan = new Scan()
//  val scanner = table.getScanner(scan)
//
//  for (scannerResult <- scanner) {
//    println {
//      "Scan: " + scannerResult
//    }
//  }
//  scanner.close()
//
//  admin.disableTable(tableName)
//  admin.deleteTable(tableName)
//
//  table.close()

  admin.close()

}
