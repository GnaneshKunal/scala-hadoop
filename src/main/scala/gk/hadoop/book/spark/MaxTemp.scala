package gk.hadoop.book.spark

import org.apache.spark.sql._

object MaxTemp extends App {

  val spark = SparkSession.builder()
    .appName("Max Temp")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  sc.textFile("hdfs://localhost:9000/data/temp.txt")
    .map(_.split('\t'))
    .filter(rec => rec(1) != "9999" && rec(2).matches("[01459]"))
    .map(rec => (rec(0).toInt, rec(1).toInt))
    .reduceByKey(Math.max)
    .saveAsTextFile("hdfs://localhost:9000/tmp/scmaxtemp")

  spark.stop()

}
