package gk.hadoop.book.spark

import org.apache.spark.sql._

object SparkInit extends App {

  val spark = SparkSession
    .builder()
    .appName("SparkInit")
    .master("local[*]")
    .getOrCreate()

  spark.conf
    .set("spark.serializer",
    "org.apache.spark.serializer.KyroSerializer")

  val sc = spark.sparkContext

  val range = 0 to 10

  val params = sc.parallelize(range, 10)

  val files = sc.wholeTextFiles("/home/ub/spark/README.md")
  files.collect()(0)._1

  spark.stop()

}
