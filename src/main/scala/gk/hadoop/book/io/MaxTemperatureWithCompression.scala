package gk.hadoop.book.io

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.hadoop.io.compress._

// Application to run the maximum temperature job producing compressed output
object MaxTemperatureWithCompression extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder().appName("Max Temperature")
    .master("local[*]")
    .getOrCreate()

  val file = spark.read.textFile("hdfs://localhost:9000/data/190[0-2]*.gz")

  import spark.implicits._

  val airTemperature = file.map(x => {
    val year = x.substring(15, 19)
    val quality = x.substring(92, 93)

    val airTemp =
      if (x.charAt(87) == '+')
        x.substring(88, 92).toInt
      else
        x.substring(87, 92).toInt

    val temp =
      if (airTemp != 9999 && quality.matches("[01459]"))
        airTemp
      else
        0
    Temperature(year.toInt, temp)
  })
  airTemperature.createOrReplaceTempView("temperature")
  airTemperature.printSchema()

  val finalVal = airTemperature.sqlContext
    .sql("SELECT year, MAX(temp) AS temp FROM temperature GROUP BY year")

  finalVal.show()
  finalVal.rdd.repartition(1).saveAsTextFile("hdfs://localhost:9000/mapred", classOf[GzipCodec])

  spark.stop()
}

case class Temperature(year: Int, temp: Int)