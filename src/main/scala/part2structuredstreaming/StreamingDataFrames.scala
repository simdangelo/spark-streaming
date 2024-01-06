package part2structuredstreaming

import common._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration._

object StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("Our first streams")
    .config("spark.master", "local[2]") // or .master("local[2]"), it's the same
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  def readFromSocket() = {
    // reading a DF
    val lines: DataFrame = spark.readStream // because we're going to read strings from the socket
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformation
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 5)

    // tell between a static vs a streaming df
    println(shortLines.isStreaming)

    // consuming a DF
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start

    // wait for the string to finish
    query.awaitTermination()
  }

  def readFromFiles() = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema) // opposed to reading static DF, on streaming DF you have to specify a schema
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream // because we're going to read strings from the socket
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // write the lines df at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
//      .trigger(Trigger.ProcessingTime(2.seconds)) // every 2 seconds run the query
//      .trigger(Trigger.Once())
      .trigger(Trigger.Continuous(2.seconds))
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
//    readFromSocket()
//    readFromFiles()
//    demoTriggers()
  }
}
