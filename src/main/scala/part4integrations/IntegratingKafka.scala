package part4integrations

import common._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object IntegratingKafka {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  def readFromKafka() = {
    def kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()

    kafkaDF
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    // transform the carsDF into a format that Kafka understand
    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") // without checkpoints the writing to Kafka will fail
      .start()
      .awaitTermination()
  }


  /**
   * Exercise: write the whole cars data structures to Kafka as Json.
   * Use struct columns and the to_json function.
   *
   */

    def writeCarsToKafka() = {
      val carsDF = spark.readStream
        .schema(carsSchema)
        .json("src/main/resources/data/cars")

      val carsJsonDF = carsDF.select(
        col("name").as("key"),
        to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("string").as("value")
      )

      carsJsonDF.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "rockthejvm")
        .option("checkpointLocation", "checkpoints") // without checkpoints the writing to Kafka will fail
        .start()
        .awaitTermination()
    }


  def main(args: Array[String]): Unit = {
//    readFromKafka()
//    writeToKafka()
    writeCarsToKafka()
  }
}
