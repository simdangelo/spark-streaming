package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  // include encoders for DF->DS transformations
  import spark.implicits._

  def readCars(): Dataset[Car] = {
    val carEncoder = Encoders.product[Car]
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with a single string column "value"
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // unwrap the previous column -- DF with multiple columns
      .as[Car]
    //      .as[Car](carEncoder) // encoder can be passed implicitly with spark.implicits
  }

  def showCarNames() = {
    val carsDS: Dataset[Car] = readCars()

    // tranformations here
    val carNamesDF: DataFrame = carsDS.select(col("Name")) // DF
    // alternative select - collection transformation maintain type info
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
   *
   * Exercises.
   * 1. Count how many powerful cars we have in the DS (HP > 140).
   * 2. Average HP fro the entire dataset (use “complete” output mode, because it’s the only available output mode since it is an aggregation).
   * 3. Count the cars by origin
   */

    // Exercise1
    def ex1() = {
      val carsDS = readCars()
      carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
        .writeStream
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination()
    }

  // Exercise2
  def ex2() = {
    val carsDS = readCars()
    carsDS.select(avg(col("Horsepower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // Exercise3
  def ex3(): Unit = {
    val carsDS = readCars()

    // option 1
//    carsDS.groupBy(col("Origin")).count()
//      .writeStream
//      .format("console")
//      .outputMode("complete")
//      .start()
//      .awaitTermination()

    //option 2 with dataset API
    carsDS.groupByKey(car => car.Origin).count()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    ex3()
  }
}
