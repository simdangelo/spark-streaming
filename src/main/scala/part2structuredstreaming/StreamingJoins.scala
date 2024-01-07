package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingJoins {

  val spark = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  val guitarPlayers = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers")
  val guitars = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars")
  val bands = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands")

  // joining static DFs.
  val joinCondition = guitarPlayers.col("band")===bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joinCondition, "inner")

  // stream joins
  val bandsSchema = bands.schema
  def joinStreamWithStatic() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // join happens PER BATCH
    val streamedBandsGuitaristsDF = streamedBandsDF.join(guitarPlayers, guitarPlayers.col("band") === streamedBandsDF.col("id"))

    streamedBandsGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // since spark 2.3 we have stream vs stream joins
  def joinStreamWithStream() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedGuitaristsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(from_json(col("value"), guitarPlayers.schema).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id", "guitarPlayer.name as name", "guitarPlayer.guitars as guitars", "guitarPlayer.band as band")

    // join stream with stream
    val streamedJoins = streamedBandsDF.join(streamedGuitaristsDF, streamedGuitaristsDF.col("band") === streamedBandsDF.col("id"), "inner")
    streamedJoins.writeStream
      .format("console")
      .outputMode("append") // only append supported for stream vs stream joins
      .start()
      .awaitTermination()
  }



  def main(args: Array[String]): Unit = {
    joinStreamWithStream()
  }
}
