package part4integrations

import common._
import org.apache.spark.sql.{Dataset, SparkSession}

object IntegratingJDBC {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  import spark.implicits._

  def writeStreamToPostgres() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch{
        (batch: Dataset[Car], batchId: Long) =>
          // each executor can control the batch
          // Batch  is a STATIC dataset/dataframe
          batch.write
            .format("jdbc")
            .option("driver", driver)
            .option("url", url)
            .option("user", user)
            .option("password", password)
            .option("dbtable", "public.cars")
            .save()
      }
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()
  }

}
