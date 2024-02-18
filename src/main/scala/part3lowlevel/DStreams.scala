package part3lowlevel

import common._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

object DStreams {

  val spark = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  /*
  Spark StreamingContext = entry point to the DStream API
  - needs the spark context
  - a duration - batch interval
   */
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
  - input sources by creating DStrems;
  - define transformations on DStreams (in the same fashion as we did with RDDs in static processing);
  - call an action on DStreams;
  - we need to trigger the start of all computations with ssc.start();
    - after this point, no more computation can be added;
  - await termination, or stop the computation;
    - after stopping the computation, you cannot restart the computation.
   */

  def readFromSocket(): Unit = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation -> lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action
//    wordsStream.print()
    wordsStream.saveAsTextFiles("src/main/resources/data/words") // each folder is an RDD = batch; each file is a partition of the RDD

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile() = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path) // directory where I will store a new file
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Sep 1 2000,12.88
          |AAPL,Oct 1 2000,9.78
          |AAPL,Nov 1 2000,8.25
          |AAPL,Dec 1 2000,7.44
          |AAPL,Jan 1 2001,10.81
          |AAPL,Feb 1 2001,9.12
          |""".stripMargin.trim)

      writer.close()
    }).start()
  }

  def readFromFile() = {
    createNewFile() // operates on another thread

    // define DStream
    val stocksFilePath = "src/main/resources/data/stocks"
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath) // ssc.textFileStream(stocksFilePath) only monitor the directory for NEW FILES

    // transformations
    val dateFormat = new SimpleDateFormat("MMM d yyyy")
    val stocksStream: DStream[Stock] = textStream.map{line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    // action
    stocksStream.print()

    // start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
  }

}
