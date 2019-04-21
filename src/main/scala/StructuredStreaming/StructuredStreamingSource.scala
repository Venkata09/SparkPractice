package StructuredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
/**
  * Created by vdokku on 1/3/2018.
  */
object StructuredStreamingSource {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext


    import scala.concurrent.duration._
    val q = sparkSession
      .readStream
      .format("amadeus")
      .load
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime(5.seconds))
      .start
    q.awaitTermination

  }
}
