package StructuredStreaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by vdokku on 1/3/2018.
  */
object StructuredStreamingWindow {

  case class TimeSeriesEvent(id: Long, value: Int, time: Timestamp)

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()


    implicit val sqlContext = sparkSession.sqlContext


    import sqlContext.implicits._
    val intsInput = MemoryStream[TimeSeriesEvent]


    import org.apache.spark.sql.functions._

    val query = intsInput.toDS
      .groupBy(window('time, "1 seconds")).agg(sum("value").as("weekly_average") as "sum")
      .sort('window)
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete)
      .option("truncate", false)
      .start

    (0 to 5).foreach { batch =>
      intsInput.addData(
        TimeSeriesEvent(batch + 0, batch * 2, new Timestamp(System.currentTimeMillis())),
        TimeSeriesEvent(batch + 1, batch * 3, new Timestamp(System.currentTimeMillis() + 1000)))
      Thread.sleep(1000 * batch)
    }

    query.awaitTermination(1000 * (0 to 5).sum)
    sparkSession.stop()
  }
}
