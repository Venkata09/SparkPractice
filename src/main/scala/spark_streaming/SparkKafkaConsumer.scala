package spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by vdokku on 1/2/2018.
  */
class SparkKafkaConsumer {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Consumer")
      .getOrCreate()
    import sparkSession.implicits._
    val schema = StructType(Array(
      StructField("id", StringType),
      StructField("name", StringType)
    ))

    val readDataFrame = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "gziptopic")
      .option("startingOffsets", "earliest")
      .load()


    val writeToConsole = readDataFrame.selectExpr("CAST(value AS STRING)").as[String]


    val query = writeToConsole.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()


    // Another Implmentation.

    val anotherQuery = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "gziptopic")
      .load()
      .select($"value".as[String])
      .map(d => {
        val name = d.toString
        name
      })
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    anotherQuery.awaitTermination()

  }
}
