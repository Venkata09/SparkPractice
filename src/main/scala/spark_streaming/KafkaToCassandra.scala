package spark_streaming

import org.apache.spark.sql.functions.{from_json, from_unixtime, to_date}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object KafkaToCassandra extends SparkSessionBuilder {

  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession

    import spark.implicits._

    // Define location of Kafka brokers:
    val broker = "ec2-18-209-75-68.compute-1.amazonaws.com:9092," +
      "ec2-18-205-142-57.compute-1.amazonaws.com:9092," +
      "ec2-50-17-32-144.compute-1.amazonaws.com:9092"

    /*Here is an example massage which I get from a Kafka stream. It contains multiple jsons separated by \n
    {"timestamp_ms": "1530305100936", "fx_marker": "EUR/GBP"}
    {"timestamp_ms": "1530305100815", "fx_marker": "USD/CHF"}
    {"timestamp_ms": "1530305100969", "fx_marker": "EUR/CHF"}
    {"timestamp_ms": "1530305100011", "fx_marker": "USD/CAD"}
    */
    val dfraw = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", broker)
      .option("subscribe", "currency_exchange")
      .load()

    val schema = StructType(
      Seq(
        StructField("fx_marker", StringType, false),
        StructField("timestamp_ms", StringType, false)
      )
    )

    val df = dfraw
      .selectExpr("CAST(value AS STRING)").as[String]
      .flatMap(_.split("\n"))

    val jsons = df.select(from_json($"value", schema) as "data").select("data.*")

    val parsed = jsons
      .withColumn("timestamp_dt", to_date(from_unixtime($"timestamp_ms" / 1000.0, "yyyy-MM-dd HH:mm:ss.SSS")))
      .filter("fx_marker != ''")

    val sink = parsed
      .writeStream
      .queryName("KafkaToCassandraForeach")
      .outputMode("update")
      /*.foreach(new CassandraSinkForeach())*/
      .start()

    sink.awaitTermination()
  }
}