package spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionBuilder extends Serializable {
  // Build a spark session. Class is made serializable so to get access to SparkSession in a driver and executors.
  // Note here the usage of @transient lazy val
  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf()
      .setAppName("Structured Streaming from Kafka to Cassandra")
      .set("spark.cassandra.connection.host", "10.118.1.242")
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")
      .setMaster("local[*]")


    @transient lazy val spark = SparkSession
      .builder()
      .config(conf)

      .getOrCreate()

    spark
  }
}
