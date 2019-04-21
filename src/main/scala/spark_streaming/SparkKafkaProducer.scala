package spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

/**
  * Created by vdokku on 1/2/2018.
  */
object SparkKafkaProducer {

  case class Person(id: String, name: String)


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("<<< SparkKafkaProducer >>>")
      .getOrCreate()


    import sparkSession.implicits._
    val caseSchema_1 = ScalaReflection.schemaFor[Person].dataType.asInstanceOf[StructType]

    // You are directly creating the  stream from the Folder CSV and giving structure as a Person object.
    val streamDataFrame = sparkSession.readStream.schema(caseSchema_1).option("header", "true").csv("/src/main/resources/csv").as[Person]

    // Now You can apply different transformation on it.

    /*
    selectExpr is a variant of select that selects columns in a SparkDataFrame while projecting SQL expressions.
     */

    val dataStream = streamDataFrame
      .selectExpr("CAST(id as STRING)", "CAST(name AS STRING) as value")


    // Now that you massged the data, and you need to write that data into the Kafka Stream

    val writeToKafkaStream = dataStream
      .writeStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "sample_read_topic")
      .option("checkpointLocation", "/tmp/").start() // --> This is the starting point.


    writeToKafkaStream.awaitTermination()

  }
}
