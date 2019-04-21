package api

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime


case class Employee(userId: String, jobTitleName: String, firstName: String, lastName: String, preferredFullName: String,
                    employeeCode: String, region: String, phoneNumber: String, emailAddress: String, formattedName: String)

object KafkaStructuredStreaming {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession
      .builder().master("local[4]")
      .appName("<<<<<<<<<<<< KafkaStructuredStreaming >>>>>>>>>>> ")
      .getOrCreate()


    println("======================== READ Stream =======================")

    val empSchema: StructType = StructType(Seq(
      StructField("userId", StringType, true),
      StructField("jobTitleName", StringType, true),
      StructField("firstName", StringType, true),
      StructField("lastName", StringType, true),
      StructField("preferredFullName", StringType, true),
      StructField("employeeCode", StringType, true),
      StructField("region", StringType, true),
      StructField("phoneNumber", StringType, true),
      StructField("emailAddress", StringType, true)
    )
    )

    // read data stream from Kafka
    import spark.implicits._
    val inputStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "streaming1")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "100")
      .load()
      .selectExpr("CAST(value AS STRING)").as[String]
      .select(from_json($"value", empSchema).as("data"))
      .select("data.*")

    val filterEmpData = inputStream.filter("userId is not null")

    filterEmpData.createTempView("employee_data")

    println("*************************************************************************************")
    val modifiedEmpDF = spark.sql(" select userId, jobTitleName, firstName, lastName, preferredFullName, " +
      "employeeCode, region, phoneNumber, emailAddress, concat(firstName, ' - ', lastName) as formattedName " +
      "from employee_data ").as[Employee]
    println("*************************************************************************************")

    println("hello 2")

    val url = "jdbc:mysql://localhost:3306/postdb"
    val user = "root"
    val pwd = "root"

    val writer = new JDBCSink(url, user, pwd)
    val query =
      modifiedEmpDF
        .writeStream
        .foreach(writer)
        .outputMode("update")
        .trigger(ProcessingTime("25 seconds"))
        .start()

    println("hello 3")
    query.awaitTermination()

    /*//Show Data after processed
    val printConsole = modifiedEmpDF.writeStream
      .format("console")
      // .option("truncate","false")
      .start()

    println("hello 3")
    printConsole.awaitTermination()*/


  }

}
