package data_analysis_on_sample_data.scalaeg

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object ReadingCSV extends App {


  System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


  import org.apache.spark.sql.Encoders
  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()

  import spark.implicits._

  val titschema = Encoders.product[tit].schema

  val dfList1 = spark.read.option("inferSchema", true)
      .option("header", false)
      .option("quote", "\"")
    .option("ignoreLeadingWhiteSpace", true)
    .csv("src/main/resources/sample_data/data1.csv")


  dfList1.filter($"a" === "2").show
  dfList1.show()


  val dfList = spark.createDataFrame(dfList1.rdd, titschema)

  dfList1.printSchema()
  dfList.printSchema()

  dfList.show()
//
  dfList1.coalesce(1).rdd.saveAsTextFile("C:\\Venkat\\Spark_ML_Temp\\testfile.csv")

  case class tit(Num: String,
                 Class: String,
                 Survival_Code: Int,
                 Name: String,
                 Age: Int,
                 Province: String,
                 Address: String,
                 Coach_No: String,
                 Coach_ID: String,
                 Floor_No: Int,
                 Gender: String)

}


