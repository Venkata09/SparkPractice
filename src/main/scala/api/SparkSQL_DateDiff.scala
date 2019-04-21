package api


import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.Try
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 3/3/2018.
  */
object SparkSQL_DateDiff {


  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext


    import spark.implicits._

    val df = Seq(
      ("red", "2016-11-29 07:10:10.234"),
      ("green", "2016-11-29 07:10:10.234"),
      ("blue", "2016-11-29 07:10:10.234")).toDF("color", "date")


    df.where(unix_timestamp($"date", "yyyy-MM-dd HH:mm:ss.S").cast("timestamp").between(Timestamp.valueOf(LocalDateTime.now().minusHours(1)),
      Timestamp.valueOf(LocalDateTime.now())
    )).show(false)


    import java.text.SimpleDateFormat

    val format = new SimpleDateFormat("dd-MM-yyyy")
    val data_dates = sc.parallelize(
      List(
        (100, "AAA", "13-06-2015"),
        (101, "BBB", "11-07-2015"),
        (102, "CCC", "15-08-2015"),
        (103, "DDD", "05-09-2015"),
        (100, "AAA", "29-08-2015"),
        (100, "AAA", "22-08-2015")).toSeq)
      .map {
        r =>
          val date: java.sql.Date = new java.sql.Date(format.parse(r._3).getTime)
          (r._1, r._2, date)
      }.toDF("ID", "Desc", "Week_Ending_Date")

    data_dates.show(false)


    data_dates.rdd.partitions.length

    println("<<<<<====================================================================================>>>>")

    data_dates.select(data_dates("Week_Ending_Date")).filter(data_dates("Week_Ending_Date").lt(lit("2015-08-14")))
      .show()
    println("<<<<<====================================================================================>>>>")
    data_dates.select(data_dates("Week_Ending_Date")).filter(data_dates("Week_Ending_Date").gt(lit("2015-03-14"))).show()
    println("<<<<<====================================================================================>>>>")

    val filteredData = data_dates
      .select(data_dates("ID"),
        date_format(data_dates("Week_Ending_Date"), "yyyy-MM-dd").alias("date")).filter($"date".between("2015-07-05", "2015-09-02"))


    val filteredData_1 = data_dates
      /*.where(date_format(data_dates("Week_Ending_Date"), "yyyy-MM-dd").alias("date"))*/
      .filter(date_format(data_dates("Week_Ending_Date"), "yyyy-MM-dd").between("2015-07-05", "2015-09-02")).select(data_dates("ID"))


    filteredData_1.distinct().show()


    filteredData.show()


  }
}
