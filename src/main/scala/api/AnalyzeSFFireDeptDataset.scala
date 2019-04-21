package api

import org.apache.spark.sql.{SparkSession, _}
import org.joda.time.{DateTime, Years}
import org.joda.time.format.DateTimeFormat


/**
  * Created by vdokku on 1/29/2018.
  */
object AnalyzeSFFireDeptDataset {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< " +
      "AnalyzeSFFireDeptDataset >>>>>>>>>>> ").getOrCreate()


    import spark.sqlContext.implicits._

    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("CSV_FILE_PATH")




    // Once You get the data.

    println("*********************************************************")
    df.groupBy("Call Type").count().show(5)
    println("*********************************************************")

    // Find different types of call were made to the fire department
    val differentCallTypes = df.groupBy("Call Type").count()
    println(s"The different numbers of call types ===== $differentCallTypes")

    // Number of incidents of each call type
    val incidentsByCallType = df.select("Incident Number", "Call Type").groupBy("Call Type", "")
      .count().show(10, false)

    implicit def dateEncoder = org.apache.spark.sql.Encoders.kryo(classOf[DateTime])

    implicit def parserEncoder = org.apache.spark.sql.Encoders.kryo(classOf[DateTimeFormat])

    def getDate(date: String): DateTime = {
      val formatter = DateTimeFormat.forPattern("MM/dd/yyyy")
      DateTime.parse(date, formatter)
    }

    // Find start date
    def minDate = df.select("Call Date").map {
      case Row(date) => getDate(date.toString)
    }.reduce {
      (a, b) => if (a.isBefore(b)) a else b
    }

    // Find last date
    val maxDate = df.select("Call Date").map {
      case Row(date) => getDate(date.toString)
    }.reduce {
      (a, b) =>
        if (a.isAfter(b)) a else b
    }


    val noOfServiceCallsYear = Years.yearsBetween(minDate, maxDate).getYears


    // Number of calls in that last 7 days.

    val numberOfCallsFromLast7Days = df.select("Call Date").map {
      case Row(date) => getDate(date.toString)
    }.filter(_.isAfter(maxDate.minusDays(7))).count()


    df.select("city", "Neighborhood  District", "Call Date").createOrReplaceTempView("table")


    implicit def tupleEncoder = org.apache.spark.sql.Encoders.kryo[(String, Long)]


    val neighborhoodInSFGeneratedMostCalls = spark.sql("select * from table where city='San Francisco'")
      .filter(record => getDate(record.getString(2)).getYear == maxDate.getYear)
      .groupBy("Neighborhood  District")
      .count()
      .map(r => (r.getString(0), r.getLong(1)))
      .reduce((a, b) => if (a._2 > b._2) a else b)

    println(s"The neighborhood in SF with max generated calls ========== $neighborhoodInSFGeneratedMostCalls")


  }

}
