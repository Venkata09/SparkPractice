package gov.uspto.est

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp


/**
  * @author vdokku
  */
object UDFExample_StackOverflow {

  def main(args: Array[String]): Unit = {


    /* System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")*/


    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Remove-String-From-One-DF-From-Another").getOrCreate()


    import spark.implicits._
    import org.apache.spark.sql.functions.{min, max}
    import org.apache.spark.sql.Row

    import org.apache.spark.sql.functions._


    val tripDF1 = Seq(
      ("8", Timestamp.valueOf("2017-02-12 03:04:00"), Timestamp.valueOf("2017-02-12 03:12:00"), "washington", "roslyn", "01010101", "Annual Member"),
      ("60", Timestamp.valueOf("2017-02-13 11:04:00"), Timestamp.valueOf("2017-02-13 12:04:00"), "reston", "ashburn", "01010102", "casual"),
      ("20", Timestamp.valueOf("2017-02-14 19:04:00"), Timestamp.valueOf("2017-02-14 19:24:00"), "Boston ", "roslyn", "01010103", "30 Day Member"),
      ("30", Timestamp.valueOf("2017-02-12 03:14:00"), Timestamp.valueOf("2017-02-12 03:44:00"), "Philadelphia ", "Washington", "01010104", "Annual Member"),
      ("17", Timestamp.valueOf("2017-02-11 12:04:00"), Timestamp.valueOf("2017-02-11 12:21:00"), "Baltimore", "Washington", "01010105", "casual"),
      ("30", Timestamp.valueOf("2017-02-15 05:00:00"), Timestamp.valueOf("2017-02-15 05:30:00"), "washington", "Miami ", "01010106", "30 Day Member"),
      ("20", Timestamp.valueOf("2017-02-16 07:10:00"), Timestamp.valueOf("2017-02-16 07:30:00"), "Cincinnati", "Chicago", "01010107", "casual"),
      ("10", Timestamp.valueOf("2017-02-17 17:10:00"), Timestamp.valueOf("2017-02-17 17:20:00"), "Raleigh", "Charlotte", "01010108", "30 Day Member"),
      ("30", Timestamp.valueOf("2017-02-15 05:00:00"), Timestamp.valueOf("2017-02-15 05:30:00"), "washington", "Miami ", "01010106", "30 Day Member"),
      ("20", Timestamp.valueOf("2017-02-16 07:10:00"), Timestamp.valueOf("2017-02-16 07:30:00"), "Cincinnati", "Chicago", "01010107", "casual"),
      ("10", Timestamp.valueOf("2017-02-17 17:10:00"), Timestamp.valueOf("2017-02-17 17:20:00"), "Raleigh", "Charlotte", "01010108", "30 Day Member"),
      ("30", Timestamp.valueOf("2017-02-15 05:00:00"), Timestamp.valueOf("2017-02-15 05:30:00"), "washington", "Miami ", "01010106", "30 Day Member"),
      ("20", Timestamp.valueOf("2017-02-16 07:10:00"), Timestamp.valueOf("2017-02-16 07:30:00"), "Cincinnati", "Chicago", "01010107", "casual"),
      ("10", Timestamp.valueOf("2017-02-17 17:10:00"), Timestamp.valueOf("2017-02-17 17:20:00"), "Raleigh", "Charlotte", "01010108", "30 Day Member")
    ).toDF("Duration", "StartDate", "EndDate", "StartStation", "EndStation", "BikeNumber", "MemberType")

    spark.udf.register("upperCase", (inputString:String) => inputString.toUpperCase())
    tripDF1.show()

    tripDF1.createOrReplaceTempView("TRIPS")

    spark.sql(" Select Duration, StartDate, EndDate, upperCase(StartStation), upperCase(EndStation), BikeNumber, MemberType  from TRIPS").show()




    spark.sql(" select * from TRIPS")

    /*

    Duration, Start Date, End Date, Start Station, End Station, Bike Number, Member Type


    Duration – Duration of trip
    Start Date – Includes start date and time
    End Date – Includes end date and time
    Start Station – Includes starting station name and number
    End Station – Includes ending station name and number
    Bike Number – Includes ID number of bike used for the trip
    Member Type – Indicates whether user was a "registered" member (Annual Member, 30-Day Member or Day Key Member) or a "casual" rider
    (Single trip, 24-Hour Pass, 3-Day Pass or 5-Day Pass)

    question is how to define UDF STRICTLY USING SPARK SQL to:

    ○ Convert the Start Station and End Station to UPPER case.

    ○ Generate columns ■ ‘start_day’ in format "mm-dd-YY", ■ ‘quarter_of_day’, ■ ‘is_weekend’ … all of the above using 'Start date' column.

    i tried every way but itz not happening

     */


  }


}
