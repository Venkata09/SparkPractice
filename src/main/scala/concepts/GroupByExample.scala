package concepts

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 1/3/2018.
  */
object GroupByExample {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("<<<< GroupByExample  >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._


    val officeData = sc.parallelize(
      Seq(
        (10, "Bob", "Manager", ""),
        (9, "Joe", "", "HQ"),
        (8, "Tim", "", "New York Office"),
        (7, "Joe", "", "New York Office"),
        (6, "Joe", "Head Programmer", ""),
        (5, "Bob", "", "LA Office"),
        (4, "Tim", "Manager", "HQ"),
        (3, "Bob", "", "New York Office"),
        (2, "Bob", "DB Administrator", "HQ"),
        (1, "Joe", "Programmer", "HQ")
      )).toDF("event_id", "name", "job", "location")

    import org.apache.spark.sql.functions._

    val latest = officeData.groupBy("name").agg(max(officeData("event_id")).alias("event_id"))
    latest.join(officeData, "event_id").drop("event_id").show
    officeData.createOrReplaceTempView("data")


    sqlContext.sql("desc data").show

    val df = sc.parallelize(Seq(
      ("1", "3", "unknown"),
      ("1", "unknown", "4"),
      ("2", "unknown", "3"),
      ("2", "2", "unknown")
    )).toDF("id", "group_a", "group_b")

    val groupAList = collect_list("group_a").as("group_a")
    val groupBList = collect_list("group_b").as("group_b")

    val grouped = df.groupBy("id").agg(groupAList, groupBList)

    grouped.show


    val removeUnknown = udf((list: Seq[String]) => {
      list.filter(value => !value.equalsIgnoreCase("unknown"))
    })

    grouped.printSchema()


    grouped.withColumn("group_a", removeUnknown($"group_a")).show
    grouped.withColumn("group_b", removeUnknown($"group_b")).show

    println(List("3", "unknown").filter(value => value.equalsIgnoreCase("unknown")))


    val datesDataFrame = sc.parallelize(Seq(
      ("2017-05-21", 1),
      ("2017-05-21", 1),
      ("2017-05-22", 1),
      ("2017-05-22", 1),
      ("2017-05-23", 1),
      ("2017-05-23", 1),
      ("2017-05-23", 1),
      ("2017-05-23", 1))).toDF("time_window", "foo")


//
//    datesDataFrame.select("*", date_format(datesDataFrame("time_window"), "yyyy-MM-dd hh:mm").alias("time_window")


//      data_1.withColumn("$time_window", date_format(data_1("time_window"), "yyyy-MM-dd hh:mm")).groupBy("$time_window").agg(sum("foo")).show

    import org.apache.spark.sql.functions._
    datesDataFrame.groupBy("date").agg(avg(when(col("foo") === "a", 1).otherwise(0))).show()

    // Have to revisit these scenarios.
//    datesDataFrame.groupBy("date").agg(avg((col("foo") == "a").cast("integer")))


    val data = sc.parallelize(Seq(
      (Array("1", "2", "3"), 1),
      (Array("1", "2", "3"), 1),
      (Array("1", "2", "3"), 1))).toDF("value", "value1")

    val toInt = udf((value: Seq[String]) => value.map(_.toInt).sum.toString)

    val i = datesDataFrame.withColumn("value", toInt(col("value"))).show














  }
}
