package concepts

import java.time.Month

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 2/11/2018.
  *
  *
  *
  * https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-multi-dimensional-aggregation.html
  *
  *
  *
  */


object MultiLevelAggreagationExample {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("<<<< GroupByExample  >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._


    sparkSession.sql("set spark.sql.shuffle.partitions = 10")

    val sales = Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    ).toDF("city", "year", "amount")

    import org.apache.spark.sql.functions._


    val groupByCityAndYear = sales
      .groupBy("city", "year") // <-- subtotals (city, year)
      .agg(sum("amount") as "amount")

    println(" <<<<<<<<<< I am coming HERE >>>>>>>>>> ")
    groupByCityAndYear.show(false)
    /*
    +-------+----+------+
    |city   |year|amount|
    +-------+----+------+
    |Boston |2015|101   |
    |Warsaw |2017|201   |
    |Boston |2016|150   |
    |Warsaw |2016|100   |
    |Toronto|2017|50    |
    +-------+----+------+
     */

    println(" <<<<<<<<<< I am coming HERE >>>>>>>>>> ")

    val groupByCityOnly = sales
      .groupBy("city") // <-- subtotals (city)
      .agg(sum("amount") as "amount")
      .select($"city", lit(null) as "year", $"amount") // <-- year is null


    println("<<<<<<<<<<<<<<<<< Spark Example 111>>>>>>>>>>>>>>>>")
    groupByCityOnly.show(false)
    println("<<<<<<<<<<<<<<<<< Spark Example 2222>>>>>>>>>>>>>>>>")

    val withUnion = groupByCityAndYear
      .union(groupByCityOnly)
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)


    withUnion.show()


    println(" Here I am starting the ROLL UP Process.... ")

    sales
      .rollup("city", "year") // You will be gettiing the Relational Grouped DataSets.
      .agg(sum("amount").as("Total_Summed_Amount"), grouping_id() as "gid") // Now this returns a DataFrame ...
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)
      .filter(grouping_id() =!= 3)
      .show(false)

    /*
    ("Warsaw", 2016, 100),
  ("Warsaw", 2017, 200),
  ("Boston", 2015, 50),
  ("Boston", 2016, 150),
  ("Toronto", 2017, 50)

|Warsaw |2016|100                |0  |
|Warsaw |2017|200                |0  |
|Warsaw |null|300                |1  | ====> After 1 City ----> this is the subtotal and the GID w.r.t Warsaw
|Toronto|2017|50                 |0  |
|Toronto|null|50                 |1  |====> After 1 City ----> this is the subtotal and the GID w.r.t Toronto
|Boston |2015|50                 |0  |
|Boston |2016|150                |0  |
|Boston |null|200                |1  |====> After 1 City ----> this is t he subtotal and the GID w.r.t Boston
+-------+----+-------------------+---+

     */

    println(" <<<<<<<<<<<<< Completed the ROLL - UP >>>>>>>>>>>>>>")

    sales.createOrReplaceTempView("Sales_Data")

    println(" <<<<<<<<<<<<< Completed the ROLL - UP with SQL >>>>>>>>>>>>>>")
    sparkSession.sql(
      """
        select city, year, sum(amount)
        from Sales_Data
        group by city, year
        grouping sets ((city, year) , (city), ())
        order by city desc nulls last, year asc nulls last


      """.stripMargin).show(false)
    /*
    +-------+----+-----------+
|city   |year|sum(amount)|
+-------+----+-----------+
|Warsaw |2016|100        |
|Warsaw |2017|200        |
|Warsaw |null|300        |  ===> Subtotal
|Toronto|2017|50         |
|Toronto|null|50         | ===> Subtotal
|Boston |2015|50         |
|Boston |2016|150        |
|Boston |null|200        | ===> Subtotal
|null   |null|550        | ==> Grand Total.
+-------+----+-----------+
     */
    println(" <<<<<<<<<<<<< Completed the ROLL - UP with SQL  >>>>>>>>>>>>>>")
    val withRollup = sales
      .rollup("city", "year")

      .agg(sum("amount") as "amount", grouping_id() as "gid")

      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)

      .filter(grouping_id() =!= 3)

      .select("city", "year", "amount")


    withRollup.show()

    sales.createOrReplaceTempView("sales")

    val withGroupingSets = sparkSession.sql(
      """
  SELECT city, year, SUM(amount) as amount
  FROM sales
  GROUP BY city, year

  GROUPING SETS ((city, year), (city))

  ORDER BY city DESC NULLS LAST, year ASC NULLS LAST
  """)


    import java.time.LocalDate
    import java.sql.Date
    val expenses = Seq(
      ((2012, Month.DECEMBER, 12), 5),
      ((2016, Month.AUGUST, 13), 10),
      ((2017, Month.MAY, 27), 15))
      .map { case ((yy, mm, dd), a) => (LocalDate.of(yy, mm, dd), a) }
      .map { case (d, a) => (d.toString, a) }
      .map { case (d, a) => (Date.valueOf(d), a) }
      .toDF("date", "amount")


    // rollup time!
    val q = expenses
      .rollup(year($"date") as "year", month($"date") as "month")
      .agg(sum("amount") as "amount")
      .sort($"year".asc_nulls_last, $"month".asc_nulls_last)


    val sales_1 = Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    ).toDF("city", "year", "amount")

    val q_1 = sales_1
      .rollup("city", "year")
      .agg(sum("amount") as "amount")
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)


    println("<<<<<<<<<<<< CUBE Operation 222222>>>>>>>>>>>>>>>")

    sales.cube("city", "year")
      .agg(sum($"amount").as("Total_Amount"))
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last).show(false)

    println("<<<<<<<<<<<< CUBE Operation 111111 >>>>>>>>>>>>>>>")


    /*

    +-------+----+------------+
|city   |year|Total_Amount|
+-------+----+------------+
|Warsaw |2016|100         |
|Warsaw |2017|200         |
|Warsaw |null|300         | ====> SubTotal for Warsaw City
|Toronto|2017|50          |
|Toronto|null|50          | ==> Subtotal for Toranto City.
|Boston |2015|50          |
|Boston |2016|150         |
|Boston |null|200         | ===> Subtotal for Boston City
|null   |2015|50          |===> Subtotal for 2015 City
|null   |2016|250         |===> Subtotal for 2016 City
|null   |2017|250         |===> Subtotal for 2017 City
|null   |null|550         | ==> Grand Total of all the
+-------+----+------------+

     */
  }

}
