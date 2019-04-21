import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/*

https://stackoverflow.com/questions/33729787/computing-rank-of-a-row

http://xinhstechblog.blogspot.com/2016/04/spark-window-functions-for-dataframes.html


https://stackoverflow.com/questions/40048919/what-is-the-difference-between-rowsbetween-and-rangebetween

ROWS BETWEEN doesn't care about the exact values. It cares only about the order of rows when computing frame.
RANGE BETWEEN considers values when computing frame.
Let's use an example using two window definitions:

ORDER BY x ROWS BETWEEN 2  PRECEDING AND CURRENT ROW
ORDER BY x RANGE BETWEEN 2  PRECEDING AND CURRENT ROW
and data as

+---+
|  x|
+---+
| 10|
| 20|
| 30|
| 31|
+---+
Assuming the current row is the one with value 31 for the first window following rows will be included (current one, and two preceding):

+---+----------------------------------------------------+
|  x|ORDER BY x ROWS BETWEEN 2  PRECEDING AND CURRENT ROW|
+---+----------------------------------------------------+
| 10|                                               false|
| 20|                                                true|
| 30|                                                true|
| 31|                                                true|
+---+----------------------------------------------------+
and for the second one following (current one, and all preceding where x >= 31 - 2):

+---+-----------------------------------------------------+
|  x|ORDER BY x RANGE BETWEEN 2  PRECEDING AND CURRENT ROW|
+---+-----------------------------------------------------+
| 10|                                                false|
| 20|                                                false|
| 30|                                                 true|
| 31|                                                 true|
+---+-----------------------------------------------------+




 */
/**
  * Created by vdokku on 6/29/2017.
  */
object SparkWindowFunctions {

  case class Foobar(foo: Option[Int], bar: Option[Int])

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master")

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("QueryingWithStreamingSources").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._


    case class Datum(product: String, category: String, revenue: Int)

    val customers = sc.parallelize(List(("Alice", "2016-05-01", 50.00),
      ("Alice", "2016-05-03", 45.00),
      ("Alice", "2016-05-04", 55.00),
      ("Bob", "2016-05-01", 25.00),
      ("Bob", "2016-05-04", 29.00),
      ("Bob", "2016-05-06", 27.00))).toDF("name", "date", "amountSpent")

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._


    println("**************Customer DF **************")
    customers.show()

    println("**************Customer Window Functions **************")

    val movingAverageSpec = Window.partitionBy("name").orderBy("amountSpent").rowsBetween(-1, 1)

    // Apply this window function (MOVING average)


    println("**************MOVING Average **************")
    customers.withColumn("movingAverage", avg(customers("amountSpent")).over(movingAverageSpec)).show()

    val cumulativeSum = Window.partitionBy("name").orderBy("amountSpent").rowsBetween(Long.MinValue, 0)

    println("**************Cumulative Sum **************")
    customers.withColumn("cumulativeSum", sum(customers("amountSpent")).over(cumulativeSum)).show()


    val rankAndLeadAndLagWindowSpec = Window.partitionBy("name").orderBy("amountSpent")
    customers.withColumn("RANK", rank().over(rankAndLeadAndLagWindowSpec)).show()

    customers.withColumn("LEAD", lead("amountSpent", 1).over(rankAndLeadAndLagWindowSpec)).show()

    // Both of the below queries, will result in the same result.

    customers.withColumn("LAG", lag("amountSpent", 1).over(rankAndLeadAndLagWindowSpec)).show()

    customers.select(col("*"), lag("amountSpent", 1).over(rankAndLeadAndLagWindowSpec).alias("LAG")).show()

    /*

        scala> customers.select(col("*"), lag("amountSpent", 1).over(rankAndLeadAndLagWindowSpec).alias("LAG")).show()
        +-----+----------+-----------+----+
        | name|      date|amountSpent| LAG|
        +-----+----------+-----------+----+
        |Alice|2016-05-03|       45.0|null|
          |Alice|2016-05-01|       50.0|45.0|
          |Alice|2016-05-04|       55.0|50.0|
          |  Bob|2016-05-01|       25.0|null|
          |  Bob|2016-05-06|       27.0|25.0|
          |  Bob|2016-05-04|       29.0|27.0|
          +-----+----------+-----------+----+
    */


    // DenseRank ->

    val rdd = sc.parallelize(Seq(("user_1", "object_1", 3),
      ("user_1", "object_2", 2),
      ("user_2", "object_1", 5),
      ("user_2", "object_2", 2),
      ("user_2", "object_2", 6))).toDF("unit", "objectInfo", "score")


    case class UserData(user: String, objectInfo: String, score: Int)

    val testDF = sc.parallelize(Seq(
      ("a", 5), ("b", 10), ("c", 5), ("d", 6)
    )).toDF("user", "value")

    val windowSpecTest_1 = Window.orderBy("value")

    val windowSpecTest = Window.orderBy("value")

    testDF.withColumn("RANK", rank().over(windowSpecTest)).show()

    case class sample(testString: String, testNumber: Int)


    // If I want to go with the SPARK SQL.

    testDF.createTempView("TestDF")

    spark.sqlContext.sql("select user, rank() over (order by value) as RANK from TestDF").show()

    //    if you want to convert an DF to an RDD.


    //If you want to convert a DF to an RDD.
    import org.apache.spark.sql.Row

    // If you want to convert
    val testRDD: Dataset[(String, Int)] = testDF.select($"user", $"value").map { case Row(user: String, value: Int) => (user, value) }


    val employeeSalaryDF = sc.parallelize(Seq(
      ("10", "1000"), ("10", "1000"), ("10", "2000"), ("10", "3000"), ("20", "5000"), ("20", "6000"), ("20", "NULL")
    )).toDF("dept_id", "salary")

    employeeSalaryDF.createTempView("EmployeeDF")

    spark.sqlContext.sql("select dept_id, salary, FIRST_VALUE(salary) over (order by (salary is not null), salary) first_sal from EmployeeDF").show()


    val df = sc.parallelize(Seq(
      Foobar(Some(1), Some(1235)), Foobar(Some(12345767), Some(2)),
      Foobar(None, Some(4)), Foobar(None, Some(1098)))).toDF()

    df.select(coalesce($"foo", $"bar", lit("--"))).show

    /*

    Returns the first column that is not null, or null if all inputs are null.
    For example, `coalesce(a, b, c)` will return a if a is not null,
    or b if a is null and b is not null, or c if both a and b are null but c is not null.



scala>     df.select(coalesce($"foo", $"bar", lit("--"))).show
+--------------------+
|coalesce(foo,bar,--)|
+--------------------+
|                   1|
|            12345767|
|                   4|
|                1098|
+--------------------+


scala>



d

     */
        df.select(coalesce($"foo", $"bar", lit("--"))).show




  }
}
