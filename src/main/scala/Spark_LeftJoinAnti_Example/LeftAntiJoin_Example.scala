package Spark_LeftJoinAnti_Example

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object LeftAntiJoin_Example {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("QueryingWithStreamingSources").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master")

    val tableName = "table1"
    val tableName2 = "table2"

    val format = new SimpleDateFormat("yyyy-MM-dd")
    val data = List(
      List("mike", 26, true),
      List("susan", 26, false),
      List("john", 33, true)
    )
    val data2 = List(
      List("mike", "grade1", 45, "baseball", new java.sql.Date(format.parse("1957-12-10").getTime)),
      List("john", "grade2", 33, "soccer", new java.sql.Date(format.parse("1978-06-07").getTime)),
      List("john", "grade2", 32, "golf", new java.sql.Date(format.parse("1978-06-07").getTime)),
      List("mike", "grade2", 26, "basketball", new java.sql.Date(format.parse("1978-06-07").getTime)),
      List("lena", "grade2", 23, "baseball", new java.sql.Date(format.parse("1978-06-07").getTime))
    )

    val rdd = spark.sparkContext.parallelize(data).map(Row.fromSeq(_))
    val rdd2 = spark.sparkContext.parallelize(data2).map(Row.fromSeq(_))


    val schema = StructType(Array(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("isBoy", BooleanType, false)
    ))
    val schema2 = StructType(Array(
      StructField("name", StringType, true),
      StructField("grade", StringType, true),
      StructField("howold", IntegerType, true),
      StructField("hobby", StringType, true),
      StructField("birthday", DateType, false)
    ))

    val df = spark.createDataFrame(rdd, schema)
    val df2 = spark.createDataFrame(rdd2, schema2)
    df.createOrReplaceTempView(tableName)
    df2.createOrReplaceTempView(tableName2)


    import spark.implicits._

    df.show(false)

    /*
+-----+---+-----+
|name |age|isBoy|
+-----+---+-----+
|mike |26 |true |
|susan|26 |false|
|john |33 |true |
+-----+---+-----+
     */

    df2.show(false)

    /*
+----+------+------+----------+----------+
|name|grade |howold|hobby     |birthday  |
+----+------+------+----------+----------+
|mike|grade1|45    |baseball  |1957-12-10|
|john|grade2|33    |soccer    |1978-06-07|
|john|grade2|32    |golf      |1978-06-07|
|mike|grade2|26    |basketball|1978-06-07|
|lena|grade2|23    |baseball  |1978-06-07|
+----+------+------+----------+----------+

     */

    df.as("table1").join(
      df2.as("table2"),
      $"table1.name" === $"table2.name" && $"table1.age" === $"table2.howold",
      "leftanti"
    ).show(false)

    /* Left ANTI ante difference between the two data frames. */
    /* If you look A*/

    /*
    +-----+---+-----+
|name |age|isBoy|
+-----+---+-----+
|susan|26 |false|
+-----+---+-----+
     */



    spark.sql(
      """SELECT table1.* FROM table1
        | LEFT ANTI JOIN table2
        | ON table1.name = table2.name AND table1.age = table2.howold
      """.stripMargin).show(false)



    println("=====================================================")
    println("<<<<<<<<<<<<<<<<<<< Difference in DATA FRAME >>>>>>>>>>>>>>>>>>>>>")
    println("=====================================================")

    val a = sc.parallelize(Seq((1, "a", 123), (2, "b", 456))).toDF("col1", "col2", "col3")
    val b = sc.parallelize(Seq((4, "a", 432), (2, "t", 431), (2, "b", 456))).toDF("col1", "col2", "col3")

    a.show(false)
    b.show(false)

    a.except(b).show(false)

  }
}
