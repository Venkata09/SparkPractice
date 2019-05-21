package concepts


import org.apache.spark.sql.SparkSession

object All_Joins {

  case class Row(id: Int, value: String)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val r1 = Seq(Row(1, "A1"), Row(2, "A2"), Row(3, "A3"), Row(4, "A4")).toDS()
    val r2 = Seq(Row(3, "A3"), Row(4, "A4"), Row(4, "A4_1"), Row(5, "A5"), Row(6, "A6")).toDS()

    val joinTypes = Seq("inner", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", "left_semi", "left_anti")

    joinTypes foreach { joinType =>
      println(s"${joinType.toUpperCase()} JOIN")
      r1.join(right = r2, usingColumns = Seq("id"), joinType = joinType).orderBy("id").show()
    }


    /*
    INNER JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  3|   A3|   A3|
|  4|   A4| A4_1|
|  4|   A4|   A4|
+---+-----+-----+

OUTER JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  1|   A1| null|
|  2|   A2| null|
|  3|   A3|   A3|
|  4|   A4|   A4|
|  4|   A4| A4_1|
|  5| null|   A5|
|  6| null|   A6|
+---+-----+-----+

FULL JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  1|   A1| null|
|  2|   A2| null|
|  3|   A3|   A3|
|  4|   A4|   A4|
|  4|   A4| A4_1|
|  5| null|   A5|
|  6| null|   A6|
+---+-----+-----+

FULL_OUTER JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  1|   A1| null|
|  2|   A2| null|
|  3|   A3|   A3|
|  4|   A4|   A4|
|  4|   A4| A4_1|
|  5| null|   A5|
|  6| null|   A6|
+---+-----+-----+

LEFT JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  1|   A1| null|
|  2|   A2| null|
|  3|   A3|   A3|
|  4|   A4| A4_1|
|  4|   A4|   A4|
+---+-----+-----+

LEFT_OUTER JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  1|   A1| null|
|  2|   A2| null|
|  3|   A3|   A3|
|  4|   A4| A4_1|
|  4|   A4|   A4|
+---+-----+-----+

RIGHT JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  3|   A3|   A3|
|  4|   A4| A4_1|
|  4|   A4|   A4|
|  5| null|   A5|
|  6| null|   A6|
+---+-----+-----+

RIGHT_OUTER JOIN
+---+-----+-----+
| id|value|value|
+---+-----+-----+
|  3|   A3|   A3|
|  4|   A4| A4_1|
|  4|   A4|   A4|
|  5| null|   A5|
|  6| null|   A6|
+---+-----+-----+

LEFT_SEMI JOIN
+---+-----+
| id|value|
+---+-----+
|  3|   A3|
|  4|   A4|
+---+-----+

LEFT_ANTI JOIN
+---+-----+
| id|value|
+---+-----+
|  1|   A1|
|  2|   A2|
+---+-----+
     */


  }

}
