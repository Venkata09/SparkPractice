package concepts

import concepts.All_Joins.Row
import org.apache.spark.sql.SparkSession

object SelectiveDifferences_ColumnWise_Except {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val r1 = Seq(Row(1, "A1"), Row(2, "A2"), Row(3, "A3"), Row(4, "A4")).toDS()
    val r2 = Seq(Row(3, "A3"), Row(4, "A4"), Row(4, "A4_1"), Row(5, "A5"), Row(6, "A6")).toDS()


    r1.show()


    r2.show()


    /*

+---+-----+
| id|value|
+---+-----+
|  1|   A1|
|  2|   A2|
|  3|   A3|
|  4|   A4|
+---+-----+

+---+-----+
| id|value|
+---+-----+
|  3|   A3|
|  4|   A4|
|  4| A4_1|
|  5|   A5|
|  6|   A6|
+---+-----+


     */

    /*
    First we need to find the columns in expected and actual dataframes.


     */
    val columns = r1.schema.fields.map(_.name)

    /*
    Then we have to find difference columnwise.


     */
    val selectiveDifferences = columns.map(col => r2.select(col).except(r1.select(col)))
    /*
    At last we need to find out which columns contains different values.


     */
    selectiveDifferences.map(diff => {
      if (diff.count > 0) diff.show
    })


  }

}
