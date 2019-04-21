package gov.uspto.est

import org.apache.spark.sql.SparkSession


object ReadingData {


  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("SparkSQLFirstAndLastAggregatedFunctions").getOrCreate()
    import org.apache.spark.sql.functions.{max, _}
    import spark.implicits._

    val columnsDF = Seq(
      ("Harshit", 23, 43, 44, "A", "q", "z"),
      ("Mohit", 24, 56, 62, "B", "w", "x"),
      ("Harshit", 23, 32, 44, "C", "e", "c"),
      ("Kali", 10, 20, 460, "D", "r", "v"),
      ("Aman", 20, 30, 180, "E", "t", "b"),
      ("Ram", 30, 100, 270, "F", "yu", "n"),
      ("Kali", 10, 600, 360, "G", "io", "m"),
      ("Kali", 10, 600, 460, "k", "p", "o")
    ).toDF("ColA", "ColB", "ColC", "ColD", "ColE", "extraCol1", "extraCol2")


    println("Before Aggregation")
    columnsDF.show()

    val cols = List("colA", "colB")

    println("After Aggregation")
    val aggSeqFunction = columnsDF.agg(max(columnsDF.columns(2)),
      max(columnsDF.columns(3)),
      first(columnsDF.columns(4)),
      first(columnsDF.columns(6)),
      first(columnsDF.columns(5)))

    val aggFunction = aggSeqFunction.columns.map(en => expr(en))


    columnsDF.groupBy(cols.head, cols.tail: _*).agg(aggFunction.head, aggFunction.tail: _*).show()

    /*
            +-------+----+---------+---------+------------------+-----------------------+-----------------------+
            |   colA|colB|max(ColC)|max(ColD)|first(ColE, false)|first(extraCol2, false)|first(extraCol1, false)|
            +-------+----+---------+---------+------------------+-----------------------+-----------------------+
            |Harshit|  23|       43|       44|                 A|                      z|                      q|
            |   Aman|  20|       30|      180|                 E|                      b|                      t|
            |   Kali|  10|      600|      460|                 D|                      v|                      r|
            |    Ram|  30|      100|      270|                 F|                      n|                     yu|
            |  Mohit|  24|       56|       62|                 B|                      x|                      w|
            +-------+----+---------+---------+------------------+-----------------------+-----------------------+
     */


    /*

    After group by - we don't have any guarantee your data frame will remain sorted.

     */




    /*
    First is window function, until you order by it wont give you the result as expected.
    You need to do something like Window.partitionBy(colA,colB).orderBy(colE))

    When you groupBy in Spark, you may change the order of your DataFrame.
    But not always (eg. if your data is contained on a single worker, it won't change).

    Hence, just to make sure and to have a scalable solution, you need to re-order in your window function.

    In this case, try this:

    val w = Window.partitionBy($"key").orderBy($"value")
    df
        .withColumn("row_number", row_number.over(w))
        .where($"row_number" === 1)
        .drop("row_number")
    This selects only the first row, filtered on the row_number with row_number defined as the index of the row after ordering. This is dropped afterwards because it becomes useless.

    Remark: you may replace $ operators with col operators. It is only a shortcut for a more concise code.


     */




  }


}
