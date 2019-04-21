package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author vdokku
  */
object FindingMinAndMax {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Find-Min-Max").getOrCreate()


    import spark.implicits._
    import org.apache.spark.sql.functions.{min, max, _}
    import org.apache.spark.sql.Row

    val df = Seq((2.0, 2.1), (21.2, 11.4), (99.9, 109.1))
      .toDF("Col_A", "Col_B")

    val df_List = List((2.0, 2.1, 133.2), (21.2, 11.4, 232.1), (11.2, 96.4, 14.0))
      .toDF("Col_A", "Col_B", "Col_C")

    println("-------------------------------------------")
    df.show()
    println("-------------------------------------------")

    println("-------------------------------------------")
    df_List.show()
    println("-------------------------------------------")


    println("-------------------------------------------")

    df.agg(
      max(df(df.columns(0))).as("MAX of the Data FRAME "),
      min(df(df.columns(0))).as("Min of the Data FRAME"))
      .show
    println("-------------------------------------------")

    println("-------------------------------------------")
    /*
    Out of this SEQUENCE COLUMN (1) lo <---> MAX Element is 109.1 & MIN element is 2.1
    Out of this SEQUENCE COLUMN (1) lo <---> MAX Element is 99.9 & MIN element is 2.0

    Seq((2.0, 2.1), (21.2, 11.4), (99.9, 109.1))

     */

    df.agg(
      max(df(df.columns(1))).as("MAX of the Data FRAME "),
      min(df(df.columns(1))).as("Min of the Data FRAME"))
      .show
    println("-------------------------------------------")
    /*

+----------------------+---------------------+
|MAX of the Data FRAME |Min of the Data FRAME|
+----------------------+---------------------+
|                1111.4|                  2.1|
+----------------------+---------------------+

     */

    val dataFrame1 = Seq(
      (1, 2, 1, 0, 3),
      (2, 5, 2, 1, 4),
      (3, 2, 2, 4, 1),
      (4, 5, 1, 1, 3),
      (5, 5, 1, 2, 4),
      (6, 2, 1, 0, 3),
      (7, 2, 2, 4, 1))
      .toDF("sno", "fir", "sec", "th", "four")

    val attr = List("sno", "fir", "sec", "th", "four")


    dataFrame1.show()

    println("#######################################")
    println(attr.map(max))
    println("#######################################")

    /*
    #######################################
    List(max(sno), max(fir), max(sec), max(th), max(four))
    #######################################
     */

    println("#######################################")
    dataFrame1.select(attr.map(max): _*).show()
    println("#######################################")

    /*
#######################################
+--------+--------+--------+-------+---------+
|max(sno)|max(fir)|max(sec)|max(th)|max(four)|
+--------+--------+--------+-------+---------+
|       7|       5|       2|      4|        4|
+--------+--------+--------+-------+---------+

#######################################
     */

    dataFrame1.select(attr.map(max) ++ attr.map(min): _*).show(false)
    /*
        +--------+--------+--------+-------+---------+--------+--------+--------+-------+---------+
        |max(sno)|max(fir)|max(sec)|max(th)|max(four)|max(sno)|max(fir)|max(sec)|max(th)|max(four)|
        +--------+--------+--------+-------+---------+--------+--------+--------+-------+---------+
        |7       |5       |2       |4      |4        |7       |5       |2       |4      |4        |
        +--------+--------+--------+-------+---------+--------+--------+--------+-------+---------+
        */

    dataFrame1.select(attr.map(x => max(x).as(s"max_${x}")) ++ attr.map(x => min(x).as(s"min_${x}")): _*)
      .show(false)

    /*
+-------+-------+-------+------+--------+-------+-------+-------+------+--------+
|max_sno|max_fir|max_sec|max_th|max_four|min_sno|min_fir|min_sec|min_th|min_four|
+-------+-------+-------+------+--------+-------+-------+-------+------+--------+
|7      |5      |2      |4     |4       |1      |2      |1      |0     |1       |
+-------+-------+-------+------+--------+-------+-------+-------+------+--------+
     */


  }


}
