package StackOverflow

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


/*


colA    colB    colC    colD    colE    extraCol1   extracol2
Harshit 23        43    44         A           q    z
Mohit   24        56    62         B           w    x
Harshit 23        32    44         C           e    c
Kali    10        20    460        D           r    v
Aman    20        30    180        E           t    b
Ram     30        100   270        F          yu    n
Kali    10        600   360        G          io    m
Kali    10        600   460        k           p    o


 */
object SparkSQLFirstAndLastAggregatedFunctions {


  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkSQLFirstAndLastAggregatedFunctions")
      .getOrCreate()


    import spark.implicits._
    val genderDF = Seq(
      ("Harshit", 23, 43, 44, "A", "q", "z"),
      ("Mohit", 24, 56, 62, "B", "w", "x"),
      ("Harshit", 23, 32, 44, "C", "e", "c"),
      ("Kali", 10, 20, 460, "D", "r", "v"),
      ("Aman", 20, 30, 180, "E", "t", "b"),
      ("Ram", 30, 100, 270, "F", "yu", "n"),
      ("Kali", 10, 600, 360, "G", "io", "m"),
      ("Kali", 10, 600, 460, "k", "p", "o")
    ).toDF("ColA", "ColB", "ColC", "ColD", "ColE", "extraCol1", "extraCol2")


    genderDF.show(19)


    /*val cols = List("colA", "colB")

    var aggFuncSeq = List(max(`colC`) as colC_new, max(`colD`) as colD_new, first(`colE`, true) as colE, first(`extracol2`, true) as extracol2, first(`extraCol1`, true) as extraCol1)

    var aggFuncs = aggFuncSeq.map(e => expr(e))

    df = df.groupBy(cols.head, cols.tail: _*).agg(aggFuncs.head, aggFuncs.tail: _*)

    df.show(10)*/
  }


}
