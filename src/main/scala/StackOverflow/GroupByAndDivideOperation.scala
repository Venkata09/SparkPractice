package StackOverflow

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 10/10/2017.
  */
object GroupByAndDivideOperation {


  //https://stackoverflow.com/questions/39946210/spark-2-0-datasets-groupbykey-and-divide-operation-and-type-safety?rq=1



  case class MyClass (c1: String,
                      c2: String,
                      c3: String,
                      c4: Double)


  case class AnotherClass(key: (String, String, String), sum: Double)


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("QueryingWithStreamingSources").getOrCreate()
    val sc: SparkContext = spark.sparkContext


/*


Problem #1 - divide operation on aggregated column- Consider below code - I have a DataSet[MyCaseClass] and I wanted to groupByKey on c1,c2,c3 and sum(c4)
/ 8. The below code works well if I just calculate the sum but it gives compile time error for divide(8). I wonder how I can achieve following.

Problem #2 - converting groupedByKey result to another Typed DataFrame - Now second part of my problem is I want output again a typed DataSet. For that I have another case class (not sure if it is needed) but I am not sure how to map with grouped result -

* */

    import org.apache.spark.sql.expressions.scalalang.typed.{sum => typedSum}
    import org.apache.spark.sql.functions._
    import spark.sqlContext.implicits._



    val eight = lit(8.0)
      .as[Double]  // Not necessary

    val sumByEight = typedSum[MyClass](_.c4)
      .divide(eight)
      .as[Double]  // Required
      .name("div(sum(c4), 8)")



    val myCaseClass = Seq(
      MyClass("a", "b", "c", 2.0),
      MyClass("a", "b", "c", 3.0)
    ).toDS

    myCaseClass
      .groupByKey(myCaseClass => (myCaseClass.c1, myCaseClass.c2, myCaseClass.c3))
      .agg(sumByEight)

/*

+-------+---------------+
|    key|div(sum(c4), 8)|
+-------+---------------+
|[a,b,c]|          0.625|
+-------+---------------+


 */

    myCaseClass
      .groupByKey(myCaseClass => (myCaseClass.c1, myCaseClass.c2, myCaseClass.c3))
      .agg(typedSum[MyClass](_.c4).name("sum"))
      .as[AnotherClass]

/*

+-------+---+
|    key|sum|
+-------+---+
|[a,b,c]|5.0|
+-------+---+


 */




    import org.apache.spark.sql.functions._
    import org.apache.spark.ml.linalg.Vector

    val euclidean = udf((v1: Vector, v2: Vector) => ???)  // Fill with preferred logic

//    val jP2 = jP.withColumn("dist", euclidean($"features", $"episodeFeatures"))




  }
}
