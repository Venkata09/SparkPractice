package data_analysis_on_sample_data.scalaeg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object SelectTopNExample extends App {

  case class A(id: Long, distance: Double)

  System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


  val spark =
    SparkSession.builder().master("local").appName("test").getOrCreate()

  import spark.implicits._


  val df = List(A(1, 5.0), A(1, 3.0), A(1, 7.0), A(1, 4.0), A(2, 1.0), A(2, 3.0), A(2, 4.0), A(2, 7.0))
    .toDF("id", "distance")
  df.show

  val window = Window.partitionBy("id").orderBy("distance")

  val result = df.withColumn("rank", row_number().over(window)).where(col("rank") <= 2)

  result.drop("rank").show


  /*
  +---+--------+
| id|distance|
+---+--------+
|  1|     3.0|
|  1|     4.0|
|  2|     1.0|
|  2|     3.0|
+---+--------+
   */


  /*  println(List(2,4) ++ List(2,6))


    val data = spark.sparkContext.parallelize(Seq(
      (1,(2,3)), (1,(5,6)), (7,(8,9))
    ))/*.toDF("id", "values")*/*/


}
