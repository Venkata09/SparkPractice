package QueryPlanner

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Rolling_Average {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("QueryingWithStreamingSources").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master")


    val schema = Seq("id", "cykle", "value")
    val data = Seq(
      (1, 1, 1),
      (1, 2, 11),
      (1, 3, 1),
      (1, 4, 11),
      (1, 5, 1),
      (1, 6, 11),
      (2, 1, 1),
      (2, 2, 11),
      (2, 3, 1),
      (2, 4, 11),
      (2, 5, 1),
      (2, 6, 11)
    )

    import spark.implicits._

    val dft = sc.parallelize(data).toDF(schema: _*)
    dft.select('*).show


    val w = Window.partitionBy("id").orderBy("cykle").rowsBetween(-2, 2)
    val x = dft.select($"id", $"cykle", avg($"value").over(w))

    x.show


/*
+---+-----+---------------------------------------------------------------------------------------------------------+
| id|cykle|avg(value) OVER (PARTITION BY id ORDER BY cykle ASC NULLS FIRST ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)|
+---+-----+---------------------------------------------------------------------------------------------------------+
|  1|    1|                                                                                        4.333333333333333|
|  1|    2|                                                                                                      6.0|
|  1|    3|                                                                                                      5.0|
|  1|    4|                                                                                                      7.0|
|  1|    5|                                                                                                      6.0|
|  1|    6|                                                                                        7.666666666666667|
|  2|    1|                                                                                        4.333333333333333|
|  2|    2|                                                                                                      6.0|
|  2|    3|                                                                                                      5.0|
|  2|    4|                                                                                                      7.0|
|  2|    5|                                                                                                      6.0|
|  2|    6|                                                                                        7.666666666666667|
+---+-----+---------------------------------------------------------------------------------------------------------+

 */


  }

}
