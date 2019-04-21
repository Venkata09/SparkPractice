package api

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object SecondarySort {


  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("<<<< SecondarySort test >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    import sparkSession.implicits._

    val inputPath = "src/main/resources/secondary_sort/secondary_sort.csv"


    println("-----------------------------------------------------")
    println(" ::::::::::::IN-memory SORTING Technique:::::::::::::: ")
    println("-----------------------------------------------------")


    val sortList = udf((input: Seq[Row]) => {
      input.map {
        row: Row => (row.getInt(0), row.getInt(0))
      }
        .sortBy(_._1)
        .map(_._2)
    })

    sparkSession.read
      .option("header", "true")
      .option("inferschema", "true")
      .csv(inputPath)
      .groupBy("name")
      .agg(collect_list(struct("time", "value")).as("key2_list"))
      .withColumn("sorted_value_list", sortList(col("key2_list")))
      .select("name", "sorted_value_list")
      .show()


    /*

+----+-----------------+
|name|sorted_value_list|
+----+-----------------+
|   x|        [1, 2, 3]|
|   z|     [1, 2, 3, 4]|
|   p|  [1, 2, 4, 6, 7]|
|   y|        [1, 2, 3]|
+----+-----------------+

     */

    println("-----------------------------------------------------")
    println(" ::::::::::::COMPOSITE Key Sort :::::::::::::: ")
    println("-----------------------------------------------------")


    val sortBy = Window.partitionBy("name").orderBy(asc("time"))

    sparkSession.read.option("header", "true")
      .csv(inputPath)
      .repartition(col("name"))
      .sortWithinPartitions("time")
      .groupBy("name")
      .agg(collect_list("value").as("sorted_value_list")).show()


    /*
    +----+-----------------+
|name|sorted_value_list|
+----+-----------------+
|   x|        [3, 9, 6]|
|   z|     [4, 8, 7, 0]|
|   p|  [9, 6, 7, 0, 3]|
|   y|        [7, 5, 1]|
+----+-----------------+
     */

  }

}
