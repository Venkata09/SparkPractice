package api


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._


object SecondarySort_Example {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    val inputPath = "src/main/resources/data/sample-data.csv"

    // Solution 1: in-memory sort

    val sortList = udf((input: Seq[Row]) => {
      input.map { row: Row => (row.getInt(0), row.getInt(0)) }.sortBy(_._1).map(_._2)
    })

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)
      .groupBy("name")
      .agg(collect_list(struct("time", "value")).as("key2_list"))
      .withColumn("sorted_value_list", sortList(col("key2_list")))
      .select("name", "sorted_value_list")
      .show


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

    // Solution 2: composite key sort

    val sortBy = Window.partitionBy("name").orderBy(asc("time"))

    spark.read
      .option("header", "true")
      .csv(inputPath)
      .repartition(col("name"))
      .sortWithinPartitions("time")
      .groupBy("name").agg(collect_list("value").as("sorted_value_list"))
      .show



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
