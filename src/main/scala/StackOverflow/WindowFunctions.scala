package StackOverflow

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 2/5/2018.
  */
object WindowFunctions {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().master("local").appName("test").getOrCreate()

    import spark.implicits._

    val sqlContext = spark.sqlContext

    val df = sqlContext.sql("select 1, '2015-09-01'"
    ).unionAll(sqlContext.sql("select 2, '2015-09-01'")
    ).unionAll(sqlContext.sql("select 1, '2015-09-03'")
    ).unionAll(sqlContext.sql("select 1, '2015-09-04'")
    ).unionAll(sqlContext.sql("select 2, '2015-09-04'"))


    val sampleList = df.rdd
      .groupBy(entry => entry(0))
      .map(entry => entry._2.toList.sortBy(row => row(1).toString))
      .collect()

    df.rdd.take(22).foreach(entry => entry(0))


    sampleList.foreach(println(_))
    /*
    List([1,2015-09-01], [1,2015-09-03], [1,2015-09-04])
    List([2,2015-09-01], [2,2015-09-04])
     */

    val sampleList_WithZipIndex = df.rdd
      .groupBy(entry => entry(0))
      .map(entry => entry._2.toList.sortBy(row => row(1).toString).zipWithIndex)
      .collect()

    sampleList_WithZipIndex.foreach(println(_))
    /*
    List(([1,2015-09-01],0), ([1,2015-09-03],1), ([1,2015-09-04],2))
    List(([2,2015-09-01],0), ([2,2015-09-04],1))
     */

    // Another Solution:


    val anotherSolutionDF = spark.sparkContext.parallelize(
      (1, "2015-09-01") ::
        (2, "2015-09-01") ::
        (1, "2015-09-03") ::
        (1, "2015-09-04") ::
        (1, "2015-09-04") :: Nil)
      .toDF("id", "date")

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val w = Window.partitionBy($"id").orderBy($"date")
    anotherSolutionDF.select($"id", $"date", row_number().over(w).alias("rowNum")).show


  /*
    These are the other WINDOW Functions that needs to be explored.

    rank()
    dense_rank()
    cume_dist()
    lag("", 2)
    lead("", 3)
    coalesce()
    collect_list("")
    collect_set("")

    */
    /*
      +---+----------+------+
      | id|      date|rowNum|
      +---+----------+------+
      |  1|2015-09-01|     1|
      |  1|2015-09-03|     2|
      |  1|2015-09-04|     3|
      |  1|2015-09-04|     4|
      |  2|2015-09-01|     1|
      +---+----------+------+
      //sampleDataToGroup.txt
     */

    //    val dfDuplicate = anotherSolutionDF.selecExpr("id as idDup", "date as dateDup")

    /*

      val dfWithCounter = anotherSolutionDF
                                .where($"date" <= $"dateDup")
                                .groupBy($"id", $"date")
                                .agg($"id", $"date", count($"idDup").as("counter"))
                                .select($"id", $"date", $"counter")


      */




  }

}
