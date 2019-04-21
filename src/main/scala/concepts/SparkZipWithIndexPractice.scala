package concepts

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 2/22/2018.
  */
object SparkZipWithIndexPractice {


  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().appName("DataFrame VS DataSet").master("local[4]")
      .appName("<<<< GroupByExample  >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext


    val data_PairRDD = sc.parallelize(List(("A", 11), ("B", 22)))

    val data_RDD = sc.parallelize(List(("A"), ("B")))

      /*

      takeOrdered(n, [ordering]) where n is the number of results to bring back and ordering the comparator you'd like to use.

       */

    val sortedData = data_PairRDD.sortBy(_._2, ascending = false) // Sorting by the VALUE.. that's the Second Element....

    println("=============================================================")
    sortedData.foreach(println)
    println("=============================================================")


    println("=============================================================")

    sortedData.zipWithIndex() foreach (println)
    println("=============================================================")





  }

}
