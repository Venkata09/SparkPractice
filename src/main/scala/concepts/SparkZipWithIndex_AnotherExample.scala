package concepts

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 2/22/2018.
  */
object SparkZipWithIndex_AnotherExample {


  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().appName("DataFrame VS DataSet").master("local[4]")
      .appName("<<<< GroupByExample  >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext


    /*
    1929  abc
    2384  def
    8753  ghi
    3893  jkl
     */
    val data_PairRDD = sc.parallelize(List((1929, "abc"), (2384, "def"), (8753, "ghi"), (3893, "jkl")))

    val data_RDD = sc.parallelize(List(("A"), ("B")))

    /*

    takeOrdered(n, [ordering]) where n is the number of results to bring back and ordering the comparator you'd like to use.

     */

    val sortedData = data_PairRDD.sortBy(_._2, ascending = false) // Sorting by the VALUE.. that's the Second Element....

    println("=============================================================")
    sortedData.foreach(println)
    println("=============================================================")


    println("=============================================================")

    sortedData.zipWithIndex().foreach(println)


    /*
    Normal ZIPWITHINDEX will be giving the following ouput.......
    ((8753,ghi),1)
    ((1929,abc),3)
    ((2384,def),2)
    ((3893,jkl),0)
     */
    sortedData.zipWithIndex().map {
      case (r, i) => (r._1, r._2, s"Serial-${i + 1}")
    }.foreach(println)


    /*

    (2384,def,Serial-3)
    (8753,ghi,Serial-2)
    (3893,jkl,Serial-1)
    (1929,abc,Serial-4)

     */
    println("=============================================================")






  }

}
