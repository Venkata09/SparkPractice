package api

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 6/15/2017.
  */
object GroupByReduceByKey {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("<<<< Aggregate by key test >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    val words = Array("a", "b", "b", "c", "d", "e", "a", "b", "b", "c", "d", "e", "b", "b", "c", "d", "e")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD
      .reduceByKey(_ + _)
      .collect()
    wordCountsWithReduce.foreach(f => println(f))

    //Avoid GroupByKey
    println("Avoid GroupByKey")
    val wordCountsWithGroup = wordPairsRDD
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect()
    wordCountsWithGroup.foreach(f => println(f))


  }
}
