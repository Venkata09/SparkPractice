package examples

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 6/18/2017.
  */
object SparkGroupTopN {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("my-spark-app")
      .getOrCreate()

    val lines = sparkSession.sparkContext.textFile("src/main/resources/spark_data/groupTop.txt")
    val groupRDD = lines.map(line => (line.split(" ")(0), line.split(" ")(1).toInt)).groupByKey()
    val top5 = groupRDD.map(pair => (pair._1, pair._2.toList.sortWith(_ > _).take(5))).sortByKey()
    top5.collect().foreach(pair => {
      println(pair._1 + ":")
      pair._2.foreach(println)
      println("*****************")
    })

  }
}
