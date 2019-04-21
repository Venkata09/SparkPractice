package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 1/29/2018.
  */
object CountByKeyExample {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext


    val list = List((1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (3, 3))
    val rdd = sc.parallelize(list, 3)

    val map = rdd.countByKey()
    for ((k, v) <- map) println("KEY is :0- " + k + "  Value is :0- " + v)


  }

}
