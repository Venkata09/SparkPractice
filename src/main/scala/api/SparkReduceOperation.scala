package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 1/29/2018.
  */
object SparkReduceOperation {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession
      .builder().master("local[4]")
      .appName("<<<<<<<<<<<< SparkReduceOperation >>>>>>>>>>> ")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext


    val list = List(11, 0, 12, 1, 5)
    val rdd = sc.parallelize(list, 3)


    println(rdd.first())
    val s = rdd.take(3)
    for (a <- s) println("take :" + a)


    val sample = rdd.sample(false, 0.3)
    for (a <- sample) println("sample :" + a)


    val takeOrdered = rdd.takeOrdered(3)
    for (a <- takeOrdered) println("takeOrdered :" + a)


    val sum = rdd.count()
    val all = rdd.collect()
    print("all:");
    for (a <- all) print(a + " ");
    println()


    val result = rdd.reduce(
      (x, y) => {
        println("x= " + x + " ; y= " + y);
        x - y
      })
    println("result:" + result)


    rdd.foreach(x => println("foreach" + x))

  }

}
