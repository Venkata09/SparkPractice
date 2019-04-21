package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 1/29/2018.
  */
object MapvsFlatMapExample {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< MAP vs FLAT MAP Implementation >>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext


    val x = sc.parallelize(List("spark rdd example", "sample example"))

    // map operation will return Array of Arrays in following case : check type of result
    val y = x.map(x => x.split(" ")) // split(" ") returns an array of words
    //result ->  Array[Array[String]] = Array(Array(spark, rdd, example), Array(sample, example))
    println("COUNT of WORDS :::>>::: ", y.count())

    y.collect().map(entry => println("ENTRY :::>>>::: " + entry.toList.mkString("<:::>")))
    /*
                ENTRY :::>>>::: spark<:::>rdd<:::>example   This is 1 ARRAY
                ENTRY :::>>>::: sample<:::>example          This is 2 ARRAY
     */

    /*Similar to map, but each input item can be mapped to 0 or more output items
    (so func should return a Seq rather than a single item).*/

    // flatMap operation will return Array of words in following case : Check type of result
    val z = x.flatMap(x => x.split(" "))
    z.collect().foreach { x => println("WORD :::>>::: " + x) }
    //result -> Array[String] = Array(spark, rdd, example, sample, example)

  }

}
