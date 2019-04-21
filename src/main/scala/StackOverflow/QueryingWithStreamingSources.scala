package StackOverflow

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 10/10/2017.
  */
object QueryingWithStreamingSources {

  System.setProperty("hadoop.home.dir", "C:\\Venkata_DO\\hadoop-common-2.2.0-bin-master")
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("QueryingWithStreamingSources").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._



    /*
         Now I want to apply a division formula.
        Group by "KEY " and calculate "re_pct" as ( sum(sa) / sum( sa / (pct/100) ) ) * 100

    */


    val sampleList = List(List("01", "20000", "45.30"), List("01", "30000", "45.30")).map(
      entry => (entry(0), entry(1), entry(2))
    ).toDF("Key", "SA", "PCT")


    sampleList.show()
    import org.apache.spark.sql.functions._

    val resultDF = sampleList.groupBy("Key").agg(
      sum($"SA").divide(sum($"SA".divide($"PCT".divide(100)))) * 100)


    resultDF.show()

    val listOfLists = List(List("01", "20000", "45.30"), List("01", "30000", "45.30"))

    val result = List(List("01", "20000", "45.30"), List("01", "30000", "45.30"))
                      .map( entry => (entry(0), entry(1), entry(2))).toDF("Key", "SA", "PCT").groupBy("Key")
                      .agg((sum($"SA").divide(sum($"SA".divide($"PCT".divide(100)))) * 100)
                      .as("re_pcnt"))

    result.show()

  }
}
