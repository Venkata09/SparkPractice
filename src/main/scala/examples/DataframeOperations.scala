package examples

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 6/18/2017.
  */
object DataframeOperations {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("my-spark-app")
      .getOrCreate()



    val testRDD = sparkSession.sparkContext.textFile("")

    // Spark Session is the unified entry point for the Spark Application


    //    Unified entry point for reading data SparkSession is the entry point for reading data, similar to the
    // old SQLContext.read.
    //    If you want to read the json, avro, parquet file and apply the compression. Use the unified spark
    // session entry point  sparkSession
//    val jsonData = sparkSession.read.json("/home/webinar/person.json")

//    val tables = sparkSession.catalog.listTables()


  }
}
