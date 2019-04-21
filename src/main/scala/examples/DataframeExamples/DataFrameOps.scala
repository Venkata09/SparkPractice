package examples.DataframeExamples

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 11/8/2017.
  */
object DataFrameOps {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("<<<< Aggregate by key test >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext


    sc.parallelize(1 to 100)

    val dataFrames = sqlContext.read.json("PATH to String")
    dataFrames.show()

    dataFrames.printSchema()

    dataFrames.select("name").show()
    dataFrames.select(dataFrames("age") > 10).show()

    dataFrames.groupBy("age").count().show()


  }
}
