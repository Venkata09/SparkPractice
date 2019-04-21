package examples

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 6/18/2017.
  */
object SparkReadJson {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("my-spark-app")
      .getOrCreate()


    val jsonDF = sparkSession.read.json("src/main/resources/spark_data/people.json")

    jsonDF.show()
    jsonDF.printSchema()
    jsonDF.createTempView("people") // registerTempTable is the Spark version of
    val teenagers = sparkSession.sql("select * from people where age > 12")

    teenagers.show()

  }
}
