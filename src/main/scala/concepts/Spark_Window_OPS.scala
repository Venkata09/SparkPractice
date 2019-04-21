package concepts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * @author vdokku
  */
object Spark_Window_OPS {

  System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


  def main(args: Array[String]): Unit = {


    val spark =
      SparkSession.builder().master("local").appName("test").getOrCreate()

    import spark.implicits._

    val sqlContext = spark.sqlContext

    val sampleDataSet = spark.read.csv("src/main/resources/data/Spark_Window_Operations/window_ops_EX.csv").toDF("Dev_No", "model", "Tested")


    println("----------------------------------------------------------")
    print("COUNT:>>>>>: ", sampleDataSet.count())
    println("----------------------------------------------------------")


    sampleDataSet.show(false)

    val resultDF = sampleDataSet.groupBy("Dev_No", "model").agg(concat_ws(", ", collect_list("model")).alias("Tested_devices"))
      .where(sampleDataSet("Tested") === 'Y')


    resultDF.show(false)


  }

}
