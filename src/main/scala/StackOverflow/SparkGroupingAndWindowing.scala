package StackOverflow

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 2/5/2018.
  */
object SparkGroupingAndWindowing {

  case class SampleTextFile(sampleId: String, sampleString1: String, sampleString2: String, sampleString3: String,
                            sampleString4: String, sampleString5: String)

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession.builder().master("local").appName("test").getOrCreate()

    import spark.implicits._

    val sqlContext = spark.sqlContext

    val sampleDataSet = spark.read.csv("src/main/resources/data/sampleDataToGroup.csv")

    sampleDataSet.show()

    val groupedDS = sampleDataSet.groupBy(sampleDataSet("_c0"))

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    //|_c0|_c1|_c2|_c3|_c4|_c5|

    val windowSpec = Window.partitionBy(col("_c0"), col("_c1"), col("_c2")).orderBy(col("_c4").desc, col("_c5").desc)

    val finalDF = sampleDataSet.select(col("*"), row_number().over(windowSpec).alias("_c7"))

    finalDF.show()


  }

}
