package activityRunnerProject

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Helper {
  val sc = new SparkContext()
  val sqlContext = SparkSession.builder().getOrCreate().sqlContext
  val BaseDir = new java.io.File(".").getCanonicalPath

  protected def createDf(file: String, headers: Seq[String], delimiter: String = ","): DataFrame = {
    sqlContext.read.format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .load(s"$BaseDir/$file")
      .toDF(headers: _*)
  }

  protected def writeDfToOutput(dataframe: DataFrame, outputDir: String): Unit = {
    var output = s"$BaseDir/$outputDir"

    dataframe.write.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .save(output)
  }
}
