package FL_Insurance_Analysis

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameApp {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<< FL_insurance_sample>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val df = spark.sqlContext.read.format("com.databricks.spark.csv").options(Map(
      "header" -> "true",
      "delimiter" -> ",",
      "inferSchema" -> "true"
    )).load("src/main/resources/FL_Insurance_Data/FL_insurance_sample.csv")

    df.show()
    df.printSchema()

    /* Some selection and adding columns */
    df.select("policyID", "county").withColumn("zeros", lit(0)).show()
    df.select("policyID", "county").withColumn("policyID*2", df("policyID") * 2).show()
    df.select("policyID", "county").withColumn("is_policyID>20000", df("policyID") > 20000).show()

    /* Some filtering */
    df.select(df("policyID"), (df("policyID") > 20000 && df("policyID") < 30000).alias("policyID_test")).show()

    /* Some aggregation */
    df.groupBy(df("county")).avg("eq_site_limit")
    df.groupBy(df("county")).agg(
      avg("eq_site_limit").alias("average"),
      min("eq_site_limit").alias("minimum"),
      max("eq_site_limit").alias("maximum")
    ).show()

    /* Some windowing, you'd need a HiveContext to run it*/
    // import org.apache.spark.sql.expressions.Window
    // val window = Window.partitionBy("county").orderBy("policyID")
    // df.select("policyID", "county", "eq_site_limit")
    // .withColumn("diff", lead("eq_site_limit", 1).over(window)).show()




  }
}