package dynamic_schema

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Spark_Sql_Explode_Example {


  case class Setting(key: String, value: String)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("QueryingWithStreamingSources").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master")

    import spark.implicits._
    val jsonStr = """{ "setting": { "key":"key1","value":"val1" }, {"key":"key2","value":"val2"}, {"key":"key3","value":"val3"}}"""
    val df = spark.read.json(Seq(jsonStr).toDS)



    df.show(false)


    val schema = StructType(
      StructField("a", IntegerType) ::
        StructField("b", StringType) :: Nil
    )


    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window
    import org.joda.time.LocalDate


    /*val newDF = df.withColumn("setting", explode($"settings"))
      .select($"id", from_json($"setting" Encoders.product[Setting].schema) as 'settings
    )


    newDF.show()*/

  }

}
