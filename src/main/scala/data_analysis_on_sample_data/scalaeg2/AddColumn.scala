package data_analysis_on_sample_data.scalaeg2

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}


object AddColumn {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local")
      .appName("test").getOrCreate()

    import spark.implicits._

    val data = spark.sparkContext.parallelize(Seq(
      (1, "1994-11-21 Xyz"),
      (2, "1994-11-21 00:00:00"),
      (3, "1994-11-21 00:00:00")
    )).toDF("id", "date")

    val checkUDF = udf((value: String) => {

      Try(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value)) match {
        case Success(d) => 1
        case Failure(e) => 0
      }
    })


    val check = udf((value: String) => {
      Try(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value)) match {
        case Success(d) => 1
        case Failure(e) => 0
      }
    })

    println(data.withColumn("badData", check($"date")).rdd.collect()(1))

    println(data.withColumn("badData", check($"date")).collect())


  }


}
