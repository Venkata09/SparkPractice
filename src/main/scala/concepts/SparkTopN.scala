package concepts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

/**
  * Created by vdokku on 1/3/2018.
  */
object SparkTopN {


  case class distanceAndcheckPoint(id: Long, distance: Double)
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext

    import sparkSession.implicits._
    implicit val sqlContext = sparkSession.sqlContext

    // So Here I am trying to calculate difference between those checkpoints and calculate the value.

    val df = List(
      distanceAndcheckPoint(1, 5.0),
      distanceAndcheckPoint(1, 3.0),
      distanceAndcheckPoint(1, 7.0),
      distanceAndcheckPoint(1, 4.0),
      distanceAndcheckPoint(2, 1.0),
      distanceAndcheckPoint(2, 3.0),
      distanceAndcheckPoint(2, 4.0),
      distanceAndcheckPoint(2, 7.0))
      .toDF("id", "distance")
    df.show


    val window = Window.partitionBy("id").orderBy("distance")

    val result = df.withColumn("rank", row_number().over(window)).where(col("rank") <= 2)

    result.drop("rank").show

  }
}
