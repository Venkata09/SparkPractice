package Interview

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author vdokku
  */
object Agg {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession
      .builder().master("local[4]")
      .appName("<<<<<<<<<<<< Spark String CONCAT Operation >>>>>>>>>>> ")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext




  }

}
