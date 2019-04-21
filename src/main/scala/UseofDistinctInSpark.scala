import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 2/9/2018.
  */
object UseofDistinctInSpark {

  def main(args: Array[String]): Unit = {



    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._


    val sales = Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    ).toDF("city", "year", "amount")


    val salesRDD = sales.rdd

    // Now I got the RDD.

    val hashPartioner = new HashPartitioner(2)

    val myRDD = salesRDD.distinct




  }

}
