import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 2/9/2018.
  */
object MultiDimentionalAggregations {

  def main(args: Array[String]): Unit = {


    /*

    Multi-dimensional aggregate operators are enhanced variants of groupBy operator that allow you to create
    queries for subtotals, grand totals and superset of subtotals in one go.


     */
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


    import org.apache.spark.sql.functions._
    val groupedByCityANdYear = sales
      .groupBy("city", "year") // This is the subtotal for both the CITY & Year.
      .agg(sum("amount"))


    val groupedCityOnly = sales
      .groupBy("city")
      .agg(sum("amount"))
      .select($"city", lit(null) as "year", $"amount")


    val withUnion = groupedByCityANdYear
      .union(groupedCityOnly)
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)





  }

}
