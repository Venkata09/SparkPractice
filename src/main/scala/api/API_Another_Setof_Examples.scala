package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 2/16/2018.
  */
object API_Another_Setof_Examples {

  case class Business(business_id: String, full_address: String, categories: String)

  case class Review(review_id: String, user_id: String, business_id: String, stars: Double)


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master")

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<Spark Examples>>>>").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    println("---------------------------------------------------------------------------")
    val businessRDD_1 = sc.textFile("src/main/resources/data/business.csv")


    println(" Initial COUNT :>>>> ", businessRDD_1.count())
    println("---------------------------------------------------------------------------")

    val buddinessRDD = sc.textFile("src/main/resources/data/business.csv")
      .map(entry => entry.split("\\^")).map(entry => Business(entry(0), entry(1), entry(2)))
    val reviewsRDD = sc.textFile("src/main/resources/data/review.csv")
      .map(entry => entry.split("\\^"))
      .map(entry => Review(entry(0), entry(1), entry(2), entry(3).toDouble))

    println("<<<<<<<<<<<Bussiness Counts >>>>>>>> ", buddinessRDD.count())

    println("<<<<<<<<<<<< Reviews Count >>>>>>>>>>> ", reviewsRDD.count())

    val bussinessMap = buddinessRDD.map(business => (business.business_id, business))


    val reviewsMap = reviewsRDD.map(review => (review.business_id, review))

    val reviewBusinessJoinedData = bussinessMap.join(reviewsMap)

    val cleanedData = reviewBusinessJoinedData
      .map(entry => (entry._1, entry._2._1.full_address, entry._2._1.categories, entry._2._2.stars))

    cleanedData.map(entry => ((entry._1, entry._2, entry._3), (entry._4, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    /*
    Here you are passing the value ===
    That sample value is going to be X ==> (Double, Int)  & Y ==> (Double, Int)

    ==> This will be converting to
        double + Doube
*/

// TODO: YET to complete this....
    val businessFilterdData = bussinessMap.filter(entry => entry._2.full_address.contains("TX")) // Another


  }

}
