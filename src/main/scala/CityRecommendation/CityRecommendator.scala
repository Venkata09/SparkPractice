package CityRecommendation

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * Created by vdokku on 10/7/2017.
  */
object CityRecommendator {

  def main(args: Array[String]): Unit = {

//    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("CityRecommendation")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val ratingsRDD = sc.textFile("src/main/resources/CityRecommendation/ratings.csv")
    val cityRDD = sc.textFile("src/main/resources/CityRecommendation/city.csv")

    println("Ratings RDD Count :> ", ratingsRDD.count())
    println("City RDD Count :> ", cityRDD.count())

    def modelRatingsRDD: RDD[(Long, Rating)] = {

      ratingsRDD.map(ratingsEntry => {
        val entry = ratingsEntry.split(",")

        (entry(3).toLong % 10, Rating(entry(0).toInt, entry(1).toInt, entry(2).toDouble))
      }
      )
    }

    def collectCitiesMap: Map[Int, String] = {

      cityRDD.map(cityEntries => {
        val entry = cityEntries.split(",")

        (entry(0).toInt, entry(1))

      }).collect().toMap
    }


    def getTop10Cities: List[(Int, String)] = {

      /*
            RDD has an INT, Rating object.
            so you iterated over an RDD and you are working only on the Integers.

            val output = modelRatingsRDD.map(entry => entry._2.product)

            RDD[Int] ==> Apply the count by Values operation.

            So if the RDD is an Single RDD ==> only countByValue operation okkate untadhi ==>

            countByValue() ==> RDD[KEY, LONG(This is nothing but the count value)]

            If it's a PAIR RDD or RDD[Key, Value] ==> There can be two options.

            countByKey()  ==>  MAP[Key, LONG(count of the Key's)]
            countByValue() ==>  MAP[(KEY, VALUE), LONG(Count of the value)]

            output.map()



            */



      val top50Cities = modelRatingsRDD.map { ratings => ratings._2.product }
        .countByValue()
        .toList
        .sortBy(-_._2)
        .take(50)
        .map(ratingsData => ratingsData._1)

      top50Cities.filter(id => collectCitiesMap.contains(id))
        .map { cityId => (cityId, collectCitiesMap.getOrElse(cityId, "No City Found")) }
        .sorted
        .take(10)
    }


  }


}
