package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author vdokku
  */
object IPLMatchAnalysis {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession
      .builder().master("local[4]")
      .appName("<<<<<<<<<<<< IPL Data Operation >>>>>>>>>>> ")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext


    val iplData = spark.read.option("header", "true").
      csv("src/main/resources/IPL/matches.csv")

    /*
    +---+------+---------+----------+---------------------------+---------------------------+---------------------------+-------------+------+----------+---------------------------+-----------+--------------+---------------+-----------------------------------------+---------------------+-------------+-------+
|id |season|city     |date      |team1                      |team2                      |toss_winner                |toss_decision|result|dl_applied|winner                     |win_by_runs|win_by_wickets|player_of_match|venue                                    |umpire1              |umpire2      |umpire3|
+---+------+---------+----------+---------------------------+---------------------------+---------------------------+-------------+------+----------+---------------------------+-----------+--------------+---------------+-----------------------------------------+---------------------+-------------+-------+
|1  |2017  |Hyderabad|2017-04-05|Sunrisers Hyderabad        |Royal Challengers Bangalore|Royal Challengers Bangalore|field        |normal|0         |Sunrisers Hyderabad        |35         |0             |Yuvraj Singh   |Rajiv Gandhi International Stadium, Uppal|AY Dandekar          |NJ Llong     |null   |
|2  |2017  |Pune     |2017-04-06|Mumbai Indians             |Rising Pune Supergiant     |Rising Pune Supergiant     |field        |normal|0         |Rising Pune Supergiant     |0          |7             |SPD Smith      |Maharashtra Cricket Association Stadium  |A Nand Kishore       |S Ravi       |null   |

     */

    iplData.show(false)

    val dataRdd = sc.textFile("src/main/resources/IPL/matches.csv", 3)
    //Split each line into an array of string, based on a comma, as a delimiter
    val data = dataRdd.map { x => x.split(",") }
    /* Which stadium is best suitable for first batting  */

    val fil = data.map { x => (x(7), x(11), x(12), x(14)) }
    val wonByRuns = fil.filter(f => if (f._2.toInt != 0) true else false)

    val bat_first_won_per_venue = wonByRuns.map(f => (f._4, 1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false)
    println("Highest Bat First winning count = " + bat_first_won_per_venue.take(1)(0)._2)

    val bat_first_won_per_venue_unSorted = wonByRuns.map(f => (f._4, 1)).reduceByKey(_ + _)

    val batFirstWonCountPerVenue = wonByRuns.map(f => (f._4, 1)).reduceByKey(_ + _).collect().sortBy(-_._2)
    batFirstWonCountPerVenue.foreach(println)

    //Now calculate how many matches that each stadium has been venued

    val matchCountPerVenue = fil.map(f => (f._4, 1)).reduceByKey(_ + _).collect().sortBy(-_._2)
    matchCountPerVenue.foreach(println)
    //Alternate syntax map(_.swap)
    val match_Count_Per_Venue = fil.map(f => (f._4, 1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false)

    val match_Count_Per_Venue_unSorted = fil.map(f => (f._4, 1)).reduceByKey(_ + _)

    //Now calculate winning percentage

    //We cannot join array it needs to be converted into RDD - val fCRDD = sc.parallelize(batFirstWonCountPerVenue) the only we can join


    //bat_first_won_per_venue.join(match_Count_Per_Venue).map(x=>(x._1._1,((x._2._1.toInt*100)/x._2._1.toInt))).collect.foreach(println)

    val join = bat_first_won_per_venue_unSorted.join(match_Count_Per_Venue_unSorted)

    join.map(f => ((f._1), ((f._2._1 * 100) / f._2._2))).map(item => item.swap).sortByKey(false).foreach(println)


  }

}
