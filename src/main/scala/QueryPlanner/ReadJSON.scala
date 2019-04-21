package QueryPlanner

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 1/2/2018.
  */
object ReadJSON {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()


    val jsonDf = sparkSession
      .read
      .option("wholeFile", true)
      .option("mode", "FAILFAST")
      .option("allowComments", true)
      .option("allowBackslashEscapingAnyCharacter", true)
      .option("allowBackslashEscapingAnyCharacter", true)
      .json("/src/main/resources/json/tweets.json")

    jsonDf.printSchema()

    jsonDf.createOrReplaceTempView("jsonTable")


  }

}
