package api

import org.apache.spark.sql.SparkSession

/**
  *
  *
  *
  *
  *
  *
  * *
  * You can join the two DataFrames and apply a UDF that computes the diff between the two sequence columns:
  *
  * @author vdokku
  */
object RemoveStringFromOneDFBasedOnAnotherDF {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Remove-String-From-One-DF-From-Another").getOrCreate()


    import spark.implicits._
    import org.apache.spark.sql.functions.{min, max}
    import org.apache.spark.sql.Row

    import org.apache.spark.sql.functions._


    val stringTokenDF = Seq(
      (1, Seq("A", "B", "C", "D")),
      (1, Seq("B", "C", "D", "G")),
      (1, Seq("A", "D", "E")),
      (1, Seq("B", "C", "F")),
      (2, Seq("A", "C", "D")),
      (2, Seq("C", "E", "F")),
      (2, Seq("A", "C", "D", "H"))
    ).toDF("Id", "Tokens")

    val leastFrequenctDf = Seq(
      (1, Seq("E", "G")),
      (2, Seq("E", "F", "H"))
    ).toDF("Id", "LeastFrequentWords")

    def diff = udf(
      (firstString: Seq[String], secondString: Seq[String]) => firstString diff secondString
    )


    stringTokenDF
      .join(leastFrequenctDf, Seq("Id"))
      .select($"Id", diff($"Tokens", $"LeastFrequentWords").as("Tokens"))
      .show


  }

}
