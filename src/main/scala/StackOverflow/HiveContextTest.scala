package StackOverflow


import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 10/10/2017.
  */
object HiveContextTest {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("QueryingWithStreamingSources").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.sqlContext.implicits._

    val df = sc.parallelize(
      ("foo", 1) :: ("foo", 2) :: ("bar", 1) :: ("bar", 2) :: Nil
    ).toDF("k", "v")


    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._



    val w = Window.partitionBy($"k").orderBy($"v")
    df.select($"k", $"v", row_number().over(w).alias("rn")).show

    import org.apache.spark.sql.functions._



    val sampleDF = sc.parallelize((1, "2015-09-01") :: (2, "2015-09-01") :: (1, "2015-09-03") :: (1, "2015-09-04") :: (1, "2015-09-04") :: Nil)
      .toDF("id", "date")

    val dfDuplicate = sampleDF.selectExpr("id as idDup", "date as dateDup")
    val dfWithCounter = sampleDF.join(dfDuplicate, $"id" === $"idDup")
      .where($"date" <= $"dateDup")
      .groupBy($"id", $"date")
      .agg($"id", $"date", count($"idDup").as("counter"))
      .select($"id", $"date", $"counter")

    dfWithCounter.show()


    /*import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.{coalesce, datediff, lag, lit, min, sum}*/


    val userNameAndLoginDateDF = Seq(
      ("SirChillingtonIV", "2012-01-04"), ("Booooooo99900098", "2012-01-04"),
      ("Booooooo99900098", "2012-01-06"), ("OprahWinfreyJr", "2012-01-10"),
      ("SirChillingtonIV", "2012-01-11"), ("SirChillingtonIV", "2012-01-14"),
      ("SirChillingtonIV", "2012-08-11")
    ).toDF("user_name", "login_date")


    val userWindow = Window.partitionBy("user_name").orderBy("login_date")
    val userSessionWindow = Window.partitionBy("user_name", "session")


    val newSession =  (coalesce(
      datediff($"login_date", lag($"login_date", 1).over(userWindow)),
      lit(0)
    ) > 5).cast("bigint")

    val sessionized = userNameAndLoginDateDF.withColumn("session", sum(newSession).over(userWindow))


    val result = sessionized
      .withColumn("became_active", min($"login_date").over(userSessionWindow))
      .drop("session")







  }
}
