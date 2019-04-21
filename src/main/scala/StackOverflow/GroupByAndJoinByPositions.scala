package StackOverflow

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 10/10/2017.
  */
object GroupByAndJoinByPositions {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("QueryingWithStreamingSources").getOrCreate()
    val sc: SparkContext = spark.sparkContext


    /*
      group by gender and join by positions per group?

    Conver the following

    Gender, Age, Value
    1,      20,  21
    2,      23   22
    1,      26,  23
    2,      29,  24

    into THIS :>>>>>>>>>>>>

    Male_Age, Male_Value, Female_Age,  Female_Value
    20          21         23           22
    26          23         29           24*/


    import spark.sqlContext.implicits._
    val genderDF = Seq(
      (1, 20, 21),
      (2, 23, 22),
      (1, 26, 23),
      (2, 29, 24)
    ).toDF("Gender", "Age", "Value")


    genderDF.show()


    // Gender 1 = Male
    // Gender 2 = Female

    import org.apache.spark.sql.expressions.Window
    val byGender = Window.partitionBy("gender").orderBy("gender")


    val dateDF = spark.sqlContext.sql("select 1, '2015-09-01'"
    ).unionAll(spark.sqlContext.sql("select 2, '2015-09-01'")
    ).unionAll(spark.sqlContext.sql("select 1, '2015-09-03'")
    ).unionAll(spark.sqlContext.sql("select 1, '2015-09-04'")
    ).unionAll(spark.sqlContext.sql("select 2, '2015-09-04'"))


    // dataframe as an RDD (of Row objects)
    dateDF.rdd
      // grouping by the first column of the row
      .groupBy(r => r(0))
      // map each group - an Iterable[Row] - to a list and sort by the second column
      .map(g => g._2.toList.sortBy(row => row(1).toString))
      .collect()

    dateDF.rdd.groupBy(r => r(0)).map(g =>
      g._2.toList.sortBy(row => row(1).toString).zipWithIndex).collect()


    import org.apache.spark.sql.functions._


    val males = genderDF
      .filter("gender = 1")
      .select($"age" as "male_age",
        $"value" as "male_value",
        row_number() over byGender as "RN")


  }
}
