package concepts

import org.apache.spark.sql.SparkSession


case class Person(personid: Int, personName: String, cityid: Int)


object CompareTwoDataFrames {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("DataFrame VS DataSet").master("local[4]")
      .appName("<<<< GroupByExample  >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    import sparkSession.implicits._

    val df1 = Seq(
      Person(0, "AgataZ", 0),
      Person(1, "Iweta", 0),
      Person(2, "Patryk", 2),
      Person(9999, "Maria", 2),
      Person(5, "John", 2),
      Person(6, "Patsy", 2),
      Person(7, "Gloria", 222),
      Person(3333, "Maksym", 0)).toDF

    val df2 = Seq(
      Person(0, "Agata", 0),
      Person(1, "Iweta", 0),
      Person(2, "Patryk", 2),
      Person(5, "John", 2),
      Person(6, "Patsy", 333),
      Person(7, "Gloria", 2),
      Person(4444, "Hans", 3)).toDF


    df1.show()


    /*
    +--------+----------+------+
|personid|personname|cityid|
+--------+----------+------+
|       0|    AgataZ|     0|
|       1|     Iweta|     0|
|       2|    Patryk|     2|
|    9999|     Maria|     2|
|       5|      John|     2|
|       6|     Patsy|     2|
|       7|    Gloria|   222|
|    3333|    Maksym|     0|
+--------+----------+------+

+--------+----------+------+
|personid|personname|cityid|
+--------+----------+------+
|       0|     Agata|     0|
|       1|     Iweta|     0|
|       2|    Patryk|     2|
|       5|      John|     2|
|       6|     Patsy|   333|
|       7|    Gloria|     2|
|    4444|      Hans|     3|
+--------+----------+------+
+------+-----------+------+-----------+--------+------------+------------+
|  Name|personCity1| Name2|personCity2|PersonID|nameChange ?|cityChange ?|
+------+-----------+------+-----------+--------+------------+------------+
| Patsy|          2| Patsy|        333|       6|          No|         Yes|
|Maksym|          0|  null|       null|    3333|         Yes|         Yes|
|  null|       null|  Hans|          3|    4444|         Yes|         Yes|
|Gloria|        222|Gloria|          2|       7|          No|         Yes|
| Maria|          2|  null|       null|    9999|         Yes|         Yes|
|AgataZ|          0| Agata|          0|       0|         Yes|          No|
+------+-----------+------+-----------+--------+------------+------------+


     */

    df2.show()

    import org.apache.spark.sql.functions._


    val joined = df1.join(df2, df1("personid") === df2("personid"), "outer")
    val newNames = Seq("personId1", "personName1", "personCity1", "personId2", "personName2", "personCity2")
    val df_Renamed = joined.toDF(newNames: _*)

    // Some deliberate variation shown in approach for learning
    val df_temp = df_Renamed.filter($"personCity1" =!= $"personCity2" || $"personName1" =!= $"personName2" || $"personName1".isNull || $"personName2".isNull || $"personCity1".isNull || $"personCity2".isNull).select($"personId1", $"personName1".alias("Name"), $"personCity1", $"personId2", $"personName2".alias("Name2"), $"personCity2").withColumn("PersonID", when($"personId1".isNotNull, $"personId1").otherwise($"personId2"))

    val df_final = df_temp.withColumn("nameChange ?", when($"Name".isNull or $"Name2".isNull or $"Name" =!= $"Name2", "Yes").otherwise("No")).withColumn("cityChange ?", when($"personCity1".isNull or $"personCity2".isNull or $"personCity1" =!= $"personCity2", "Yes").otherwise("No")).drop("PersonId1").drop("PersonId2")

    df_final.show()
  }

}
