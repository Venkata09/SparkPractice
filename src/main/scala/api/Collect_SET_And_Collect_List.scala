package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author vdokku
  */
object Collect_SET_And_Collect_List {

  def main(args: Array[String]): Unit = {

    /*
        collect_set() contains distinct elements and collect_list() contains all elements (except nulls)
    */

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val spark: SparkSession = SparkSession
      .builder().master("local[4]")
      .appName("<<<<<<<<<<<< Spark String CONCAT Operation >>>>>>>>>>> ")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext


    val sampleDF = spark.createDataFrame(Seq(("a", null, null),
      ("a", "code1", null),
      ("a", "code2", "name2"))).toDF("id", "code", "name")

    /*

+---+-----+-----+
|id |code |name |
+---+-----+-----+
|a  |null |null |
|a  |code1|null |
|a  |code2|name2|
+---+-----+-----+
     */

    sampleDF.show(false)

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._


/*
    +---+------------------+------------------+
    |id |collect_list(code)|collect_list(name)|
      +---+------------------+------------------+
    |a  |[code1, code2]    |[name2]           |
    +---+------------------+------------------+
*/


    sampleDF.groupBy("id").agg(collect_list("code"), collect_list("name")).show(false)

  }
}
