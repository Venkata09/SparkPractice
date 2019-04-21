package DataframeExamplesOnUSPresidentsDS

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 2/7/2018.
  */
object PresidentsDataAnalysis {

  case class president(presidentId: String, presidentName: String, wikipediaLink: String, tookOffice: String,
                       leftOffice: String, party: String, portrait: String, thumbnail: String, homeState: String)

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val dataSetEx = spark.read
      .option("header", "true")
      /*.option("inferSchema","false")*/
      .csv("src/main/resources/presidents_ds/USPresident_1.csv")


    val presidentsDataSet = spark.read.format("com.databricks.spark.csv").option("header", "false")
      .option("inferschema", "true").load("src/main/resources/presidents_ds/USPresident-Wikipedia-URLs-Thmbs-HS.csv")


    /*
    +---+--------------------+--------------------+----------+----------+--------------------+--------------------+--------------------+-------------+
|_c0|                 _c1|                 _c2|       _c3|       _c4|                 _c5|                 _c6|                 _c7|          _c8|
+---+--------------------+--------------------+----------+----------+--------------------+--------------------+--------------------+-------------+
|  1|   George Washington|http://en.wikiped...|30/04/1789| 4/03/1797|        Independent |GeorgeWashington.jpg|thmb_GeorgeWashin...|     Virginia|

     */



    presidentsDataSet.show()

    presidentsDataSet.select("_c0").show()


    // For now I am considering the _c3 as the DOB to simpler my analysis. Let's start.

    presidentsDataSet.select("_c0", "_c3", "_c8").show() // ==> THis is accessing the dataframe using the string IMPL.

//    presidentsDataSet.select($"_c0", $"_c3", "_c8").show()  ==> THis is accessing the dataframe using the string
    // IMPL. You cannnot put the $ and String in the same way, You can use only 1.




    /*



https://stackoverflow.com/questions/44455375/hive-subquery-in-where-clause

https://stackoverflow.com/questions/7677333/how-to-write-subquery-and-use-in-clause-in-hive

https://stackoverflow.com/questions/32077064/hive-subquery-and-group-by

https://stackoverflow.com/questions/14935116/hive-multiple-subqueries

https://stackoverflow.com/questions/44674572/hive-comparison-with-aggregate-result-from-subquery

https://stackoverflow.com/search?q=Hive+Subquery

https://github.com/seglo/exactly-once-streams

https://www.javabrahman.com/java-8/java-8-maps-computeifabsent-computeifpresent-getordefault-methods-tutorial-with-examples/
https://www.javabrahman.com/category/java-8/page/3/






https://stackoverflow.com/questions/17621596/spark-whats-the-best-strategy-for-joining-a-2-tuple-key-rdd-with-single-key-rd

https://rklicksolutions.wordpress.com/2016/11/26/tutorial-how-to-load-and-save-data-from-different-data-source-in-spark-2-0-2/comment-page-1/

https://stackoverflow.com/questions/31283932/map-side-aggregation-in-spark

https://stackoverflow.com/questions/31283932/map-side-aggregation-in-spark

http://etlcode.com/index.php/blog/info/Bigdata/Apache-Spark-Difference-between-reduceByKey-groupByKey-and-combineByKey

https://stackoverflow.com/questions/43678852/loading-data-file-with-3-spaces-as-delimiter-using-sparks-csv-reader-in-java/43679098

IMPORTANT:

http://sqlandhadoop.com/spark-dataframe-alias-as/

    See the difference here

     "" ===> This is you are accessing the dataset using a string.
     $"" ===> This is specific to DATAFRAME or DATASET ... now it will become as COLUMN and you can apply the alias
     on this column.

    */

    val modifiedDS = presidentsDataSet.alias("Presidents Data").select($"_c0".alias("president_id"), $"_c3".alias
    ("president_dob"), $"_c8".alias("president_birthState"))

    // Now let's look @ filter & more spark sql Functions.


    modifiedDS.filter($"president_birthState" === "New York")
              .select("president_id", "president_dob", "president_birthState").show()

  // You can use OR , && Operations.
    modifiedDS.filter($"president_birthState" === "New York" || $"president_birthState" === "Ohio")
      .select("president_id", "president_dob", "president_birthState").show()

    println("===================USING WHERE condition ==================================")
    modifiedDS.where($"president_birthState" === "New York").select("president_id", "president_dob", "president_birthState").show()
    println("=====================================================")



    /*println("===================USING WHERE condition ==================================")
    modifiedDS.where($"president_birthState = 'New York'").select("president_id", "president_dob",
      "president_birthState").show()
    println("=====================================================")


    println("===================USING WHERE condition ==================================")
    modifiedDS.where($"president_birthState = 'New York' and pres_name='Martin Van Buren'").select("president_id", "president_dob",
      "president_birthState").show()
    println("=====================================================")*/


    println("===================USING =!= condition ==================================")
    modifiedDS.where($"president_birthState" =!= "New York").select("president_id",
      "president_dob",
      "president_birthState").show()
    println("=====================================================")


    println("===================USING notEqual condition ==================================")
    modifiedDS.where($"president_birthState".notEqual("New York")).select("president_id",
      "president_dob",
      "president_birthState").show()
    println("=====================================================")



    println("===================USING Equal condition ==> equalTo ==================================")
    modifiedDS.where($"president_birthState".equalTo("New York")).select("president_id",
      "president_dob",
      "president_birthState").show()
    println("=====================================================")




    /*dataSetEx.show()


    dataSetEx.printSchema()

    dataSetEx.select("Name").show()*/


  }

}
