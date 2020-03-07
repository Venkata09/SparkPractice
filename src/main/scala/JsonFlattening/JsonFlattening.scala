package JsonFlattening

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

/**
  * @author vdokku
  */




object JsonFlattening {

  case class json_schema_class(cities: String, name: String, schools: Array[String])

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master")

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("<<<<< JsonFlattening >>>>>")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val df = spark.read.json(sc.parallelize(Seq(
      """{"properties": {
       "prop1": "foo", "prop2": "bar", "prop3": true, "prop4": 1}}"""
    )))


    df.printSchema
    println(" +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ ")
    df.select("properties.*").printSchema
    println(" +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ ")


    val jsonDataWithArray = spark.read.json(sc.parallelize(Seq("""{"a":1,"b":[2,3]}""")))


    jsonDataWithArray.printSchema

    import spark.implicits._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val flattened = jsonDataWithArray.withColumn("b", explode($"b"))

    println(" <<<<<<<<<<<<<<<<< AFTER FLATTENING >>>>>>>>>>>>>>>>>>>>>>>>>>> ")

    flattened.show()

    flattened.printSchema

    /*
    Spark SQL Flattening the JSON values.

    You need to specify schema explicitly to read the json file in the desired way.
    In this case it would be like this:
     */

    var json_schema = ScalaReflection
      .schemaFor[json_schema_class]
      .dataType
      .asInstanceOf[StructType]


    var people = spark.read.schema(json_schema).json("src/main/resources/data/people.json")
    var flattenedJsonData = people
      .select($"name", explode($"schools").as("schools_flat"))


    flattenedJsonData.show(false)

    /*


    root
 |-- properties: struct (nullable = true)
 |    |-- prop1: string (nullable = true)
 |    |-- prop2: string (nullable = true)
 |    |-- prop3: boolean (nullable = true)
 |    |-- prop4: long (nullable = true)

 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
18/04/07 15:27:59 INFO SparkContext: Invoking stop() from shutdown hook
root
 |-- prop1: string (nullable = true)
 |-- prop2: string (nullable = true)
 |-- prop3: boolean (nullable = true)
 |-- prop4: long (nullable = true)



     */

  }

}
