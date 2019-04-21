package dynamic_schema

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Dynamic_Schema {


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("QueryingWithStreamingSources").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master")


    /*val listOfProperties = explodeFeatures.schema
      .filter(c => c.name == "listOfFeatures")
      .flatMap(_.dataType.asInstanceOf[StructType].fields).filter(y => y.name == "properties").flatMap(_.dataType.asInstanceOf[StructType].fields)
      .map(_.name).map(x => "col(\"listOfFeatures.properties."+x+"\").as(\"properties_"+x.replace(":","_")+"\")")
*/

    /*  val listOfProperties = explodeFeatures.schema
        .filter(c => c.name == "listOfFeatures")
        .flatMap(_.dataType.asInstanceOf[StructType].fields)
        .filter(y => y.name == "properties")
        .flatMap(_.dataType.asInstanceOf[StructType].fields)
        .map(_.name)
        .map(x => col(s"listOfFeatures.properties.${x}").as(s"""properties_${x.replace(":","_")}""" ))
      */


    /*select(listOfProperties : _*)*/


    //  [{"key":"key1","value":"val1"}, {"key":"key2","value":"val2"}, {"key":"key3","value":"val3"}]




    val peopleJsonData =
      spark.read.json("C:\\Venkat_DO\\ApacheSparkCoreLearning\\src\\main\\resources\\spark_data\\people_1.json")


    peopleJsonData.show(false)


  }

}
