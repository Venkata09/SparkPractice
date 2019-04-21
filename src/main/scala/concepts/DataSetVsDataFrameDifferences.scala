package concepts

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by vdokku on 2/16/2018.
  */
object DataSetVsDataFrameDifferences {

  case class Persons(id: Int, name: String)

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().appName("DataFrame VS DataSet").master("local[4]")
      .appName("<<<< GroupByExample  >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext


    val personsRDD = sparkSession
      .sparkContext
      .parallelize(
        Seq(
          Persons(1, "Ram"),
          Persons(2, "Robert")
        ))


    personsRDD.foreach(println)

    val personsDF = sparkSession.createDataFrame(personsRDD)
    /*
    +---+------+
|id |name  |
+---+------+
|1  |Ram   |
|2  |Robert|
+---+------+
     */

    /*

    val personsDS: Dataset[Persons] = personsDF.as[Persons] You cannot

    Spark Datasets require Encoders for data type which is about to be stored.
    For common types (atomics, product types) there is a number of predefined
    encoders available but you have to import these first from SparkSession.implicits to make it work:
    */
    import sparkSession.implicits._

    val personsDS = sparkSession.createDataset(personsRDD)


    /*

    Type Safety....

        personsDF.filter(per => per.)  ===> Since this is a ROW --->
        You cannot get the domain objects..... you cannot
        access age & ID within the dataframe ....

        where as in dataset you can access ....

    */

    println("-------------------------- From the DataSets ---------------------")
    personsDS.show(false)

    println("--------------------------------------------------------------")
    println("-------------------------- From the DataFrames  ---------------------")

    personsDF.show(false)
    println("--------------------------------------------------------------")


    println("--------------------------------------------------------------")

    /*
    Cannot operate on domain Object (lost domain object):
     */
    val personDataFrame = sparkSession.createDataFrame(personsRDD)
    personDataFrame.rdd // ===> This will return a RDD of ROW's


    val personDataS = sparkSession.createDataFrame(personsRDD)
    personDataS.rdd // ===> This will return a RDD of Person because DataSet preserve the DOMAIN Objects.

  }
}
